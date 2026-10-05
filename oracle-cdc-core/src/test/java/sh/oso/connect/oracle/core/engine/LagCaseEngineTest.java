/*
 * Copyright 2026 OSO DevOps Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sh.oso.connect.oracle.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.DictionaryUnavailableException;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.mining.step.StepOutcome;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.DictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;

/**
 * P1-17, PRD-03 section 3 step 4: rows the online catalog can no longer map (written before a later
 * DDL on their table) are mined again with the redo dictionary and decode with the version valid at
 * their SCN; when that is impossible the task stops with CDC-6001 instead of guessing.
 */
class LagCaseEngineTest {

  static final TableId T = DdlEngineTest.T;
  static final String GENERIC =
      "insert into \"APP\".\"DDLT\"(\"COL 1\",\"COL 2\") values"
          + " (HEXTORAW('c102'),HEXTORAW('6f6e65'))";

  final FakeLogMiner fake = new FakeLogMiner().startAt(1000);
  final List<CommittedTransaction> committed = new ArrayList<>();
  final List<TableSchema> changes = new ArrayList<>();
  final List<Set<TableId>> replays = new ArrayList<>();
  final InMemorySchemaStore store = new InMemorySchemaStore();
  TableSchema dictionary = DdlEngineTest.schema("ID", "EXTRA"); // NAME dropped, EXTRA added
  Instant lastDdl;
  long scnAtLastDdl;

  @Test
  void rowsWrittenBeforeALaterDdlAreMinedAgainAndDecodeWithTheirOwnVersion() throws Exception {
    store.save(DdlEngineTest.schema("ID", "NAME")); // version 1, valid from any SCN
    TxKey a = fake.tx(1, 1, 1);
    TxKey d = fake.tx(1, 1, 2);
    TxKey b = fake.tx(1, 1, 3);
    fake.start(a, "APP")
        .lagged(a, Operation.INSERT, T, GENERIC, DdlEngineTest.insert(1, "NAME", "one"), "R1")
        .commit(a);
    fake.start(d, "APP").ddl(d, T, 100, "alter table app.ddlt drop column name").commit(d);
    fake.start(b, "APP")
        .dmlWithRowId(b, Operation.INSERT, T, DdlEngineTest.insert(2, "EXTRA", "x"), "R2")
        .commit(b);
    CaptureEngine e = engine();
    run(e);

    assertThat(fake.replays).as("one step mined again").isEqualTo(1);
    assertThat(e.metrics().lagReplays.get()).isEqualTo(1);
    assertThat(replays).containsExactly(Set.of(T));
    assertThat(committed).hasSize(2);
    RowChange before = committed.get(0).events().get(0);
    assertThat(before.after()).containsEntry("NAME", "one");
    assertThat(before.schemaVersion()).as("decoded with the version of its SCN").isEqualTo(1);
    RowChange after = committed.get(1).events().get(0);
    assertThat(after.after()).containsEntry("EXTRA", "x").doesNotContainKey("NAME");
    assertThat(after.schemaVersion()).isEqualTo(2);
    assertThat(changes).extracting(TableSchema::version).containsExactly(2);
  }

  @Test
  void withoutAUsableDictionaryBuildTheLagCaseStops() throws Exception {
    store.save(DdlEngineTest.schema("ID", "NAME"));
    fake.redoDictionaryFault =
        new DictionaryUnavailableException("No dictionary build ends before SCN 1000.", "Build.");
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP")
        .lagged(a, Operation.INSERT, T, GENERIC, DdlEngineTest.insert(1, "NAME", "one"), "R1")
        .commit(a);
    CaptureEngine e = engine();
    assertThatThrownBy(() -> run(e))
        .isInstanceOf(DictionaryUnavailableException.class)
        .hasMessageContaining("CDC-6001");
    assertThat(committed).isEmpty();
  }

  @Test
  void aRowStillInGenericNamesWithTheRedoDictionaryStops() throws Exception {
    store.save(DdlEngineTest.schema("ID", "NAME"));
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP").lagged(a, Operation.INSERT, T, GENERIC, GENERIC, "R1").commit(a);
    CaptureEngine e = engine();
    assertThatThrownBy(() -> run(e))
        .isInstanceOf(DictionaryUnavailableException.class)
        .hasMessageContaining("still have generic column names");
    assertThat(committed).isEmpty();
  }

  @Test
  void aTableFirstReadAfterTheDdlCannotDecodeItsEarlierRows() throws Exception {
    // nothing stored: the only version comes from today's dictionary, valid from its last DDL
    lastDdl = Instant.parse("2026-10-05T10:00:00Z");
    scnAtLastDdl = 5000;
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP")
        .lagged(a, Operation.INSERT, T, GENERIC, DdlEngineTest.insert(1, "NAME", "one"), "R1")
        .commit(a);
    CaptureEngine e = engine();
    assertThatThrownBy(() -> run(e))
        .isInstanceOf(DictionaryUnavailableException.class)
        .hasMessageContaining("CDC-6001")
        .hasMessageContaining("first read the table after that DDL");
    assertThat(committed).isEmpty();
  }

  @Test
  void lobRowsAndUndoRowsAreNotLagRows() {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP")
        .dml(a, Operation.LOB_WRITE, T, "DECLARE ... \"COL 1\" ...", false)
        .undo(a, Operation.INSERT, T, GENERIC, "R1");
    List<MiningEvent> events = new ArrayList<>(fake.events());
    events.replaceAll(
        ev ->
            ev instanceof MiningEvent.Dml d
                ? new MiningEvent.Dml(
                    d.tx(),
                    d.id(),
                    d.thread(),
                    d.op(),
                    d.table(),
                    d.dataObj(),
                    d.dataObjd(),
                    d.dataObjv(),
                    d.rowId(),
                    d.sqlRedo(),
                    d.sqlUndo(),
                    d.undo(),
                    2,
                    d.info(),
                    d.username(),
                    d.timestamp())
                : ev);
    StepOutcome outcome =
        new StepOutcome(
            StepOutcome.Kind.COMPLETE,
            events,
            sh.oso.connect.oracle.core.mining.step.StepCursor.at(2000),
            events.size(),
            null);
    assertThat(CaptureEngine.lagTables(outcome)).isEmpty();
  }

  private void run(CaptureEngine e) throws Exception {
    for (int i = 0; i < 20; i++) {
      e.runOnce();
    }
  }

  private CaptureEngine engine() {
    FakeCatalog catalog = new FakeCatalog().archivedRun(1, 1, 20, 1000, 100);
    long safeEnd = fake.nextScn() + 1;
    EventSink recording =
        new CaptureEngineTest.Sink() {
          @Override
          public void schemaChanged(TableSchema schema, MiningEvent.Ddl ddl) {
            changes.add(schema);
          }

          @Override
          public void committed(CommittedTransaction tx, int skip, RedoRecordId resume) {
            LagCaseEngineTest.this.committed.add(tx); // Sink has a field of the same name
          }

          @Override
          public void dictionaryReplayed(long from, long to, Set<TableId> tables) {
            replays.add(tables);
          }
        };
    DictionaryReader dict =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId t) {
            return t.equals(T) ? Optional.ofNullable(dictionary) : Optional.empty();
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            return new KeySelector.Candidates(List.of("ID"), List.of());
          }

          @Override
          public Optional<Instant> lastDdlTime(TableId t) {
            return Optional.ofNullable(lastDdl);
          }

          @Override
          public Optional<Long> scnAt(Instant time) {
            return lastDdl == null ? Optional.empty() : Optional.of(scnAtLastDdl);
          }
        };
    SchemaRegistry registry =
        new SchemaRegistry(
            store, dict, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE));
    return new CaptureEngine(
            Position.initial(1000, new DatabaseIdentity(1, 1)),
            fake,
            new LogInventory(catalog, CaptureMode.ONLINE, 1),
            () -> safeEnd,
            new HeapTransactionBuffer(),
            registry,
            ChangeDecoder.rowDecoder(),
            recording,
            new EngineSettings(
                Duration.ofSeconds(2),
                8,
                Duration.ofHours(1),
                Duration.ofMillis(10),
                3,
                DecodeErrorAction.FAIL),
            new OraErrorClassifier(),
            Set.of("APP"),
            () -> Set.of("APP"),
            cause -> {
              throw new AssertionError("unexpected reconnect", cause);
            },
            () -> Instant.EPOCH)
        .withCapturedTables(T::equals);
  }
}
