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
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.DOCS;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.SCHEMA;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.insertEmpty;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.trim;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.undoUpdate;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.update;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.where;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.write;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.errors.LobTooLargeException;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
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

/** CORE-DEC-6 and CORE-DEC-7 through the whole engine loop, with the real decoder. */
class LobEngineTest {

  static final String R5 = "AAAR5FAAYAAAAANAAA";

  final FakeCatalog catalog = new FakeCatalog().archivedRun(1, 1, 20, 1000, 100);
  final FakeLogMiner fake = new FakeLogMiner().startAt(1000);
  final CaptureEngineTest.Sink sink = new CaptureEngineTest.Sink();
  long safeEnd;

  @Test
  void aSavepointRollbackOfLobWritesKeepsTheEarlierUpdateOfTheSameRow() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP");
    fake.dmlWithRowId(a, Operation.UPDATE, DOCS, update("\"NAME\" = 'kept'", 5, "five"), R5);
    // SAVEPOINT; UPDATE docs SET c = :big; ROLLBACK TO SAVEPOINT
    fake.dmlWithRowId(
        a, Operation.LOB_WRITE, DOCS, write("C", where(5, "kept"), 1, "bbbb"), RowIds.PLACEHOLDER);
    fake.dmlWithRowId(
        a, Operation.LOB_TRIM, DOCS, trim("C", where(5, "kept"), 4), RowIds.PLACEHOLDER);
    fake.undo(a, Operation.UPDATE, DOCS, undoUpdate("\"C\" = 'small five'"), R5);
    fake.commit(a);
    run(engine(LobAssembler.Mode.INLINE, 1 << 20, true));

    List<RowChange> events = sink.committed.get(0).events();
    assertThat(events).hasSize(2);
    assertThat(events.get(0).rowId()).isEqualTo(R5);
    assertThat(events.get(0).after()).containsEntry("NAME", "kept");
    // the undone LOB group remains as an update whose LOB value is unavailable
    assertThat(RowIds.isInert(events.get(1).rowId())).isTrue();
    assertThat(events.get(1).after()).doesNotContainKey("C");
  }

  @Test
  void aLargeInsertIsOneRecordWithItsValue() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP");
    fake.dmlWithRowId(a, Operation.INSERT, DOCS, insertEmpty(8, "eight"), RowIds.PLACEHOLDER);
    // code 9 rows: labelled INTERNAL on 23ai, no SQL_REDO, nothing to apply
    fake.dmlWithRowId(a, Operation.SELECT_LOB_LOCATOR, DOCS, null, RowIds.PLACEHOLDER);
    fake.dmlWithRowId(
        a,
        Operation.LOB_WRITE,
        DOCS,
        write("C", where(8, "eight"), 1, "x".repeat(50)),
        RowIds.PLACEHOLDER);
    fake.dmlWithRowId(
        a,
        Operation.UPDATE,
        DOCS,
        update("\"NC\" = NULL, \"B\" = NULL", 8, "eight"),
        "AAAR5FAAYAAAAANAAD");
    fake.commit(a);
    CaptureEngine e = engine(LobAssembler.Mode.INLINE, 1 << 20, true);
    run(e);
    List<RowChange> events = sink.committed.get(0).events();
    assertThat(events).hasSize(1);
    assertThat(events.get(0).op()).isEqualTo(Operation.INSERT);
    assertThat(events.get(0).rowId()).isEqualTo("AAAR5FAAYAAAAANAAD");
    assertThat(events.get(0).after()).containsEntry("C", "x".repeat(50));
    assertThat(e.metrics().lobRowsApplied.get()).isEqualTo(1);
    assertThat(e.metrics().lobInsertsMerged.get()).isEqualTo(1);
  }

  @Test
  void anOversizeValueStopsAtCommitOrIsLeftUnavailable() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP");
    fake.dmlWithRowId(
        a,
        Operation.LOB_WRITE,
        DOCS,
        write("C", where(5, "five"), 1, "y".repeat(20)),
        RowIds.PLACEHOLDER);
    fake.dmlWithRowId(
        a, Operation.LOB_TRIM, DOCS, trim("C", where(5, "five"), 20), RowIds.PLACEHOLDER);
    fake.commit(a);
    CaptureEngine failing = engine(LobAssembler.Mode.INLINE, 10, true);
    assertThatThrownBy(() -> run(failing))
        .isInstanceOf(LobTooLargeException.class)
        .hasMessageContaining("CDC-3003")
        .hasMessageContaining("FREEPDB1.APP.DOCS.C");
    assertThat(sink.committed).isEmpty();

    run(engine(LobAssembler.Mode.INLINE, 10, false));
    assertThat(sink.committed.get(0).events().get(0).after()).doesNotContainKey("C");
  }

  @Test
  void reselectFillsWhatTheRedoDidNotCarryAtTheCommitScn() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP");
    fake.dmlWithRowId(a, Operation.UPDATE, DOCS, update("\"NAME\" = 'n'", 5, "five"), R5);
    fake.dmlWithRowId(
        a,
        Operation.DELETE,
        DOCS,
        "delete from \"APP\".\"DOCS\" where " + where(6, "six"),
        "AAAR5FAAYAAAAANAAB");
    fake.commit(a);
    List<String> asked = new ArrayList<>();
    LobReselector reselector =
        (schema, change, scn, columns) -> {
          asked.add(change.after().get("ID") + "@" + scn + columns);
          return Map.of("C", "from the table", "B", new byte[] {7});
        };
    CaptureEngine e = engine(LobAssembler.Mode.RESELECT, 1 << 20, true).withReselector(reselector);
    run(e);
    List<RowChange> events = sink.committed.get(0).events();
    assertThat(events.get(0).after()).containsEntry("C", "from the table").doesNotContainKey("NC");
    assertThat(events.get(1).op()).isEqualTo(Operation.DELETE); // nothing to reselect for a delete
    assertThat(asked).hasSize(1);
    assertThat(asked.get(0)).startsWith("5@").endsWith("[C, NC, B]");

    // an oversize reselected value follows the oversize action
    sink.committed.clear();
    LobReselector huge = (schema, change, scn, columns) -> Map.of("C", "z".repeat(64));
    CaptureEngine stopping = engine(LobAssembler.Mode.RESELECT, 16, true).withReselector(huge);
    // the reselect runs as the sink reads the events, inside the engine's step, as
    // RecordQueueSink.committed does
    assertThatThrownBy(
            () -> {
              run(stopping);
              sink.committed.get(0).events().forEach(c -> {});
            })
        .isInstanceOf(LobTooLargeException.class);
  }

  @Test
  void aRollbackLobRowOrAnUndecodableBlockIsADecodeError() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP");
    fake.undo(a, Operation.LOB_WRITE, DOCS, write("C", where(5, "five"), 1, "x"), R5);
    fake.commit(a);
    assertThatThrownBy(() -> run(engine(LobAssembler.Mode.INLINE, 1 << 20, true)))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("ROLLBACK=1");
    sink.failed.clear();
    CaptureEngine dlq = engine(LobAssembler.Mode.INLINE, 1 << 20, true, DecodeErrorAction.DLQ);
    run(dlq);
    assertThat(sink.failed).hasSize(1);
    assertThat(dlq.metrics().decodeFailures.get()).isEqualTo(1);
  }

  private CaptureEngine engine(LobAssembler.Mode mode, long max, boolean fail) {
    return engine(mode, max, fail, DecodeErrorAction.FAIL);
  }

  private CaptureEngine engine(
      LobAssembler.Mode mode, long max, boolean fail, DecodeErrorAction onError) {
    safeEnd = fake.nextScn() + 1;
    EngineSettings s =
        new EngineSettings(
                Duration.ofSeconds(2), 8, Duration.ofHours(1), Duration.ofMillis(10), 3, onError)
            .withLobs(mode, max, fail);
    return new CaptureEngine(
        Position.initial(1000, new DatabaseIdentity(1, 1)),
        fake,
        new LogInventory(catalog, CaptureMode.ONLINE, 1),
        () -> safeEnd,
        new HeapTransactionBuffer(),
        registry(),
        ChangeDecoder.rowDecoder(),
        sink,
        s,
        new OraErrorClassifier(),
        Set.of("APP"),
        () -> Set.of("APP"),
        cause -> {
          throw new AssertionError("unexpected reconnect", cause);
        },
        () -> Instant.EPOCH);
  }

  static SchemaRegistry registry() {
    InMemorySchemaStore store = new InMemorySchemaStore();
    store.save(SCHEMA);
    DictionaryReader none =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId t) {
            throw new IllegalStateException("dictionary must not be read: " + t);
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            throw new IllegalStateException();
          }
        };
    return new SchemaRegistry(
        store, none, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE));
  }

  private void run(CaptureEngine e) throws Exception {
    for (int i = 0; i < 100; i++) {
      if (e.runOnce() == CaptureEngine.Progress.IDLE) {
        return;
      }
    }
    throw new AssertionError("did not go idle");
  }
}
