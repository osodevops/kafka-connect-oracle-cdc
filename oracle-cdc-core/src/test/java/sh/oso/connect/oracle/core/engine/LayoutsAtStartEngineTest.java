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
import java.util.HashMap;
import java.util.HashSet;
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
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.DictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;

/**
 * ADR-0016 amendment (found by {@code SchemaTopicLossNightlyIT} on 6 October 2026): a table's
 * layout used to be read the first time one of its rows was decoded. A DDL between the start and
 * that read left the rows written before it with no version known to be valid, so the lag case
 * replay stopped with CDC-6001. The layout of every captured table without a stored version is now
 * read when the task starts, and when a table joins the captured set, valid from that position.
 */
class LayoutsAtStartEngineTest {

  static final TableId T = DdlEngineTest.T;
  static final TableId U = new TableId("FREEPDB1", "APP", "JOINS");
  static final DatabaseIdentity ID = new DatabaseIdentity(1, 1);

  /** The fake database's clock: SCN 1000 is at this time and each SCN is one second later. */
  static final Instant BASE = Instant.parse("2026-10-06T09:00:00Z");

  final FakeLogMiner fake = new FakeLogMiner().startAt(1101);
  final List<CommittedTransaction> committed = new ArrayList<>();
  final InMemorySchemaStore store = new InMemorySchemaStore();
  final Map<TableId, TableSchema> dictionary = new HashMap<>();
  final Map<TableId, Instant> lastDdl = new HashMap<>();
  final Set<TableId> captured = new HashSet<>(Set.of(T));

  @Test
  void rowsWrittenBeforeADdlSoonAfterTheStartDecodeWithTheVersionReadAtStart() throws Exception {
    dictionary.put(T, layout(T, "ID", "NAME"));
    lastDdl.put(T, time(1000)); // created long before the start at SCN 1100
    TxKey a = fake.tx(1, 1, 1);
    TxKey d = fake.tx(1, 1, 2);
    TxKey b = fake.tx(1, 1, 3);
    fake.start(a, "APP")
        .lagged(a, Operation.INSERT, T, generic(T), DdlEngineTest.insert(1, "NAME", "one"), "R1")
        .commit(a)
        .start(d, "APP");
    long ddlScn = fake.nextScn();
    fake.ddl(d, T, 100, "alter table app.ddlt add (extra varchar2(10))")
        .commit(d)
        .start(b, "APP")
        .dmlWithRowId(
            b, Operation.INSERT, T, DdlEngineTest.insert(2, "NAME", "two", "EXTRA", "x"), "R2")
        .commit(b);
    CaptureEngine e = engine(Position.initial(1100, ID), () -> Set.of("APP"));

    List<TableSchema> read = e.readStartLayouts(List.of(T));
    assertThat(read).extracting(TableSchema::validFromScn).containsExactly(1100L);
    assertThat(read).allMatch(TableSchema::exact);
    // the ALTER runs before the engine reaches the row written ahead of it, which the online
    // catalog then returns with generic names
    dictionary.put(T, layout(T, "ID", "NAME", "EXTRA"));
    lastDdl.put(T, time(ddlScn));
    run(e);

    assertThat(fake.replays).as("the first step is mined again").isEqualTo(1);
    assertThat(committed).hasSize(2);
    RowChange before = committed.get(0).events().get(0);
    assertThat(before.after()).containsEntry("NAME", "one").doesNotContainKey("EXTRA");
    assertThat(before.schemaVersion()).as("the version read at start").isEqualTo(1);
    RowChange after = committed.get(1).events().get(0);
    assertThat(after.after()).containsEntry("EXTRA", "x");
    assertThat(after.schemaVersion()).isEqualTo(2);
  }

  @Test
  void aFirstStartMiningOpenTransactionsFromBeforeItsStartScnDecodesThemWithTheStartVersion()
      throws Exception {
    // ADR-0019: x began at 1001 and is still open at the start SCN 1110, so mining begins at 1001.
    // x changed the table at 1055, after the table's last DDL at 1050; no DDL on the table can
    // complete while x holds its lock, so the layout at 1055 is the layout at the start. The
    // version is valid from the mining start, and whether a DDL can follow is judged at 1108, the
    // SCN read before the open transactions were listed: judged at 1001 the DDL at 1050 would count
    // and the version would be valid only from 1060, and valid from 1110 it would not cover 1055
    dictionary.put(T, layout(T, "ID", "NAME"));
    lastDdl.put(T, time(1050));
    TxKey x = fake.tx(1, 1, 1);
    TxKey y = fake.tx(1, 1, 2);
    TxKey d = fake.tx(1, 1, 3);
    TxKey b = fake.tx(1, 1, 4);
    fake.startAt(1001).start(x, "APP");
    fake.startAt(1055)
        .lagged(x, Operation.INSERT, T, generic(T), DdlEngineTest.insert(1, "NAME", "one"), "R1");
    // y ended before the start and was not listed: dropped before it is decoded (ADR-0019)
    fake.startAt(1103)
        .start(y, "APP")
        .lagged(y, Operation.INSERT, T, generic(T), DdlEngineTest.insert(2, "NAME", "two"), "R2")
        .commit(y);
    fake.startAt(1112).commit(x).start(d, "APP");
    long ddlScn = fake.nextScn();
    fake.ddl(d, T, 100, "alter table app.ddlt add (extra varchar2(10))")
        .commit(d)
        .start(b, "APP")
        .dmlWithRowId(
            b, Operation.INSERT, T, DdlEngineTest.insert(3, "NAME", "three", "EXTRA", "x"), "R3")
        .commit(b);
    Position start = Position.firstStart(1110, 1108, 1001, Set.of(x), ID);
    assertThat(start.resumeScn()).isEqualTo(1001);
    CaptureEngine e = engine(start, () -> Set.of("APP"));

    List<TableSchema> read = e.readStartLayouts(List.of(T));
    assertThat(read)
        .as("valid from the mining start, below the start SCN")
        .extracting(TableSchema::validFromScn)
        .containsExactly(1001L);
    dictionary.put(T, layout(T, "ID", "NAME", "EXTRA"));
    lastDdl.put(T, time(ddlScn));
    run(e);

    assertThat(committed).extracting(CommittedTransaction::key).containsExactly(x, b);
    RowChange early = committed.get(0).events().get(0);
    assertThat(early.after()).containsEntry("NAME", "one");
    assertThat(early.schemaVersion()).isEqualTo(1);
    assertThat(committed.get(1).events().get(0).schemaVersion()).isEqualTo(2);
  }

  @Test
  void aDdlThatMayHaveRunAfterTheStartIsNeverTakenAsBeforeIt() throws Exception {
    // the ALTER ran three seconds after the start SCN and before the read. SCN_TO_TIMESTAMP is too
    // coarse to tell that from three seconds before, so the version keeps the first-read rule
    // (valid from ten seconds after the table's last DDL) and the row written before the ALTER
    // stops the task. Taken as valid from the start, it would decode that row with a layout that
    // did not exist when the row was written
    TxKey a = fake.tx(1, 1, 1);
    TxKey d = fake.tx(1, 1, 2);
    fake.start(a, "APP")
        .lagged(a, Operation.INSERT, T, generic(T), DdlEngineTest.insert(1, "NAME", "one"), "R1")
        .start(d, "APP");
    long ddlScn = fake.nextScn();
    fake.ddl(d, T, 100, "alter table app.ddlt add (extra varchar2(10))").commit(d).commit(a);
    dictionary.put(T, layout(T, "ID", "NAME", "EXTRA"));
    lastDdl.put(T, time(ddlScn));
    CaptureEngine e = engine(Position.initial(ddlScn - 3, ID), () -> Set.of("APP"));

    List<TableSchema> read = e.readStartLayouts(List.of(T));
    assertThat(read).extracting(TableSchema::validFromScn).containsExactly(ddlScn + 10);
    assertThatThrownBy(() -> run(e))
        .isInstanceOf(DictionaryUnavailableException.class)
        .hasMessageContaining("CDC-6001");
    assertThat(committed).isEmpty();
  }

  @Test
  void aTableThatJoinsTheCapturedSetIsReadWhenItJoins() throws Exception {
    // SRC-SEL-4: a refresh-tables signal adds JOINS; its layout is read at the refresh, valid
    // from the cursor, and an ALTER right after it does not strand the rows written before
    dictionary.put(U, layout(U, "ID", "NAME"));
    lastDdl.put(U, time(1000));
    TxKey a = fake.tx(1, 1, 1);
    TxKey d = fake.tx(1, 1, 2);
    TxKey b = fake.tx(1, 1, 3);
    fake.start(a, "APP")
        .lagged(a, Operation.INSERT, U, generic(U), insert(U, 1, "NAME", "one"), "R1")
        .commit(a)
        .start(d, "APP");
    long ddlScn = fake.nextScn();
    fake.ddl(d, U, 200, "alter table app.joins add (extra varchar2(10))")
        .commit(d)
        .start(b, "APP")
        .dmlWithRowId(b, Operation.INSERT, U, insert(U, 2, "NAME", "two", "EXTRA", "x"), "R2")
        .commit(b);
    List<TableSchema> joined = new ArrayList<>();
    CaptureEngine[] engine = new CaptureEngine[1];
    CaptureEngine e =
        engine(
            Position.initial(1100, ID),
            () -> {
              if (captured.add(U)) {
                // as the task does when the session reports the table added
                joined.addAll(engine[0].readJoinedLayouts(Set.of(U)));
                dictionary.put(U, layout(U, "ID", "NAME", "EXTRA"));
                lastDdl.put(U, time(ddlScn));
              }
              return Set.of("APP");
            });
    engine[0] = e;
    e.requestRefresh();
    run(e);

    assertThat(joined).extracting(TableSchema::validFromScn).containsExactly(1100L);
    assertThat(committed).hasSize(2);
    assertThat(committed.get(0).events().get(0).after()).containsEntry("NAME", "one");
    assertThat(committed.get(0).events().get(0).schemaVersion()).isEqualTo(1);
    assertThat(committed.get(1).events().get(0).schemaVersion()).isEqualTo(2);
  }

  static Instant time(long scn) {
    return BASE.plusSeconds(scn - 1000);
  }

  static TableSchema layout(TableId table, String... columns) {
    List<ColumnSpec> cols = new ArrayList<>();
    for (String c : columns) {
      cols.add(
          ColumnSpec.of(
              c, cols.size() + 1, c.equals("ID") ? OracleType.NUMBER : OracleType.VARCHAR2));
    }
    return new TableSchema(table, cols, List.of(), KeySource.NONE, true, false);
  }

  /** What the online catalog returns for a row written before a later DDL on its table. */
  static String generic(TableId table) {
    return "insert into \""
        + table.schema()
        + "\".\""
        + table.table()
        + "\"(\"COL 1\",\"COL 2\") values (HEXTORAW('c102'),HEXTORAW('6f6e65'))";
  }

  static String insert(TableId table, int id, String... namesAndValues) {
    return DdlEngineTest.insert(id, namesAndValues)
        .replace("\"APP\".\"DDLT\"", "\"" + table.schema() + "\".\"" + table.table() + "\"");
  }

  private void run(CaptureEngine e) throws Exception {
    for (int i = 0; i < 20; i++) {
      e.runOnce();
    }
  }

  private CaptureEngine engine(Position start, CaptureEngine.IdRefresher refresher) {
    FakeCatalog catalog = new FakeCatalog().archivedRun(1, 1, 20, 1000, 100);
    long safeEnd = fake.nextScn() + 1;
    EventSink recording =
        new CaptureEngineTest.Sink() {
          @Override
          public void committed(CommittedTransaction tx, int skip, RedoRecordId resume) {
            LayoutsAtStartEngineTest.this.committed.add(tx);
          }
        };
    DictionaryReader dict =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId t) {
            return Optional.ofNullable(dictionary.get(t));
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            return new KeySelector.Candidates(List.of("ID"), List.of());
          }

          @Override
          public Optional<Instant> lastDdlTime(TableId t) {
            return Optional.ofNullable(lastDdl.get(t));
          }

          @Override
          public Optional<Long> scnAt(Instant time) {
            return Optional.of(1000 + Duration.between(BASE, time).toSeconds());
          }

          @Override
          public Optional<Instant> timeOfScn(long scn) {
            return Optional.of(time(scn));
          }
        };
    SchemaRegistry registry =
        new SchemaRegistry(
            store, dict, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE));
    return new CaptureEngine(
            start,
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
            refresher,
            cause -> {
              throw new AssertionError("unexpected reconnect", cause);
            },
            () -> Instant.EPOCH)
        .withCapturedTables(captured::contains);
  }
}
