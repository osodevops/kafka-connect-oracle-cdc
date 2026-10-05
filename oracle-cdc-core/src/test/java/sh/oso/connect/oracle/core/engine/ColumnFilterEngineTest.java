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
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.insertEmpty;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.trim;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.undoUpdate;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.update;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.where;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.write;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.buffer.JournalChunk;
import sh.oso.connect.oracle.core.buffer.JournalPolicy;
import sh.oso.connect.oracle.core.buffer.JournalSink;
import sh.oso.connect.oracle.core.buffer.SpillStore;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.ColumnFilter;
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
 * PRD-01 SRC-SEL-2 through the engine loop: an excluded column's value never reaches the buffer,
 * the spill files, the journal or the sink; a LOB statement on an excluded column still takes part
 * in savepoint undo; reselect never fetches it; and a key column cannot be excluded, at the first
 * change or after a DDL that moves the key.
 */
class ColumnFilterEngineTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "CUSTOMERS");
  static final String SECRET = "secret-ssn";
  static final ColumnFilter SSN =
      ColumnFilter.of(List.of("FREEPDB1\\.APP\\.CUSTOMERS\\.SSN"), false);

  final FakeCatalog catalog = new FakeCatalog().archivedRun(1, 1, 20, 1000, 100);
  final FakeLogMiner fake = new FakeLogMiner().startAt(1000);

  /** Reads each committed transaction's events at once, as RecordQueueSink does. */
  final CaptureEngineTest.Sink sink =
      new CaptureEngineTest.Sink() {
        @Override
        public void committed(
            sh.oso.connect.oracle.core.buffer.CommittedTransaction tx,
            int skip,
            sh.oso.connect.oracle.core.model.RedoRecordId resume) {
          super.committed(
              new sh.oso.connect.oracle.core.buffer.CommittedTransaction(
                  tx.key(),
                  tx.firstCaptured(),
                  tx.startId(),
                  tx.commitId(),
                  tx.commitTimestamp(),
                  tx.thread(),
                  tx.username(),
                  tx.clientId(),
                  new ArrayList<>(tx.events())),
              skip,
              resume);
        }
      };

  long safeEnd;
  List<String> keyCandidates = List.of("ID");
  TableSchema dictionary = customers();

  @TempDir Path dir;

  static TableSchema customers() {
    return new TableSchema(
        T,
        List.of(
            ColumnSpec.of("ID", 1, OracleType.NUMBER),
            ColumnSpec.of("NAME", 2, OracleType.VARCHAR2),
            ColumnSpec.of("SSN", 3, OracleType.VARCHAR2)),
        List.of(),
        KeySource.NONE,
        true,
        false);
  }

  static String insert(int id, String name) {
    return "insert into \"APP\".\"CUSTOMERS\"(\"ID\",\"NAME\",\"SSN\") values ('"
        + id
        + "','"
        + name
        + "','"
        + SECRET
        + "-"
        + id
        + "')";
  }

  @Test
  void anExcludedValueNeverReachesTheBufferSpillJournalOrSink() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP");
    for (int i = 1; i <= 20; i++) {
      fake.dmlWithRowId(a, Operation.INSERT, T, insert(i, "n" + i), "AAAR" + i);
    }
    long beforeCommit = fake.nextScn();
    fake.commit(a);
    List<JournalChunk> chunks = new ArrayList<>();
    JournalSink journal =
        new JournalSink() {
          @Override
          public void chunk(JournalChunk chunk) {
            chunks.add(chunk);
          }
        };
    try (SpillStore spill = new SpillStore(dir.resolve("spill"), 1L << 30)) {
      // a budget of one byte spills the transaction at once; one event is enough to journal it
      HeapTransactionBuffer buffer =
          new HeapTransactionBuffer(
              1, spill, new JournalPolicy(null, 1, 1 << 20), journal, 1, () -> Instant.EPOCH);
      CaptureEngine e = engine(buffer, ChangeDecoder.rowDecoder()).withColumnFilter(SSN);
      safeEnd = beforeCommit; // the commit is not mined yet: the transaction stays open
      run(e);
      assertThat(buffer.openTransactions()).isEqualTo(1);
      assertThat(spilledBytes()).as("the transaction was spilled").isPositive();
      assertThat(spilledText()).doesNotContain(SECRET).contains("n20");
      assertThat(chunks).as("the transaction was journaled").isNotEmpty();
      for (JournalChunk c : chunks) {
        assertThat(new String(c.payload(), StandardCharsets.UTF_8)).doesNotContain(SECRET);
      }
      safeEnd = fake.nextScn() + 1;
      run(e);
    }
    List<RowChange> events = sink.committed.get(0).events();
    assertThat(events).hasSize(20);
    for (RowChange c : events) {
      assertThat(c.after()).containsOnlyKeys("ID", "NAME");
    }
  }

  @Test
  void aSubstituteDecodersChangeIsProjectedBeforeItIsBuffered() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP").dmlWithRowId(a, Operation.INSERT, T, "anything", "AAAR1").commit(a);
    ChangeDecoder stub =
        (d, schema) ->
            new RowChange(
                d.table(),
                d.op(),
                null,
                Map.of("ID", 1, "SSN", SECRET),
                false,
                d.rowId(),
                d.id(),
                d.tx(),
                d.timestamp());
    safeEnd = fake.nextScn() + 1;
    run(engine(new HeapTransactionBuffer(), stub).withColumnFilter(SSN));
    assertThat(sink.committed.get(0).events().get(0).after()).containsOnlyKeys("ID");
  }

  @Test
  void anExcludedKeyColumnStopsTheEngineAtTheTablesFirstChange() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP").dmlWithRowId(a, Operation.INSERT, T, insert(1, "one"), "AAAR1").commit(a);
    keyCandidates = List.of("SSN");
    safeEnd = fake.nextScn() + 1;
    CaptureEngine e = engine(new HeapTransactionBuffer(), ChangeDecoder.rowDecoder());
    e.withColumnFilter(SSN);
    assertThatThrownBy(() -> run(e))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("CDC-3001")
        .hasMessageContaining("column SSN of FREEPDB1.APP.CUSTOMERS");
    assertThat(sink.committed).isEmpty();
  }

  @Test
  void aDdlThatMovesTheKeyOntoAnExcludedColumnStopsTheEngine() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP").dmlWithRowId(a, Operation.INSERT, T, insert(1, "one"), "AAAR1").commit(a);
    TxKey d = fake.tx(1, 1, 2);
    fake.start(d, "APP")
        .ddl(d, T, 100, "alter table app.customers add constraint customers_pk primary key (ssn)")
        .commit(d);
    TxKey b = fake.tx(1, 1, 3);
    fake.start(b, "APP").dmlWithRowId(b, Operation.INSERT, T, insert(2, "two"), "AAAR2").commit(b);
    safeEnd = fake.nextScn() + 1;
    CaptureEngine e = engine(new HeapTransactionBuffer(), ChangeDecoder.rowDecoder());
    e.withColumnFilter(SSN);
    // the dictionary shows the key on SSN once the engine reads it again at the DDL
    afterFirstRead = () -> keyCandidates = List.of("SSN");
    assertThatThrownBy(() -> run(e))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("column SSN of FREEPDB1.APP.CUSTOMERS");
    assertThat(sink.committed).as("the change before the DDL").hasSize(1);
    assertThat(sink.committed.get(0).events().get(0).after()).containsOnlyKeys("ID", "NAME");
  }

  @Test
  void aLobStatementOnAnExcludedColumnIsStillUndoneAsAWhole() throws Exception {
    String r5 = "AAAR5FAAYAAAAANAAA";
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP");
    fake.dmlWithRowId(a, Operation.UPDATE, DOCS, update("\"NAME\" = 'kept'", 5, "five"), r5);
    // SAVEPOINT; UPDATE docs SET c = :big; ROLLBACK TO SAVEPOINT: C is excluded
    fake.dmlWithRowId(
        a, Operation.LOB_WRITE, DOCS, write("C", where(5, "kept"), 1, "bbbb"), RowIds.PLACEHOLDER);
    fake.dmlWithRowId(
        a, Operation.LOB_TRIM, DOCS, trim("C", where(5, "kept"), 4), RowIds.PLACEHOLDER);
    fake.undo(a, Operation.UPDATE, DOCS, undoUpdate("\"C\" = 'small five'"), r5);
    fake.commit(a);
    safeEnd = fake.nextScn() + 1;
    run(lobEngine(LobAssembler.Mode.INLINE).withColumnFilter(clob()));
    List<RowChange> events = sink.committed.get(0).events();
    // as without the filter: the earlier update stays, the undone LOB group is inert
    assertThat(events).hasSize(2);
    assertThat(events.get(0).rowId()).isEqualTo(r5);
    assertThat(events.get(0).after()).containsEntry("NAME", "kept").doesNotContainKey("C");
    assertThat(RowIds.isInert(events.get(1).rowId())).isTrue();
    assertThat(events.get(1).after()).doesNotContainKey("C");
  }

  @Test
  void anExcludedLobIsNeitherAssembledNorReselected() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP");
    fake.dmlWithRowId(a, Operation.INSERT, DOCS, insertEmpty(8, "eight"), RowIds.PLACEHOLDER);
    fake.dmlWithRowId(
        a, Operation.LOB_WRITE, DOCS, write("C", where(8, "eight"), 1, SECRET), RowIds.PLACEHOLDER);
    fake.dmlWithRowId(
        a,
        Operation.UPDATE,
        DOCS,
        update("\"NC\" = NULL, \"B\" = NULL", 8, "eight"),
        "AAAR5FAAYAAAAANAAD");
    // a LOB-only write of the excluded column, then one of a published column at an unknown base
    fake.dmlWithRowId(
        a,
        Operation.LOB_WRITE,
        DOCS,
        write("C", where(8, "eight"), 5, SECRET),
        "AAAR5FAAYAAAAANAAD");
    fake.dmlWithRowId(
        a,
        Operation.LOB_WRITE,
        DOCS,
        write("NC", where(8, "eight"), 5, "nc"),
        "AAAR5FAAYAAAAANAAD");
    fake.commit(a);
    List<List<String>> asked = new ArrayList<>();
    LobReselector reselector =
        (schema, change, scn, columns) -> {
          asked.add(columns);
          assertThat(schema.column("C")).as("the reselect layout").isNull();
          return Map.of("NC", "from the table");
        };
    safeEnd = fake.nextScn() + 1;
    run(lobEngine(LobAssembler.Mode.RESELECT).withColumnFilter(clob()).withReselector(reselector));
    List<RowChange> events = sink.committed.get(0).events();
    // LogMiner shows no boundary between LOB-only writes of one row: they are one change
    assertThat(events).as("the insert, then the LOB-only writes").hasSize(2);
    for (RowChange c : events) {
      assertThat(c.after()).doesNotContainKey("C");
      assertThat(String.valueOf(c.after())).doesNotContain(SECRET);
    }
    assertThat(events.get(1).after()).containsEntry("NC", "from the table");
    assertThat(asked).isNotEmpty().allSatisfy(cols -> assertThat(cols).doesNotContain("C"));
  }

  private static ColumnFilter clob() {
    return ColumnFilter.of(List.of("FREEPDB1\\.APP\\.DOCS\\.C"), false);
  }

  /** Runs when the engine reads the fake dictionary again (after a DDL). */
  Runnable afterFirstRead = () -> {};

  private CaptureEngine engine(TransactionBuffer buffer, ChangeDecoder decoder) {
    InMemorySchemaStore store = new InMemorySchemaStore();
    int[] reads = {0};
    DictionaryReader dict =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId t) {
            if (reads[0]++ > 0) {
              afterFirstRead.run();
            }
            return t.equals(T) ? Optional.of(dictionary) : Optional.empty();
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            return new KeySelector.Candidates(keyCandidates, List.of());
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
            buffer,
            registry,
            decoder,
            sink,
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

  private CaptureEngine lobEngine(LobAssembler.Mode mode) {
    return new CaptureEngine(
        Position.initial(1000, new DatabaseIdentity(1, 1)),
        fake,
        new LogInventory(catalog, CaptureMode.ONLINE, 1),
        () -> safeEnd,
        new HeapTransactionBuffer(),
        LobEngineTest.registry(),
        ChangeDecoder.rowDecoder(),
        sink,
        new EngineSettings(
                Duration.ofSeconds(2),
                8,
                Duration.ofHours(1),
                Duration.ofMillis(10),
                3,
                DecodeErrorAction.FAIL)
            .withLobs(mode, 1 << 20, true),
        new OraErrorClassifier(),
        Set.of("APP"),
        () -> Set.of("APP"),
        cause -> {
          throw new AssertionError("unexpected reconnect", cause);
        },
        () -> Instant.EPOCH);
  }

  private long spilledBytes() throws Exception {
    try (Stream<Path> files = Files.walk(dir)) {
      return files.filter(Files::isRegularFile).mapToLong(p -> p.toFile().length()).sum();
    }
  }

  private String spilledText() throws Exception {
    StringBuilder sb = new StringBuilder();
    try (Stream<Path> files = Files.walk(dir)) {
      for (Path p : files.filter(Files::isRegularFile).toList()) {
        sb.append(new String(Files.readAllBytes(p), StandardCharsets.ISO_8859_1));
      }
    }
    return sb.toString();
  }

  private static void run(CaptureEngine e) throws Exception {
    for (int i = 0; i < 100; i++) {
      if (e.runOnce() == CaptureEngine.Progress.IDLE) {
        return;
      }
    }
    throw new AssertionError("did not go idle");
  }
}
