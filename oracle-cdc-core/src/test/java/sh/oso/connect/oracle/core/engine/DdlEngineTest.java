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
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.UnsupportedDdlException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
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

/** PRD-03 section 3 through the engine: a DDL gives the rows after it a new schema version. */
class DdlEngineTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "DDLT");
  static final TableId OTHER = new TableId("FREEPDB1", "APP", "NOT_CAPTURED");

  final FakeLogMiner fake = new FakeLogMiner().startAt(1000);
  final CaptureEngineTest.Sink sink = new CaptureEngineTest.Sink();
  final List<TableSchema> changes = new ArrayList<>();
  TableSchema dictionary = schema("ID", "NAME");

  static TableSchema schema(String... columns) {
    List<ColumnSpec> cols = new ArrayList<>();
    for (String c : columns) {
      cols.add(
          ColumnSpec.of(
              c, cols.size() + 1, c.equals("ID") ? OracleType.NUMBER : OracleType.VARCHAR2));
    }
    return new TableSchema(T, cols, List.of(), KeySource.NONE, true, false);
  }

  static String insert(int id, String... namesAndValues) {
    StringBuilder cols = new StringBuilder("\"ID\"");
    StringBuilder vals = new StringBuilder("'" + id + "'");
    for (int i = 0; i < namesAndValues.length; i += 2) {
      cols.append(",\"").append(namesAndValues[i]).append('"');
      vals.append(",'").append(namesAndValues[i + 1]).append('\'');
    }
    return "insert into \"APP\".\"DDLT\"(" + cols + ") values (" + vals + ")";
  }

  @Test
  void rowsAfterADdlDecodeWithTheNewVersion() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    TxKey d = fake.tx(1, 1, 2);
    TxKey b = fake.tx(1, 1, 3);
    fake.start(a, "APP")
        .dmlWithRowId(a, Operation.INSERT, T, insert(1, "NAME", "one"), "R1")
        .commit(a);
    fake.start(d, "APP").ddl(d, T, 100, "alter table app.ddlt add (extra varchar2(10))").commit(d);
    fake.start(b, "APP")
        .dmlWithRowId(b, Operation.INSERT, T, insert(2, "NAME", "two", "EXTRA", "x"), "R2")
        .commit(b);
    CaptureEngine e = engine();
    // the dictionary already shows the new column; the DDL makes it a version from its SCN
    runUntilDdl(e);
    assertThat(sink.committed).hasSize(2);
    assertThat(sink.committed.get(1).events().get(0).after()).containsEntry("EXTRA", "x");
    assertThat(changes).hasSize(1);
    assertThat(changes.get(0).version()).isEqualTo(2);
    assertThat(changes.get(0).column("EXTRA")).isNotNull();
  }

  @Test
  void anUnknownDdlOnACapturedTableStopsAndOnAnotherTableIsIgnored() throws Exception {
    TxKey x = fake.tx(1, 1, 1);
    fake.start(x, "APP").ddl(x, OTHER, 101, "alter table app.not_captured frobnicate").commit(x);
    TxKey y = fake.tx(1, 1, 2);
    fake.start(y, "APP").ddl(y, T, 100, "alter table app.ddlt frobnicate").commit(y);
    CaptureEngine e = engine();
    assertThatThrownBy(() -> run(e))
        .isInstanceOf(UnsupportedDdlException.class)
        .hasMessageContaining("CDC-6002")
        .hasMessageContaining("FREEPDB1.APP.DDLT")
        .hasMessageContaining("frobnicate");
    assertThat(sink.ddls).hasSize(2); // both seen on the ops topic
  }

  @Test
  void aDroppedTableIsForgottenAndATruncateChangesNothing() throws Exception {
    TxKey a = fake.tx(1, 1, 1);
    fake.start(a, "APP")
        .dmlWithRowId(a, Operation.INSERT, T, insert(1, "NAME", "one"), "R1")
        .commit(a);
    TxKey t = fake.tx(1, 1, 2);
    fake.start(t, "APP").ddl(t, T, 100, "truncate table app.ddlt").commit(t);
    TxKey d = fake.tx(1, 1, 3);
    fake.start(d, "APP").ddl(d, T, 100, "drop table app.ddlt purge").commit(d);
    CaptureEngine e = engine();
    run(e);
    assertThat(changes).containsExactly((TableSchema) null);
  }

  private void runUntilDdl(CaptureEngine e) throws Exception {
    run(e);
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
          public void committed(
              sh.oso.connect.oracle.core.buffer.CommittedTransaction tx,
              int skip,
              sh.oso.connect.oracle.core.model.RedoRecordId resume) {
            sink.committed.add(tx);
          }

          @Override
          public void ddl(MiningEvent.Ddl d) {
            sink.ddls.add(d);
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
        };
    InMemorySchemaStore store = new InMemorySchemaStore();
    store.save(schema("ID", "NAME"));
    SchemaRegistry registry =
        new SchemaRegistry(
            store, dict, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE));
    dictionary = schema("ID", "NAME", "EXTRA");
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

  private static void run(CaptureEngine e) throws Exception {
    for (int i = 0; i < 100; i++) {
      if (e.runOnce() == CaptureEngine.Progress.IDLE) {
        return;
      }
    }
    throw new AssertionError("did not go idle");
  }
}
