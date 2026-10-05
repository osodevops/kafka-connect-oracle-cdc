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
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.DOCS;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.insertEmpty;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.update;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.where;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.write;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;

/** CORE-MINE-6: decoding on several threads changes nothing but the speed. */
class ParallelDecodeEngineTest {

  @Test
  void parallelDecodeCommitsTheSameChangesAndFailuresInTheSameOrder() throws Exception {
    FakeLogMiner fake = new FakeLogMiner().startAt(1000);
    for (int t = 0; t < 6; t++) {
      TxKey tx = fake.tx(1, t, 1);
      fake.start(tx, "APP");
      for (int i = 0; i < 120; i++) {
        int id = t * 1000 + i;
        if (i % 40 == 7) {
          // an INSERT with an out-of-row LOB: row piece, write, locator update
          fake.dmlWithRowId(
              tx, Operation.INSERT, DOCS, insertEmpty(id, "r" + id), RowIds.PLACEHOLDER);
          fake.dmlWithRowId(
              tx,
              Operation.LOB_WRITE,
              DOCS,
              write("C", where(id, "r" + id), 1, "v" + id),
              RowIds.PLACEHOLDER);
          fake.dmlWithRowId(
              tx,
              Operation.UPDATE,
              DOCS,
              update("\"NC\" = NULL, \"B\" = NULL", id, "r" + id),
              "AAAR5F" + id);
        } else if (i == 60 && t == 2) {
          // undecodable: a column the table does not have
          fake.dmlWithRowId(
              tx, Operation.UPDATE, DOCS, update("\"NOPE\" = 'x'", id, "r"), "AAAX" + id);
        } else {
          fake.dmlWithRowId(tx, Operation.INSERT, DOCS, insertEmpty(id, "r" + id), "AAAR5F" + id);
        }
        if (t == 3 && i == 50) {
          fake.ddl(
              tx, new TableId("FREEPDB1", "APP", "OTHER"), 200, "alter table other add (x number)");
        }
      }
      fake.commit(tx);
    }
    CaptureEngineTest.Sink one = new CaptureEngineTest.Sink();
    CaptureEngineTest.Sink four = new CaptureEngineTest.Sink();
    run(engine(fake, one, 1));
    run(engine(fake, four, 4));
    assertThat(one.committed).hasSize(6);
    assertThat(four.committed).hasSize(6);
    for (int i = 0; i < 6; i++) {
      List<RowChange> a = one.committed.get(i).events();
      List<RowChange> b = four.committed.get(i).events();
      assertThat(b).hasSameSizeAs(a);
      for (int j = 0; j < a.size(); j++) {
        assertThat(b.get(j).after()).usingRecursiveComparison().isEqualTo(a.get(j).after());
        assertThat(b.get(j).rowId()).isEqualTo(a.get(j).rowId());
        assertThat(b.get(j).id()).isEqualTo(a.get(j).id());
      }
    }
    assertThat(four.failed)
        .extracting(d -> d.id())
        .containsExactlyElementsOf(one.failed.stream().map(d -> d.id()).toList());
    assertThat(four.failed).hasSize(1);
  }

  private CaptureEngine engine(FakeLogMiner fake, CaptureEngineTest.Sink sink, int threads) {
    FakeCatalog catalog = new FakeCatalog().archivedRun(1, 1, 20, 1000, 100);
    long safeEnd = fake.nextScn() + 1;
    EngineSettings s =
        new EngineSettings(
                Duration.ofSeconds(2),
                8,
                Duration.ofHours(1),
                Duration.ofMillis(10),
                3,
                DecodeErrorAction.DLQ)
            .withLobs(LobAssembler.Mode.INLINE, 1 << 20, true);
    return new CaptureEngine(
            Position.initial(1000, new DatabaseIdentity(1, 1)),
            fake,
            new LogInventory(catalog, CaptureMode.ONLINE, 1),
            () -> safeEnd,
            new HeapTransactionBuffer(),
            LobEngineTest.registry(),
            ChangeDecoder.rowDecoder(),
            sink,
            s,
            new OraErrorClassifier(),
            Set.of("APP"),
            () -> Set.of("APP"),
            cause -> {
              throw new AssertionError("unexpected reconnect", cause);
            },
            () -> Instant.EPOCH)
        .withDecodeThreads(threads)
        .withCapturedTables(DOCS::equals); // the DDL on OTHER is not a captured table's
  }

  private static void run(CaptureEngine e) throws Exception {
    for (int i = 0; i < 1000; i++) {
      if (e.runOnce() == CaptureEngine.Progress.IDLE) {
        return;
      }
    }
    throw new AssertionError("did not go idle");
  }
}
