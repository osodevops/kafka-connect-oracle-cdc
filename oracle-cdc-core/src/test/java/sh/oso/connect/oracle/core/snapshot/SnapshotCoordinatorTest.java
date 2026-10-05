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
package sh.oso.connect.oracle.core.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.engine.EngineMetrics;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.SnapshotTooOldException;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.FakeSnapshotSource;

class SnapshotCoordinatorTest {

  static final TableId A = new TableId("FREEPDB1", "APP", "A");
  static final TableId B = new TableId("FREEPDB1", "APP", "B");
  static final TableId GONE = new TableId("FREEPDB1", "APP", "GONE");

  final FakeSnapshotSource fake = new FakeSnapshotSource();
  final EngineMetrics metrics = new EngineMetrics();
  SnapshotCoordinator c;

  static TableSchema schema(TableId t) {
    return new TableSchema(
        t,
        List.of(
            new ColumnSpec("ID", 1, OracleType.NUMBER, "NUMBER", 22, 0, 0, false),
            ColumnSpec.of("SQL", 2, OracleType.VARCHAR2)),
        List.of("ID"),
        KeySource.PRIMARY_KEY,
        true,
        false);
  }

  SnapshotCoordinator coordinator(
      List<TableId> tables, SnapshotProgress progress, int threads, int rows, int pending) {
    c =
        new SnapshotCoordinator(
            tables,
            progress,
            t -> t.equals(GONE) ? Optional.empty() : Optional.of(schema(t)),
            () -> fake,
            new SnapshotCoordinator.Settings(threads, rows, 2, pending, Map.of()),
            metrics,
            new OraErrorClassifier());
    return c;
  }

  @AfterEach
  void close() {
    if (c != null) {
      c.close();
    }
  }

  /** Takes every batch the sink would, as streaming passes {@code scn}. */
  List<SnapshotCoordinator.Batch> drain(long scn, int batches) throws Exception {
    List<SnapshotCoordinator.Batch> out = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 5000;
    while (out.size() < batches && System.currentTimeMillis() < deadline) {
      out.addAll(c.ready(scn));
      Thread.sleep(5);
    }
    return out;
  }

  @Test
  void batchesComeInKeyOrderAndWaitUntilStreamingPassesTheirScn() throws Exception {
    fake.rows(A, 1, 7).rows(B, 1, 1);
    fake.scnStep = 10; // every batch reads a later SCN
    coordinator(List.of(A, B, GONE), SnapshotProgress.begin(), 2, 1000, 100).start();
    Thread.sleep(200);
    assertThat(c.ready(5000)).as("streaming has not reached the first batch's SCN").isEmpty();
    List<SnapshotCoordinator.Batch> all = drain(Long.MAX_VALUE, 4);
    assertThat(all).extracting(b -> b.table().table()).containsExactly("A", "A", "B", "GONE");
    assertThat(all.subList(0, 3)).extracting(SnapshotCoordinator.Batch::scn).isSorted();
    // two chunks per batch (two threads), each of two rows, all chunks of a batch at one SCN
    assertThat(all.get(0).chunks()).hasSize(2);
    assertThat(all.get(1).chunks()).extracting(ch -> ch.rows().size()).containsExactly(2, 1);
    assertThat(all.get(1).tableDone()).isTrue();
    assertThat(all.get(0).tableDone()).isFalse();
    assertThat(all.get(3).schema()).as("a table that has gone").isNull();
    assertThat(all.get(3).last()).isTrue();
    assertThat(c.finished()).isTrue();
    List<Object> ids = new ArrayList<>();
    for (SnapshotCoordinator.Batch b : all.subList(0, 2)) {
      b.chunks().forEach(ch -> ch.rows().forEach(r -> ids.add(r.values().get("ID").toString())));
    }
    assertThat(ids).containsExactly("1", "2", "3", "4", "5", "6", "7");
    assertThat(metrics.snapshotChunksRead.get()).isEqualTo(5);
  }

  @Test
  void aStoredFrontierResumesTheTableAndDoneTablesAreSkipped() throws Exception {
    fake.rows(A, 1, 6).rows(B, 1, 3);
    SnapshotProgress progress =
        SnapshotProgress.begin().advance(B, null).advance(A, List.of("n:5"));
    coordinator(List.of(A, B), progress, 4, 1000, 100).start();
    List<SnapshotCoordinator.Batch> all = drain(Long.MAX_VALUE, 1);
    assertThat(c.tables()).containsExactly(A);
    assertThat(fake.reads).containsExactly("A [5,)@5000");
    assertThat(all.get(0).chunks().get(0).rows()).hasSize(2);
    assertThat(all.get(0).last()).isTrue();
  }

  @Test
  void readsPauseWhileTooManyChunksWaitForTheSink() throws Exception {
    fake.rows(A, 1, 20);
    coordinator(List.of(A), SnapshotProgress.begin(), 1, 1000, 2).start();
    Thread.sleep(300);
    assertThat(fake.reads).as("two chunks pending, then reads wait").hasSize(2);
    drain(Long.MAX_VALUE, 10);
    assertThat(fake.reads).hasSize(10);
    assertThat(c.finished()).isTrue();
  }

  @Test
  void snapshotTooOldHalvesTheChunksDownToTheFloorThenStops() throws Exception {
    fake.rows(A, 1, 4);
    for (int i = 0; i < 3; i++) {
      fake.readFaults.add(new SQLException("ORA-01555: snapshot too old", "72000", 1555));
    }
    coordinator(List.of(A), SnapshotProgress.begin(), 1, 4000, 100).start();
    Thread.sleep(500);
    assertThat(fake.planned).containsExactly(4000, 2000, 1000);
    assertThatThrownBy(() -> c.ready(Long.MAX_VALUE))
        .isInstanceOf(SnapshotTooOldException.class)
        .hasMessageContaining("CDC-8001")
        .hasMessageContaining("UNDO_RETENTION");
  }

  @Test
  void aFailedChunkIsReadAgainWithAFreshScnKeepingTheChunksBeforeIt() throws Exception {
    // dbz#2297 in PRD-02 SNAP-3: one failed chunk does not restart the table
    fake.rows(A, 1, 4);
    fake.scnStep = 1;
    coordinator(List.of(A), SnapshotProgress.begin(), 2, 1000, 100);
    // the batch's second chunk fails; its first is kept
    fake.faultAt.put("3", new SQLException("ORA-03113: end-of-file on channel", "08006", 3113));
    c.start();
    List<SnapshotCoordinator.Batch> all = drain(Long.MAX_VALUE, 2);
    List<String> ids = new ArrayList<>();
    all.forEach(
        b ->
            b.chunks()
                .forEach(ch -> ch.rows().forEach(r -> ids.add(r.values().get("ID").toString()))));
    assertThat(ids).containsExactly("1", "2", "3", "4");
    assertThat(metrics.snapshotChunkRetries.get()).isEqualTo(1);
    assertThat(all.get(1).scn()).isGreaterThan(all.get(0).scn());
  }

  @Test
  void retriesRunOutAndTheErrorStopsTheSnapshot() throws Exception {
    fake.rows(A, 1, 4);
    for (int i = 0; i < 5; i++) {
      fake.readFaults.add(new SQLException("ORA-03113: end-of-file on channel", "08006", 3113));
    }
    coordinator(List.of(A), SnapshotProgress.begin(), 1, 1000, 100).start();
    Thread.sleep(500);
    assertThatThrownBy(() -> c.ready(Long.MAX_VALUE)).hasMessageContaining("ORA-03113");
  }

  @Test
  void progressKeepsOneFrontierPerTableAndRoundTripsThroughTheOffset() {
    SnapshotProgress p = SnapshotProgress.begin();
    assertThat(p.untouched()).isTrue();
    p = p.advance(A, List.of("n:5", "s:x")).advance(B, null);
    assertThat(p.frontier(A)).containsExactly("n:5", "s:x");
    assertThat(p.done(B)).isTrue();
    assertThat(p.done(A)).isFalse();
    assertThat(SnapshotProgress.of(p.toMap())).isEqualTo(p);
    SnapshotProgress done = p.completed();
    assertThat(done.done(A)).isTrue();
    assertThat(SnapshotProgress.of(done.toMap()).complete()).isTrue();
    assertThat(SnapshotProgress.of(null)).isNull();
  }

  @Test
  void compositeKeyBoundsSpellOutTheLexicographicOrder() {
    List<String> params = new ArrayList<>();
    String lower =
        JdbcSnapshotSource.bound(
            List.of("A", "B", "C"), List.of("n:1", "s:x", "n:3"), true, params);
    assertThat(lower)
        .isEqualTo(
            "((\"A\" > ?) OR (\"A\" = ? AND \"B\" > ?) OR (\"A\" = ? AND \"B\" = ? AND \"C\" >="
                + " ?))");
    assertThat(params).containsExactly("n:1", "n:1", "s:x", "n:1", "s:x", "n:3");
    params.clear();
    assertThat(JdbcSnapshotSource.bound(List.of("ID"), List.of("n:9"), false, params))
        .isEqualTo("((\"ID\" < ?))");
  }

  @Test
  void anUnreadableTableIsSkippedWithAReasonWhenThePolicyAllows() throws Exception {
    // the reader refuses A's column types before any query, as JdbcSnapshotSource does
    FakeSnapshotSource refusing =
        new FakeSnapshotSource() {
          @Override
          public List<SnapshotRow> read(
              TableSchema s, Kind kind, ChunkRange range, long at, String where)
              throws SQLException {
            if (s.table().equals(A)) {
              throw new sh.oso.connect.oracle.core.errors.DecodeException(
                  "Column OK has type BOOLEAN which the snapshot reader does not support", "x");
            }
            return super.read(s, kind, range, at, where);
          }
        };
    refusing.rows(B, 1, 1);
    c =
        new SnapshotCoordinator(
            List.of(A, B),
            SnapshotProgress.begin(),
            t -> Optional.of(schema(t)),
            () -> refusing,
            new SnapshotCoordinator.Settings(1, 1000, 2, 100, Map.of(), true),
            metrics,
            new OraErrorClassifier());
    c.start();
    List<SnapshotCoordinator.Batch> all = drain(Long.MAX_VALUE, 2);
    assertThat(all.get(0).table()).isEqualTo(A);
    assertThat(all.get(0).skipped()).contains("BOOLEAN");
    assertThat(all.get(0).tableDone()).isTrue();
    assertThat(all.get(1).table()).isEqualTo(B);
    assertThat(all.get(1).chunks().get(0).rows()).hasSize(1);
  }
}
