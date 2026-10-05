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
package sh.oso.connect.oracle.e2e.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.JdbcDictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.snapshot.ChunkRange;
import sh.oso.connect.oracle.core.snapshot.JdbcSnapshotSource;
import sh.oso.connect.oracle.core.snapshot.SnapshotRow;
import sh.oso.connect.oracle.core.snapshot.SnapshotSource;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-02 and ADR-0004 against Oracle Database Free: chunks planned by a composite key, by ROWID for
 * a keyless heap table and by key for an index-organised table cover every row exactly once,
 * resuming from any chunk bound.
 */
@Tag("engine")
class SnapshotChunksEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void chunksCoverEveryRowOnceWhateverTheKey() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection snap = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      SessionInitializer.apply(snap, ConnectionRole.SNAPSHOT);
      try (Statement s = w.createStatement()) {
        s.execute(
            "CREATE TABLE comp (a NUMBER NOT NULL, b VARCHAR2(10) NOT NULL, c DATE NOT NULL,"
                + " v NUMBER, PRIMARY KEY (a, b, c))");
        s.execute(
            "INSERT INTO comp SELECT MOD(LEVEL, 7), 'k' || MOD(LEVEL, 13), DATE '2026-01-01' +"
                + " LEVEL, LEVEL FROM dual CONNECT BY LEVEL <= 2500");
        s.execute("CREATE TABLE heap (v NUMBER, pad VARCHAR2(200))");
        s.execute(
            "INSERT INTO heap SELECT LEVEL, RPAD('x', 200, 'x') FROM dual CONNECT BY LEVEL <="
                + " 3000");
        s.execute("CREATE TABLE iot (id NUMBER PRIMARY KEY, v VARCHAR2(20)) ORGANIZATION INDEX");
        s.execute("INSERT INTO iot SELECT LEVEL, 'v' || LEVEL FROM dual CONNECT BY LEVEL <= 1500");
        s.execute("COMMIT");
      }
      Thread.sleep(3500); // flashback reads need the SCN-to-time mapping past the CREATE TABLE
      SchemaRegistry registry =
          new SchemaRegistry(
              new InMemorySchemaStore(),
              new JdbcDictionaryReader(() -> meta),
              new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.ROWID));
      JdbcSnapshotSource source = new JdbcSnapshotSource(snap, 500, false, 1 << 20, true);

      TableSchema comp = registry.current(new TableId("FREEPDB1", schema, "COMP"));
      assertThat(source.kind(comp)).isEqualTo(SnapshotSource.Kind.KEY);
      List<List<SnapshotRow>> compChunks = readAll(source, comp, 300);
      assertThat(compChunks.size()).isBetween(8, 10);
      assertThat(distinct(compChunks, r -> r.values().get("V").toString())).isEqualTo(2500);

      TableSchema heap = registry.current(new TableId("FREEPDB1", schema, "HEAP"));
      assertThat(source.kind(heap)).isEqualTo(SnapshotSource.Kind.ROWID);
      List<List<SnapshotRow>> heapChunks = readAll(source, heap, 500);
      assertThat(heapChunks.size()).isGreaterThan(1);
      assertThat(distinct(heapChunks, SnapshotRow::rowId)).isEqualTo(3000);
      assertThat(distinct(heapChunks, r -> r.values().get("V").toString())).isEqualTo(3000);

      TableSchema iot = registry.current(new TableId("FREEPDB1", schema, "IOT"));
      assertThat(source.kind(iot)).isEqualTo(SnapshotSource.Kind.KEY);
      assertThat(distinct(readAll(source, iot, 400), r -> r.values().get("ID").toString()))
          .isEqualTo(1500);
      System.out.println(
          "snapshot-chunks: composite "
              + compChunks.size()
              + " chunks, keyless "
              + heapChunks.size()
              + " ROWID chunks");
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /** Plans and reads a whole table two chunks at a time, each batch as of a fresh SCN. */
  private static List<List<SnapshotRow>> readAll(
      JdbcSnapshotSource source, TableSchema schema, int chunkRows) throws Exception {
    SnapshotSource.Kind kind = source.kind(schema);
    List<List<SnapshotRow>> out = new ArrayList<>();
    List<String> lower = null;
    while (true) {
      List<ChunkRange> ranges = source.plan(schema, kind, lower, 2, chunkRows);
      long scn = source.currentScn();
      for (ChunkRange r : ranges) {
        out.add(source.read(schema, kind, r, scn, null));
      }
      ChunkRange last = ranges.get(ranges.size() - 1);
      if (last.last()) {
        return out;
      }
      lower = last.upper();
    }
  }

  /** Distinct values across all chunks, asserting no row was read twice. */
  private static int distinct(
      List<List<SnapshotRow>> chunks, java.util.function.Function<SnapshotRow, String> id) {
    Set<String> seen = new HashSet<>();
    int total = 0;
    for (List<SnapshotRow> c : chunks) {
      for (SnapshotRow r : c) {
        seen.add(id.apply(r));
        total++;
      }
    }
    assertThat(total).as("rows read").isEqualTo(seen.size());
    return seen.size();
  }
}
