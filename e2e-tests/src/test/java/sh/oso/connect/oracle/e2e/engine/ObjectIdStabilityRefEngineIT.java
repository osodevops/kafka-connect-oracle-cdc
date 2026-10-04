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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * ADR-0001 spike: which identifiers in V$LOGMNR_CONTENTS (DATA_OBJ#, DATA_OBJD#, DATA_OBJV#) and in
 * DBA_OBJECTS (OBJECT_ID, DATA_OBJECT_ID) change when a DDL alters a table's physical identity. A
 * row written after such a DDL but inside the same mining step would be filtered out server-side by
 * an id list resolved before the DDL; the engine must cut the step at the DDL and re-resolve.
 */
@Tag("engine")
class ObjectIdStabilityRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void identityChangingDdlIsRecordedPerIdentifier() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    List<List<String>> matrix = new ArrayList<>();
    List<List<String>> meaning = new ArrayList<>();
    Set<String> kindsSeen = new HashSet<>();
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      w.setAutoCommit(true);
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE h (id NUMBER PRIMARY KEY, v VARCHAR2(20)) ENABLE ROW MOVEMENT");
        s.execute("ALTER TABLE h ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        s.execute(
            "CREATE TABLE p (id NUMBER PRIMARY KEY, v VARCHAR2(20)) PARTITION BY RANGE (id)"
                + " (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (200),"
                + " PARTITION p3 VALUES LESS THAN (300)) ENABLE ROW MOVEMENT");
        s.execute("ALTER TABLE p ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        s.execute("CREATE TABLE i (id NUMBER PRIMARY KEY, v VARCHAR2(20)) ORGANIZATION INDEX");
        s.execute("ALTER TABLE i ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        s.execute("CREATE TABLE x (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
        s.execute("ALTER TABLE x ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(root);

      // name, table, ddl, segment compared in DBA_OBJECTS, id of the marker inserted after the DDL
      List<String[]> cases =
          List.of(
              new String[] {
                "heap: ALTER TABLE ADD column", "h", "ALTER TABLE h ADD (extra NUMBER)", "H", "1"
              },
              new String[] {"heap: TRUNCATE TABLE", "h", "TRUNCATE TABLE h", "H", "2"},
              new String[] {"heap: ALTER TABLE MOVE", "h", "ALTER TABLE h MOVE", "H", "3"},
              new String[] {
                "heap: ALTER TABLE SHRINK SPACE", "h", "ALTER TABLE h SHRINK SPACE", "H", "4"
              },
              new String[] {
                "partition: TRUNCATE PARTITION p1 (marker stays in p1)",
                "p",
                "ALTER TABLE p TRUNCATE PARTITION p1",
                "P1",
                "11"
              },
              new String[] {
                "partition: SPLIT PARTITION p2 (marker into new p2b)",
                "p",
                "ALTER TABLE p SPLIT PARTITION p2 AT (150) INTO (PARTITION p2a, PARTITION p2b)",
                "P1",
                "160"
              },
              new String[] {
                "partition: MERGE PARTITIONS p2a, p2b (marker into merged p2)",
                "p",
                "ALTER TABLE p MERGE PARTITIONS p2a, p2b INTO PARTITION p2",
                "P1",
                "161"
              },
              new String[] {
                "partition: ADD PARTITION p4 (marker into new p4)",
                "p",
                "ALTER TABLE p ADD PARTITION p4 VALUES LESS THAN (400)",
                "P1",
                "350"
              },
              new String[] {
                "partition: DROP PARTITION p4 (marker stays in p1)",
                "p",
                "ALTER TABLE p DROP PARTITION p4",
                "P1",
                "12"
              },
              new String[] {
                "partition: EXCHANGE PARTITION p3 WITH TABLE x (marker into exchanged p3)",
                "p",
                "ALTER TABLE p EXCHANGE PARTITION p3 WITH TABLE x",
                "P1",
                "250"
              },
              new String[] {
                "partition: ALTER TABLE MOVE PARTITION p1 (marker stays in p1)",
                "p",
                "ALTER TABLE p MOVE PARTITION p1",
                "P1",
                "13"
              },
              new String[] {
                "iot: ALTER TABLE ADD column", "i", "ALTER TABLE i ADD (extra NUMBER)", "I", "1"
              },
              new String[] {"iot: TRUNCATE TABLE", "i", "TRUNCATE TABLE i", "I", "2"},
              new String[] {"iot: ALTER TABLE MOVE", "i", "ALTER TABLE i MOVE", "I", "3"});
      Map<String, long[]> before = new LinkedHashMap<>();
      Map<String, long[]> after = new LinkedHashMap<>();
      Map<String, Map<String, String>> dictBefore = new LinkedHashMap<>();
      Map<String, Map<String, String>> dictAfter = new LinkedHashMap<>();
      int heapMarker = 100; // before-markers for h and i
      int partMarker = 20; // before-markers for p stay in p1
      for (String[] c : cases) {
        String name = c[0];
        String table = c[1];
        String ddl = c[2];
        String seg = c[3];
        int afterId = Integer.parseInt(c[4]);
        dictBefore.put(name, dictionaryIds(w, table.toUpperCase(), seg));
        long b0 = LogMinerHelper.currentScn(root);
        insertMarker(w, table, "p".equals(table) ? ++partMarker : ++heapMarker);
        before.put(name, new long[] {b0, LogMinerHelper.currentScn(root)});
        try (Statement s = w.createStatement()) {
          s.execute(ddl);
        }
        rebuildUnusableIndexes(w);
        dictAfter.put(name, dictionaryIds(w, table.toUpperCase(), seg));
        long a0 = LogMinerHelper.currentScn(root);
        insertMarker(w, table, afterId);
        after.put(name, new long[] {a0, LogMinerHelper.currentScn(root)});
      }
      OracleSql.archiveLogCurrent(db);
      long end = LogMinerHelper.currentScn(root);

      LogMinerHelper.start(root, start, end);
      // by user, excluding Oracle's own recursive inserts (SYS.SEG$ on deferred segment creation);
      // rows into segments that a later DDL dropped have no SEG_OWNER or TABLE_NAME
      List<Map<String, String>> rows =
          LogMinerHelper.rows(
              root,
              "USERNAME = '"
                  + schema
                  + "' AND OPERATION = 'INSERT' AND (SEG_OWNER IS NULL OR SEG_OWNER <> 'SYS')");
      LogMinerHelper.end(root);
      assertThat(rows).isNotEmpty();

      for (String[] c : cases) {
        String name = c[0];
        Map<String, String> b = firstIn(rows, before.get(name));
        Map<String, String> a = firstIn(rows, after.get(name));
        if (b == null || a == null) {
          matrix.add(List.of(name, "marker row not mined", "", "", "", "", ""));
          continue;
        }
        Map<String, String> db0 = dictBefore.get(name);
        Map<String, String> db1 = dictAfter.get(name);
        matrix.add(
            List.of(
                name,
                same(b.get("DATA_OBJ#"), a.get("DATA_OBJ#")),
                same(b.get("DATA_OBJD#"), a.get("DATA_OBJD#")),
                same(b.get("DATA_OBJV#"), a.get("DATA_OBJV#")),
                same(db0.get("object_id"), db1.get("object_id")),
                same(db0.get("data_object_id"), db1.get("data_object_id")),
                segName(a.get("SEG_NAME"))));
        if (!kindsSeen.contains(c[1])) {
          kindsSeen.add(c[1]);
          Map<String, String> tbl = dictionaryIds(w, c[1].toUpperCase(), c[1].toUpperCase());
          Map<String, String> iot = iotIndexIds(w, c[1].toUpperCase());
          meaning.add(
              List.of(
                  name.split(":")[0],
                  describe(b.get("DATA_OBJ#"), tbl, db0, iot),
                  describe(b.get("DATA_OBJD#"), tbl, db0, iot),
                  segName(b.get("SEG_NAME"))));
        }
      }

      new ReferenceDoc(
              "object-id-stability",
              "Object identifiers across identity-changing DDL",
              "For each DDL, a marker row was inserted before and after it, then mined with the"
                  + " online catalog. Each cell says whether the identifier of the after row equals"
                  + " the before row (`same`) or not (`changed`). This decides how the mining query"
                  + " filters by object and when a mining step must be cut and re-resolved (PRD-00"
                  + " CORE-MINE-3, ADR-0001).")
          .section("What DATA_OBJ# and DATA_OBJD# mean")
          .paragraph(
              "The before-marker row of the first case of each table kind, compared with"
                  + " DBA_OBJECTS identifiers of the table, of the partition the row landed in, and"
                  + " of the IOT top index.")
          .table(
              List.of("Table kind", "DATA_OBJ# equals", "DATA_OBJD# equals", "SEG_NAME"), meaning)
          .section("Stability matrix")
          .table(
              List.of(
                  "DDL",
                  "DATA_OBJ#",
                  "DATA_OBJD#",
                  "DATA_OBJV#",
                  "DBA_OBJECTS.OBJECT_ID",
                  "DBA_OBJECTS.DATA_OBJECT_ID",
                  "SEG_NAME after"),
              matrix)
          .section("Reading the matrix")
          .bullet(
              "DATA_OBJ# is the logical OBJECT_ID of the segment written (the partition for a"
                  + " partitioned table, the top index for an IOT) and survives TRUNCATE, MOVE and"
                  + " partition maintenance of existing partitions. DATA_OBJD# is the physical"
                  + " DATA_OBJECT_ID and changes whenever the segment is rebuilt; it is not used"
                  + " for filtering.")
          .bullet(
              "Rows written into a partition that a DDL created or exchanged (SPLIT, MERGE, ADD,"
                  + " EXCHANGE) carry a new DATA_OBJ#. A server-side `DATA_OBJ# IN (...)` filter"
                  + " resolved before that DDL silently drops them until the list is refreshed, so"
                  + " the step must be cut at such a DDL row and the ids re-resolved (ADR-0001)."
                  + " The same applies to CREATE TABLE for a table matching the include patterns.")
          .bullet(
              "Rows into a segment that was later dropped (SPLIT then MERGE, ADD then DROP) have no"
                  + " SEG_OWNER, SEG_NAME or TABLE_NAME under the online catalog: the id is in the"
                  + " redo, the name is not. Filtering by name would lose them; the id resolved at"
                  + " the time of the DDL keeps them.")
          .bullet(
              "DATA_OBJV# increments on structural DDL (ADD column) and is the hint that the schema"
                  + " version for decoding has changed (PRD-03).")
          .assertUpToDate();
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /** MOVE, SHRINK, partition maintenance and EXCHANGE leave indexes unusable; rebuild them. */
  private static void rebuildUnusableIndexes(Connection w) throws SQLException {
    try (Statement s = w.createStatement()) {
      List<String> stmts = new ArrayList<>();
      try (ResultSet rs =
          s.executeQuery("SELECT index_name FROM user_indexes WHERE status = 'UNUSABLE'")) {
        while (rs.next()) {
          stmts.add("ALTER INDEX " + rs.getString(1) + " REBUILD");
        }
      }
      try (ResultSet rs =
          s.executeQuery(
              "SELECT index_name, partition_name FROM user_ind_partitions WHERE status ="
                  + " 'UNUSABLE'")) {
        while (rs.next()) {
          stmts.add("ALTER INDEX " + rs.getString(1) + " REBUILD PARTITION " + rs.getString(2));
        }
      }
      for (String stmt : stmts) {
        s.execute(stmt);
      }
    }
  }

  private static void insertMarker(Connection w, String table, int id) throws SQLException {
    try (Statement s = w.createStatement()) {
      s.execute("INSERT INTO " + table + " (id, v) VALUES (" + id + ", 'm" + id + "')");
    }
  }

  private static Map<String, String> firstIn(List<Map<String, String>> rows, long[] range) {
    for (Map<String, String> r : rows) {
      long scn = Long.parseLong(r.get("SCN"));
      if (scn >= range[0] && scn <= range[1]) {
        return r;
      }
    }
    return null;
  }

  /** Object numbers differ between databases; keep the shape, drop the number. */
  private static String segName(String seg) {
    if (seg == null) {
      return "(unresolved: segment no longer in the dictionary)";
    }
    return seg.replaceAll("OBJ# \\d+", "OBJ# <n> (dropped segment, name unresolvable)")
        .replaceAll("SYS_IOT_TOP_\\d+", "SYS_IOT_TOP_<n>");
  }

  private static String same(String a, String b) {
    return a != null && a.equals(b) ? "same" : "changed";
  }

  /** Names which dictionary identifier a LogMiner id equals. */
  private static String describe(
      String id, Map<String, String> table, Map<String, String> segment, Map<String, String> iot) {
    if (id == null) {
      return "(null)";
    }
    List<String> hits = new ArrayList<>();
    if (id.equals(table.get("object_id"))) {
      hits.add("table OBJECT_ID");
    }
    if (id.equals(table.get("data_object_id"))) {
      hits.add("table DATA_OBJECT_ID");
    }
    if (!segment.isEmpty() && !segment.get("object_id").equals(table.get("object_id"))) {
      if (id.equals(segment.get("object_id"))) {
        hits.add("partition OBJECT_ID");
      }
      if (id.equals(segment.get("data_object_id"))) {
        hits.add("partition DATA_OBJECT_ID");
      }
    }
    if (id.equals(iot.get("object_id"))) {
      hits.add("IOT top index OBJECT_ID");
    }
    if (id.equals(iot.get("data_object_id"))) {
      hits.add("IOT top index DATA_OBJECT_ID");
    }
    return hits.isEmpty() ? "none of the compared identifiers" : String.join(" and ", hits);
  }

  /** OBJECT_ID and DATA_OBJECT_ID of the segment named seg (table or partition). */
  private static Map<String, String> dictionaryIds(Connection w, String table, String seg)
      throws SQLException {
    Map<String, String> out = new LinkedHashMap<>();
    String sql =
        seg.equals(table)
            ? "SELECT object_id, data_object_id FROM user_objects WHERE object_name = ? AND"
                + " object_type = 'TABLE'"
            : "SELECT object_id, data_object_id FROM user_objects WHERE object_name = ? AND"
                + " subobject_name = '"
                + seg
                + "' AND object_type = 'TABLE PARTITION'";
    try (PreparedStatement ps = w.prepareStatement(sql)) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          out.put("object_id", rs.getString(1));
          out.put("data_object_id", rs.getString(2));
        }
      }
    }
    return out;
  }

  /** OBJECT_ID and DATA_OBJECT_ID of the IOT's top index, if the table is index organised. */
  private static Map<String, String> iotIndexIds(Connection w, String table) throws SQLException {
    Map<String, String> out = new LinkedHashMap<>();
    try (PreparedStatement ps =
        w.prepareStatement(
            "SELECT o.object_id, o.data_object_id FROM user_indexes i JOIN user_objects o ON"
                + " o.object_name = i.index_name WHERE i.table_name = ? AND i.index_type = 'IOT -"
                + " TOP'")) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          out.put("object_id", rs.getString(1));
          out.put("data_object_id", rs.getString(2));
        }
      }
    }
    return out;
  }
}
