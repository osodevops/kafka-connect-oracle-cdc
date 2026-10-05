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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.decode.RowDecoder;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.JdbcLogMinerSession;
import sh.oso.connect.oracle.core.mining.JdbcObjectCatalog;
import sh.oso.connect.oracle.core.mining.ObjectIdResolver;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;
import sh.oso.connect.oracle.core.mining.event.EventCursor;
import sh.oso.connect.oracle.core.mining.event.LogMinerEventSource;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.JdbcDictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P1-03 and P1-04: every 1.0 type inserted, updated and deleted, mined with the connector's session
 * settings, decoded through the dictionary-backed schema registry and compared with the values
 * written and with what JDBC reads back.
 */
@Tag("engine")
class TypeRoundTripEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void everyTypeDecodesToTheValueWritten() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      w.setAutoCommit(false);
      try (Statement s = w.createStatement()) {
        s.execute(
            "CREATE TABLE alltypes (id NUMBER PRIMARY KEY, c_varchar VARCHAR2(100), c_char"
                + " CHAR(10), c_nvarchar NVARCHAR2(100), c_nchar NCHAR(10), c_number NUMBER,"
                + " c_number_s NUMBER(10,2), c_float FLOAT, c_bf BINARY_FLOAT, c_bd BINARY_DOUBLE,"
                + " c_date DATE, c_ts TIMESTAMP(6), c_ts9 TIMESTAMP(9), c_tstz TIMESTAMP(6) WITH"
                + " TIME ZONE, c_tsltz TIMESTAMP(6) WITH LOCAL TIME ZONE, c_iym INTERVAL YEAR(4) TO"
                + " MONTH, c_ids INTERVAL DAY(5) TO SECOND(6), c_raw RAW(100))");
        s.execute("ALTER TABLE alltypes ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        s.execute("CREATE TABLE pkonly (id NUMBER PRIMARY KEY, v NUMBER, w NUMBER)");
        s.execute("ALTER TABLE pkonly ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS");
      }
      w.commit();
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(meta);

      try (Statement s = w.createStatement()) {
        s.execute(
            "INSERT INTO alltypes VALUES (1, 'plain ''quoted'' text', 'abc',"
                + " UNISTR('\\00FCn\\00EFcode \\2603 text'), UNISTR('nch\\00E1r'),"
                + " 1234567890.123456789, -42.5, 3.14159, 1.5E10f, 2.5d, TO_DATE('2026-02-28"
                + " 13:45:59', 'YYYY-MM-DD HH24:MI:SS'), TIMESTAMP '2026-03-29 01:30:00.123456',"
                + " TIMESTAMP '2026-03-29 01:30:00.123456789', TIMESTAMP '2026-03-29 01:30:00.5"
                + " +05:30', TIMESTAMP '2026-10-25 02:30:00.25 +00:00', INTERVAL '12-3' YEAR(4) TO"
                + " MONTH, INTERVAL '5 04:03:02.123456' DAY(5) TO SECOND(6),"
                + " HEXTORAW('deadbeef00ff'))");
        s.execute("INSERT INTO alltypes (id) VALUES (2)");
        s.execute("UPDATE alltypes SET c_number_s = 1, c_varchar = 'changed' WHERE id = 1");
        s.execute("DELETE FROM alltypes WHERE id = 2");
        s.execute("INSERT INTO pkonly VALUES (1, 10, 20)");
        s.execute("UPDATE pkonly SET v = 11 WHERE id = 1");
      }
      w.commit();
      OracleSql.archiveLogCurrent(db);
      long endScn = LogMinerHelper.currentScn(meta);

      // JDBC view of the committed row; temporal values are read zone-free because
      // java.sql.Timestamp
      // cannot represent a local time inside the JVM zone's daylight-saving gap
      Map<String, Object> jdbc;
      try (Statement s = w.createStatement();
          ResultSet rs =
              s.executeQuery(
                  "SELECT c_varchar, c_char, c_nvarchar, c_nchar, c_number, c_float, c_bf, c_bd,"
                      + " c_date, c_ts, c_ts9, c_tstz, c_raw FROM alltypes WHERE id = 1")) {
        assertThat(rs.next()).isTrue();
        jdbc =
            Map.ofEntries(
                Map.entry("C_VARCHAR", rs.getString(1)),
                Map.entry("C_CHAR", rs.getString(2)),
                Map.entry("C_NVARCHAR", rs.getString(3)),
                Map.entry("C_NCHAR", rs.getString(4)),
                Map.entry("C_NUMBER", rs.getBigDecimal(5)),
                Map.entry("C_FLOAT", rs.getBigDecimal(6)),
                Map.entry("C_BF", rs.getFloat(7)),
                Map.entry("C_BD", rs.getDouble(8)),
                Map.entry("C_DATE", rs.getObject(9, LocalDateTime.class)),
                Map.entry("C_TS", rs.getObject(10, LocalDateTime.class)),
                Map.entry("C_TS9", rs.getObject(11, LocalDateTime.class)),
                Map.entry("C_TSTZ", rs.getObject(12, OffsetDateTime.class)),
                Map.entry("C_RAW", rs.getBytes(13)));
      }

      ResolvedObjects objects =
          new ObjectIdResolver(
                  new JdbcObjectCatalog(meta),
                  List.of("FREEPDB1\\." + schema + "\\..*"),
                  List.of(),
                  List.of("FREEPDB1"),
                  false)
              .resolve();
      SchemaRegistry registry =
          new SchemaRegistry(
              new InMemorySchemaStore(),
              new JdbcDictionaryReader(meta),
              new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.FAIL));
      TableId allTypes = new TableId("FREEPDB1", schema, "ALLTYPES");
      TableSchema ts = registry.current(allTypes);
      assertThat(ts.columns()).hasSize(18);
      assertThat(ts.keyColumns()).containsExactly("ID");
      assertThat(ts.keySource()).isEqualTo(KeySource.PRIMARY_KEY);
      assertThat(ts.supplementalAllColumns()).isTrue();
      TableSchema pk = registry.current(new TableId("FREEPDB1", schema, "PKONLY"));
      assertThat(pk.supplementalAllColumns()).isFalse();
      assertThat(pk.supplementalPrimaryKey()).isTrue();

      List<RowChange> changes = new ArrayList<>();
      try (LogMinerEventSource source =
          new LogMinerEventSource(
              new LogInventory(new JdbcCatalogSource(meta), CaptureMode.ONLINE, 1),
              new JdbcLogMinerSession(mining, 1000, Duration.ofMinutes(5)),
              objects,
              objects.filter(Set.of(), 1000),
              DictionaryMode.ONLINE_CATALOG)) {
        try (EventCursor c =
            source.open(sh.oso.connect.oracle.core.mining.step.StepCursor.at(startScn), endScn)) {
          while (c.next()) {
            if (c.event() instanceof MiningEvent.Dml d && !d.undo()) {
              if (d.op() == Operation.UPDATE) {
                System.out.println("type-round-trip UPDATE SQL_REDO: " + d.sqlRedo());
              }
              changes.add(RowDecoder.decode(d, registry.current(d.table())));
            }
          }
        }
      }
      assertThat(changes)
          .extracting(RowChange::op)
          .containsExactly(
              Operation.INSERT,
              Operation.INSERT,
              Operation.UPDATE,
              Operation.DELETE,
              Operation.INSERT,
              Operation.UPDATE);

      Map<String, Object> after = changes.get(0).after();
      assertThat(after.keySet()).hasSize(18);
      assertThat(after.get("ID")).isEqualTo(new BigDecimal("1"));
      assertThat(after.get("C_VARCHAR")).isEqualTo("plain 'quoted' text");
      assertThat(after.get("C_CHAR")).isEqualTo("abc       ").isEqualTo(jdbc.get("C_CHAR"));
      assertThat(after.get("C_NVARCHAR"))
          .isEqualTo("ünïcode ☃ text")
          .isEqualTo(jdbc.get("C_NVARCHAR"));
      assertThat(after.get("C_NCHAR")).isEqualTo("nchár     ").isEqualTo(jdbc.get("C_NCHAR"));
      assertThat((BigDecimal) after.get("C_NUMBER"))
          .isEqualByComparingTo(new BigDecimal("1234567890.123456789"));
      assertThat((BigDecimal) after.get("C_NUMBER"))
          .isEqualByComparingTo((BigDecimal) jdbc.get("C_NUMBER"));
      assertThat((BigDecimal) after.get("C_NUMBER_S")).isEqualByComparingTo("-42.5");
      assertThat((BigDecimal) after.get("C_FLOAT"))
          .isEqualByComparingTo((BigDecimal) jdbc.get("C_FLOAT"));
      assertThat(after.get("C_BF")).isEqualTo(1.5E10f).isEqualTo(jdbc.get("C_BF"));
      assertThat(after.get("C_BD")).isEqualTo(2.5d).isEqualTo(jdbc.get("C_BD"));
      assertThat(after.get("C_DATE"))
          .isEqualTo(LocalDateTime.of(2026, 2, 28, 13, 45, 59))
          .isEqualTo(jdbc.get("C_DATE"));
      assertThat(after.get("C_TS"))
          .isEqualTo(LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_000))
          .isEqualTo(jdbc.get("C_TS"));
      assertThat(after.get("C_TS9"))
          .isEqualTo(LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_789))
          .isEqualTo(jdbc.get("C_TS9"));
      OffsetDateTime tz = (OffsetDateTime) after.get("C_TSTZ");
      assertThat(tz)
          .isEqualTo(
              OffsetDateTime.of(
                  2026, 3, 29, 1, 30, 0, 500_000_000, ZoneOffset.ofHoursMinutes(5, 30)));
      assertThat(tz.toInstant()).isEqualTo(((OffsetDateTime) jdbc.get("C_TSTZ")).toInstant());
      assertThat(after.get("C_TSLTZ")).isEqualTo(Instant.parse("2026-10-25T02:30:00.250Z"));
      assertThat(after.get("C_IYM")).isEqualTo(Period.of(12, 3, 0));
      assertThat(after.get("C_IDS"))
          .isEqualTo(
              Duration.ofDays(5).plusHours(4).plusMinutes(3).plusSeconds(2).plusNanos(123_456_000));
      assertThat((byte[]) after.get("C_RAW")).containsExactly(0xde, 0xad, 0xbe, 0xef, 0x00, 0xff);
      assertThat((byte[]) after.get("C_RAW")).isEqualTo((byte[]) jdbc.get("C_RAW"));

      RowChange nulls = changes.get(1);
      assertThat(nulls.after())
          .hasSize(18)
          .containsEntry("C_RAW", null)
          .containsEntry("C_TSTZ", null);

      RowChange upd = changes.get(2);
      assertThat(upd.partial()).isFalse();
      assertThat(upd.before()).hasSize(18);
      assertThat((BigDecimal) upd.before().get("C_NUMBER_S")).isEqualByComparingTo("-42.5");
      assertThat((BigDecimal) upd.after().get("C_NUMBER_S")).isEqualByComparingTo("1");
      // the JDBC read happened after the update, so it sees the changed value
      assertThat(upd.after().get("C_VARCHAR"))
          .isEqualTo("changed")
          .isEqualTo(jdbc.get("C_VARCHAR"));
      assertThat(upd.after().get("C_IYM")).isEqualTo(Period.of(12, 3, 0));

      RowChange del = changes.get(3);
      assertThat(del.after()).isNull();
      assertThat(del.before())
          .hasSize(18)
          .containsEntry("ID", new BigDecimal("2"))
          .containsEntry("C_DATE", null);
      assertThat(del.partial()).isFalse();

      RowChange pkUpdate = changes.get(5);
      assertThat(pkUpdate.table().table()).isEqualTo("PKONLY");
      assertThat(pkUpdate.partial()).isTrue();
      assertThat(pkUpdate.before().keySet()).containsExactly("ID", "V");
      assertThat(pkUpdate.after()).containsEntry("V", new BigDecimal("11")).doesNotContainKey("W");

      // PRD-02: a snapshot read of the same row yields exactly the values the stream decoded, so
      // a snapshot record and a change record of one row render the same. AS OF SCN needs the
      // SCN-to-time mapping past the CREATE TABLE (ORA-01466 otherwise).
      Thread.sleep(3500);
      try (Connection snap = db.capture(OracleTestDatabase.CDB_SERVICE)) {
        SessionInitializer.apply(snap, ConnectionRole.SNAPSHOT);
        sh.oso.connect.oracle.core.snapshot.JdbcSnapshotSource reader =
            new sh.oso.connect.oracle.core.snapshot.JdbcSnapshotSource(
                snap, 100, true, 1 << 20, true);
        List<sh.oso.connect.oracle.core.snapshot.SnapshotRow> rows =
            reader.read(
                ts,
                reader.kind(ts),
                sh.oso.connect.oracle.core.snapshot.ChunkRange.ALL,
                reader.currentScn(),
                null);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).values()).usingRecursiveComparison().isEqualTo(upd.after());
      }
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
