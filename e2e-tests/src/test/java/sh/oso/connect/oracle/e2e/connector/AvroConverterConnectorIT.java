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
package sh.oso.connect.oracle.e2e.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import sh.oso.connect.oracle.e2e.support.AvroValues;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * ADR-0020 and the Phase 1 exit criterion "a Debezium consumer reads our output without change for
 * every type, Avro and JSON": a Connect worker whose connector converts keys and values with
 * Apicurio Registry's AvroConverter (auto-registration on, registry in memory) carries every column
 * type of TypeRoundTripEngineIT end to end. A plain Kafka consumer with the Apicurio Avro
 * deserializer reads each insert, update, delete and tombstone back with the Debezium envelope, and
 * every value equals what the database holds, read and compared as TypeRoundTripEngineIT does.
 *
 * <p>The table and most of its columns have names Avro refuses ({@code #}, {@code $}, a quoted
 * lower-case table name, a space, a slash, a hyphen, a leading digit) and the topic prefix has a
 * hyphen, so with {@code cdc.schema.name.adjustment.mode=avro} and {@code
 * cdc.field.name.adjustment.mode=avro} the records carry the adjusted names. With both modes at
 * {@code none} the names are refused visibly, though not in the same place: a column name fails the
 * task in the converter before anything of the table is written, while a table name passes the
 * converter and the registry (Avro does not check a namespace when it builds a schema, and the
 * registry applies no validity rule by default) and is refused by the consumer, whose Avro parser
 * checks namespaces (Avro for Java does from release 1.12; older parsers read such a schema).
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AvroConverterConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final String AVRO_CONVERTER = "io.apicurio.registry.utils.converter.AvroConverter";

  /** A hyphen, which Avro refuses in a namespace, so the schema names need adjustment too. */
  static final String PREFIX = "avro-cdc";

  /** Debezium's average month for INTERVAL YEAR TO MONTH as microseconds: 365.25 / 12 days. */
  static final double MICROS_PER_MONTH = 365.25 / 12 * 24 * 60 * 60 * 1_000_000d;

  /** The row fields under cdc.field.name.adjustment.mode=avro, in column order. */
  static final List<String> ADJUSTED_FIELDS =
      List.of(
          "ID_",
          "c_varchar_",
          "C_CHAR",
          "C_NVARCHAR",
          "c_nchar",
          "C_NUMBER",
          "C_NUMBER_S",
          "C_FLOAT",
          "C_BF",
          "C_BD",
          "C_DATE",
          "C_TS",
          "C_TS9",
          "C_TSTZ",
          "C_TSLTZ",
          "C_IYM",
          "C_IDS",
          "C_RAW",
          "_2ND_VALUE");

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      // TypeRoundTripEngineIT's ALLTYPES under names Avro refuses, plus a column with a leading
      // digit; every name stays within the 30 characters LogMiner reads
      s.execute(
          "CREATE TABLE \"avro#types$\" (\"ID#\" NUMBER PRIMARY KEY, \"c_varchar$\" VARCHAR2(100),"
              + " \"C CHAR\" CHAR(10), \"C/NVARCHAR\" NVARCHAR2(100), \"c-nchar\" NCHAR(10),"
              + " C_NUMBER NUMBER, \"C_NUMBER#S\" NUMBER(10,2), C_FLOAT FLOAT, C_BF BINARY_FLOAT,"
              + " C_BD BINARY_DOUBLE, C_DATE DATE, C_TS TIMESTAMP(6), C_TS9 TIMESTAMP(9), C_TSTZ"
              + " TIMESTAMP(6) WITH TIME ZONE, C_TSLTZ TIMESTAMP(6) WITH LOCAL TIME ZONE, C_IYM"
              + " INTERVAL YEAR(4) TO MONTH, C_IDS INTERVAL DAY(5) TO SECOND(6), C_RAW RAW(100),"
              + " \"2ND_VALUE\" VARCHAR2(20))");
      s.execute("ALTER TABLE \"avro#types$\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      // primary-key-only logging: the before image of an update is partial
      s.execute("CREATE TABLE pkonly (id NUMBER PRIMARY KEY, v NUMBER, w NUMBER)");
      s.execute("ALTER TABLE pkonly ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS");
      // for the modes at none: a key column whose name Avro refuses, and a table whose name it
      // refuses with plain columns
      s.execute("CREATE TABLE \"ORDER#\" (\"ID#\" NUMBER(9) PRIMARY KEY, v VARCHAR2(10))");
      s.execute("ALTER TABLE \"ORDER#\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("CREATE TABLE \"ITEM#\" (id NUMBER(9) PRIMARY KEY, v VARCHAR2(10))");
      s.execute("ALTER TABLE \"ITEM#\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }
    cluster =
        new ConnectCluster()
            .withSchemaRegistry()
            .withPlugin(ConnectCluster.apicurioConverterDir(), "apicurio-converter")
            .start();
  }

  @AfterAll
  void down() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
  }

  /** Connector config with the Avro converter for keys and values, as a connector override. */
  private Map<String, String> config(String prefix, String include, String adjustment) {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", prefix);
    c.put("cdc.tables.include", include);
    c.put("cdc.snapshot.mode", "none");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.schema.name.adjustment.mode", adjustment);
    c.put("cdc.field.name.adjustment.mode", adjustment);
    for (String side : List.of("key", "value")) {
      c.put(side + ".converter", AVRO_CONVERTER);
      c.put(side + ".converter.apicurio.registry.url", ConnectCluster.SCHEMA_REGISTRY_URL);
      c.put(side + ".converter.apicurio.registry.auto-register", "true");
    }
    return c;
  }

  private AvroKafkaDeserializer<Object> deserializer(boolean key) {
    AvroKafkaDeserializer<Object> d = new AvroKafkaDeserializer<>();
    d.configure(Map.of("apicurio.registry.url", cluster.schemaRegistryUrl()), key);
    return d;
  }

  /** A record as it was on the topic and as the Apicurio Avro deserializer reads it. */
  record Read(ConsumerRecord<byte[], byte[]> raw, GenericRecord key, GenericRecord value) {
    String op() {
      return value == null ? "tombstone" : value.get("op").toString();
    }

    Map<String, Object> row(String image) {
      return AvroValues.row(value.get(image));
    }

    Object keyField(String name) {
      return AvroValues.field(key, name);
    }
  }

  private static List<Read> read(
      List<ConsumerRecord<byte[], byte[]>> records,
      String topic,
      AvroKafkaDeserializer<Object> keys,
      AvroKafkaDeserializer<Object> values) {
    List<Read> out = new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> r : records) {
      if (!r.topic().equals(topic)) {
        continue;
      }
      GenericRecord k =
          r.key() == null
              ? null
              : (GenericRecord) keys.deserialize(r.topic(), r.headers(), r.key());
      GenericRecord v =
          r.value() == null
              ? null
              : (GenericRecord) values.deserialize(r.topic(), r.headers(), r.value());
      out.add(new Read(r, k, v));
    }
    return out;
  }

  @Test
  void everyTypeReachesAnAvroConsumerUnderAdjustedNames() throws Exception {
    String name = "avro-adjusted";
    cluster.register(
        name, config(PREFIX, "FREEPDB1\\." + schema + "\\.(avro#types\\$|PKONLY)", "avro"));
    try {
      cluster.awaitRunning(name, Duration.ofMinutes(2));
      // the start heartbeat, itself written through the Avro converter, makes the start SCN
      // durable before any change (SRC-HB-1)
      cluster.awaitOffsets(name, Duration.ofSeconds(90));
      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        w.setAutoCommit(false);
        try (Statement s = w.createStatement()) {
          s.execute(
              "INSERT INTO \"avro#types$\" VALUES (1, 'plain ''quoted'' text', 'abc',"
                  + " UNISTR('\\00FCn\\00EFcode \\2603 text'), UNISTR('nch\\00E1r'),"
                  + " 1234567890.123456789, -42.5, 3.14159, 1.5E10f, 2.5d, TO_DATE('2026-02-28"
                  + " 13:45:59', 'YYYY-MM-DD HH24:MI:SS'), TIMESTAMP '2026-03-29 01:30:00.123456',"
                  + " TIMESTAMP '2026-03-29 01:30:00.123456789', TIMESTAMP '2026-03-29 01:30:00.5"
                  + " +05:30', TIMESTAMP '2026-10-25 02:30:00.25 +00:00', INTERVAL '12-3' YEAR(4)"
                  + " TO MONTH, INTERVAL '5 04:03:02.123456' DAY(5) TO SECOND(6),"
                  + " HEXTORAW('deadbeef00ff'), 'second')");
          s.execute("INSERT INTO \"avro#types$\" (\"ID#\") VALUES (2)");
          s.execute(
              "UPDATE \"avro#types$\" SET \"C_NUMBER#S\" = 1, \"c_varchar$\" = 'changed'"
                  + " WHERE \"ID#\" = 1");
          s.execute("DELETE FROM \"avro#types$\" WHERE \"ID#\" = 2");
          s.execute("INSERT INTO pkonly VALUES (1, 10, 20)");
          s.execute("UPDATE pkonly SET v = 11 WHERE id = 1");
        }
        w.commit();
      }

      String typesTopic = PREFIX + ".FREEPDB1." + schema + ".avro_types_";
      String pkTopic = PREFIX + ".FREEPDB1." + schema + ".PKONLY";
      AvroKafkaDeserializer<Object> keys = deserializer(true);
      AvroKafkaDeserializer<Object> values = deserializer(false);
      try (KafkaConsumer<byte[], byte[]> c =
          cluster.byteConsumer("avro-adjusted-reader", typesTopic, pkTopic)) {
        List<ConsumerRecord<byte[], byte[]>> all =
            ConnectCluster.consume(c, 7, Duration.ofMinutes(3), Duration.ofSeconds(3));
        List<Read> types = read(all, typesTopic, keys, values);
        List<Read> pk = read(all, pkTopic, keys, values);
        assertThat(types)
            .as("records of %s: %s", typesTopic, types)
            .extracting(Read::op)
            .containsExactly("c", "c", "u", "d", "tombstone");
        assertThat(pk).as("records of %s", pkTopic).extracting(Read::op).containsExactly("c", "u");

        assertAdjustedNames(types.get(0));
        assertEnvelope(types.get(0), "c", 1);
        assertEnvelope(types.get(2), "u", 3);
        assertEnvelope(types.get(3), "d", 4);
        assertFirstInsert(types.get(0).row("after"));
        assertThat(types.get(0).row("before")).isNull();

        // the insert of a row of nulls
        Map<String, Object> nulls = types.get(1).row("after");
        assertThat(nulls).hasSize(ADJUSTED_FIELDS.size());
        assertThat(nulls.entrySet())
            .filteredOn(e -> e.getValue() != null)
            .extracting(e -> e.getKey())
            .containsExactly("ID_");
        assertThat((BigDecimal) nulls.get("ID_")).isEqualByComparingTo("2");

        // the update carries the whole before image (ALL COLUMNS logging)
        Map<String, Object> before = types.get(2).row("before");
        Map<String, Object> after = types.get(2).row("after");
        assertThat(before).hasSize(ADJUSTED_FIELDS.size());
        assertThat((BigDecimal) before.get("C_NUMBER_S")).isEqualByComparingTo("-42.5");
        assertThat((BigDecimal) after.get("C_NUMBER_S")).isEqualByComparingTo("1");
        assertThat(before.get("c_varchar_")).isEqualTo("plain 'quoted' text");
        assertThat(after.get("c_varchar_")).isEqualTo("changed");
        Map<String, Object> first = types.get(0).row("after");
        for (String f : ADJUSTED_FIELDS) {
          if (!f.equals("C_NUMBER_S") && !f.equals("c_varchar_")) {
            assertThat(AvroValues.same(after.get(f), first.get(f)))
                .as("%s unchanged by the update: %s, inserted %s", f, after.get(f), first.get(f))
                .isTrue();
            assertThat(AvroValues.same(before.get(f), first.get(f))).as("before %s", f).isTrue();
          }
        }

        // the delete, then its tombstone under the same key
        assertThat(types.get(3).row("after")).isNull();
        Map<String, Object> gone = types.get(3).row("before");
        assertThat(gone).hasSize(ADJUSTED_FIELDS.size()).containsEntry("C_DATE", null);
        assertThat((BigDecimal) gone.get("ID_")).isEqualByComparingTo("2");
        Read tombstone = types.get(4);
        assertThat(tombstone.raw().value()).isNull();
        assertThat(tombstone.raw().key()).isEqualTo(types.get(3).raw().key());
        assertThat(tombstone.key()).isEqualTo(types.get(3).key());

        assertThat(types)
            .extracting(r -> ((BigDecimal) r.keyField("ID_")).intValueExact())
            .containsExactly(1, 2, 1, 2, 2);

        assertMatchesDatabase(types);
        assertPartialImages(pk);
      } finally {
        keys.close();
        values.close();
      }

      // auto-registration under the topic: one artifact per key and value schema
      for (String artifact :
          List.of(typesTopic + "-key", typesTopic + "-value", pkTopic + "-value")) {
        assertThat(registryStatus("/groups/default/artifacts/" + artifact))
            .as("artifact %s in the registry", artifact)
            .isEqualTo(200);
      }
      assertHeartbeatIsAvro(PREFIX + ".cdc.heartbeat");
    } finally {
      cluster.delete(name);
    }
  }

  /** The adjusted schema and field names, and the semantic type names Debezium consumers use. */
  private void assertAdjustedNames(Read insert) {
    String base = "avro_cdc.FREEPDB1." + schema + ".avro_types_";
    Schema envelope = insert.value().getSchema();
    assertThat(envelope.getFullName()).isEqualTo(base + ".Envelope");
    assertThat(envelope.getFields())
        .extracting(Schema.Field::name)
        .containsExactly(
            "before", "after", "source", "op", "ts_ms", "ts_us", "ts_ns", "transaction");
    Schema row = AvroValues.nonNull(envelope.getField("after").schema());
    assertThat(row.getFullName()).isEqualTo(base + ".Value");
    assertThat(AvroValues.nonNull(envelope.getField("before").schema())).isEqualTo(row);
    assertThat(row.getFields())
        .extracting(Schema.Field::name)
        .containsExactlyElementsOf(ADJUSTED_FIELDS);
    Schema key = insert.key().getSchema();
    assertThat(key.getFullName()).isEqualTo(base + ".Key");
    assertThat(key.getFields()).extracting(Schema.Field::name).containsExactly("ID_");
    assertThat(AvroValues.connectName(key.getField("ID_").schema()))
        .isEqualTo(AvroValues.VARIABLE_SCALE_DECIMAL);
    Map<String, String> semantic = new LinkedHashMap<>();
    semantic.put("ID_", AvroValues.VARIABLE_SCALE_DECIMAL);
    semantic.put("C_NUMBER", AvroValues.VARIABLE_SCALE_DECIMAL);
    semantic.put("C_FLOAT", AvroValues.VARIABLE_SCALE_DECIMAL);
    semantic.put("C_NUMBER_S", AvroValues.DECIMAL);
    semantic.put("C_DATE", AvroValues.TIMESTAMP);
    semantic.put("C_TS", AvroValues.MICRO_TIMESTAMP);
    semantic.put("C_TS9", AvroValues.NANO_TIMESTAMP);
    semantic.put("C_TSTZ", AvroValues.ZONED_TIMESTAMP);
    semantic.put("C_TSLTZ", AvroValues.ZONED_TIMESTAMP);
    semantic.put("C_IYM", AvroValues.MICRO_DURATION);
    semantic.put("C_IDS", AvroValues.MICRO_DURATION);
    semantic.forEach(
        (f, n) -> assertThat(AvroValues.connectName(row.getField(f).schema())).as(f).isEqualTo(n));
    // the fixed names are valid Avro already and never change
    assertThat(AvroValues.nonNull(envelope.getField("source").schema()).getFullName())
        .isEqualTo("io.debezium.connector.oracle.Source");
    assertThat(AvroValues.nonNull(envelope.getField("transaction").schema()).getFullName())
        .isEqualTo("event.block");
  }

  /** The Debezium envelope fields of a change record of the types table. */
  private void assertEnvelope(Read r, String op, int totalOrder) {
    GenericRecord v = r.value();
    assertThat(v.get("op").toString()).isEqualTo(op);
    assertThat((Long) v.get("ts_ms")).as("ts_ms, the commit time").isPositive();
    GenericRecord source = (GenericRecord) v.get("source");
    assertThat(source.get("connector").toString()).isEqualTo("oracle-cdc");
    assertThat(source.get("name").toString()).as("the configured prefix").isEqualTo(PREFIX);
    assertThat(source.get("db").toString()).isEqualTo("FREEPDB1");
    assertThat(source.get("schema").toString()).isEqualTo(schema);
    assertThat(source.get("table").toString()).as("the Oracle name").isEqualTo("avro#types$");
    assertThat(source.get("snapshot").toString()).isEqualTo("false");
    assertThat(source.get("txId").toString()).matches("\\d+\\.\\d+\\.\\d+");
    assertThat(Long.parseLong(source.get("scn").toString())).isPositive();
    GenericRecord tx = (GenericRecord) v.get("transaction");
    assertThat((Long) tx.get("total_order")).isEqualTo(totalOrder);
  }

  /** TypeRoundTripEngineIT's expectations for its first insert, under the adjusted names. */
  private static void assertFirstInsert(Map<String, Object> after) {
    assertThat(after).hasSize(ADJUSTED_FIELDS.size());
    assertThat((BigDecimal) after.get("ID_")).isEqualByComparingTo("1");
    assertThat(after.get("c_varchar_")).isEqualTo("plain 'quoted' text");
    assertThat(after.get("C_CHAR")).isEqualTo("abc       ");
    assertThat(after.get("C_NVARCHAR")).isEqualTo("ünïcode ☃ text");
    assertThat(after.get("c_nchar")).isEqualTo("nchár     ");
    assertThat((BigDecimal) after.get("C_NUMBER"))
        .isEqualByComparingTo(new BigDecimal("1234567890.123456789"));
    assertThat((BigDecimal) after.get("C_NUMBER_S")).isEqualByComparingTo("-42.5");
    assertThat((BigDecimal) after.get("C_FLOAT")).isEqualByComparingTo("3.14159");
    assertThat(after.get("C_BF")).isEqualTo(1.5E10f);
    assertThat(after.get("C_BD")).isEqualTo(2.5d);
    assertThat(after.get("C_DATE")).isEqualTo(LocalDateTime.of(2026, 2, 28, 13, 45, 59));
    assertThat(after.get("C_TS")).isEqualTo(LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_000));
    assertThat(after.get("C_TS9")).isEqualTo(LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_789));
    assertThat(after.get("C_TSTZ"))
        .isEqualTo(
            OffsetDateTime.of(
                2026, 3, 29, 1, 30, 0, 500_000_000, ZoneOffset.ofHoursMinutes(5, 30)));
    assertThat(((OffsetDateTime) after.get("C_TSLTZ")).toInstant())
        .isEqualTo(Instant.parse("2026-10-25T02:30:00.250Z"));
    assertThat((Double) after.get("C_IYM")).isCloseTo(147 * MICROS_PER_MONTH, within(1.0));
    assertThat((Double) after.get("C_IDS")).isCloseTo(446_582_123_456d, within(1.0));
    assertThat((byte[]) after.get("C_RAW")).containsExactly(0xde, 0xad, 0xbe, 0xef, 0x00, 0xff);
    assertThat(after.get("_2ND_VALUE")).isEqualTo("second");
  }

  /**
   * The latest image per key is what the database holds: the same keys, and each column of the
   * remaining row as JDBC reads it (TypeRoundTripEngineIT's JDBC view; the local time zone and
   * interval columns are checked against their literals above, as there).
   */
  private void assertMatchesDatabase(List<Read> types) throws Exception {
    Map<BigDecimal, Map<String, Object>> latest = new LinkedHashMap<>();
    for (Read r : types) {
      BigDecimal id = ((BigDecimal) r.keyField("ID_")).stripTrailingZeros();
      if (r.value() == null || "d".equals(r.op())) {
        latest.remove(id);
      } else {
        latest.put(id, r.row("after"));
      }
    }
    Map<String, Object> jdbc = new LinkedHashMap<>();
    int rows = 0;
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT \"ID#\", \"c_varchar$\", \"C CHAR\", \"C/NVARCHAR\", \"c-nchar\","
                    + " C_NUMBER, \"C_NUMBER#S\", C_FLOAT, C_BF, C_BD, C_DATE, C_TS, C_TS9,"
                    + " C_TSTZ, C_RAW, \"2ND_VALUE\" FROM \"avro#types$\"")) {
      while (rs.next()) {
        rows++;
        jdbc.put("ID_", rs.getBigDecimal(1));
        jdbc.put("c_varchar_", rs.getString(2));
        jdbc.put("C_CHAR", rs.getString(3));
        jdbc.put("C_NVARCHAR", rs.getString(4));
        jdbc.put("c_nchar", rs.getString(5));
        jdbc.put("C_NUMBER", rs.getBigDecimal(6));
        jdbc.put("C_NUMBER_S", rs.getBigDecimal(7));
        jdbc.put("C_FLOAT", rs.getBigDecimal(8));
        jdbc.put("C_BF", rs.getFloat(9));
        jdbc.put("C_BD", rs.getDouble(10));
        jdbc.put("C_DATE", rs.getObject(11, LocalDateTime.class));
        jdbc.put("C_TS", rs.getObject(12, LocalDateTime.class));
        jdbc.put("C_TS9", rs.getObject(13, LocalDateTime.class));
        jdbc.put("C_TSTZ", rs.getObject(14, OffsetDateTime.class));
        jdbc.put("C_RAW", rs.getBytes(15));
        jdbc.put("_2ND_VALUE", rs.getString(16));
      }
    }
    assertThat(rows).as("rows left in the table").isEqualTo(1);
    assertThat(latest.keySet()).containsExactly(new BigDecimal("1"));
    Map<String, Object> kafka = latest.get(new BigDecimal("1"));
    jdbc.forEach(
        (f, v) ->
            assertThat(AvroValues.same(kafka.get(f), v))
                .as("%s: Kafka %s, database %s", f, kafka.get(f), v)
                .isTrue());
  }

  /** Primary-key-only logging: the before image of the update holds the key and the change. */
  private void assertPartialImages(List<Read> pk) {
    String base = "avro_cdc.FREEPDB1." + schema + ".PKONLY";
    assertThat(pk.get(0).value().getSchema().getFullName()).isEqualTo(base + ".Envelope");
    assertThat(pk.get(0).key().getSchema().getFullName()).isEqualTo(base + ".Key");
    Map<String, Object> inserted = pk.get(0).row("after");
    assertThat((BigDecimal) inserted.get("V")).isEqualByComparingTo("10");
    assertThat((BigDecimal) inserted.get("W")).isEqualByComparingTo("20");
    Map<String, Object> before = pk.get(1).row("before");
    Map<String, Object> after = pk.get(1).row("after");
    assertThat((BigDecimal) before.get("ID")).isEqualByComparingTo("1");
    assertThat((BigDecimal) before.get("V")).isEqualByComparingTo("10");
    assertThat(before.get("W")).as("not logged").isNull();
    assertThat((BigDecimal) after.get("V")).isEqualByComparingTo("11");
    assertThat(after.get("W")).as("not logged").isNull();
    assertThat((BigDecimal) pk.get(1).keyField("ID")).isEqualByComparingTo("1");
  }

  /** The connector's own heartbeat records go through the same converter. */
  private void assertHeartbeatIsAvro(String topic) {
    AvroKafkaDeserializer<Object> values = deserializer(false);
    try (KafkaConsumer<byte[], byte[]> c = cluster.byteConsumer("avro-heartbeat-reader", topic)) {
      List<ConsumerRecord<byte[], byte[]>> beats =
          ConnectCluster.consume(c, 1, Duration.ofMinutes(1), Duration.ofSeconds(1));
      assertThat(beats).as("heartbeats on %s", topic).isNotEmpty();
      ConsumerRecord<byte[], byte[]> r = beats.get(0);
      GenericRecord beat = (GenericRecord) values.deserialize(r.topic(), r.headers(), r.value());
      assertThat(beat.getSchema().getFullName()).isEqualTo("io.oso.cdc.heartbeat.Value");
      assertThat((Long) beat.get("resume_scn")).isPositive();
    } finally {
      values.close();
    }
  }

  private int registryStatus(String path) throws Exception {
    HttpResponse<String> r =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(cluster.schemaRegistryUrl() + path))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    return r.statusCode();
  }

  @Test
  void withoutAdjustmentAvroRefusesTheNamesVisibly() throws Exception {
    String tableConnector = "avro-none-table";
    // a column name Avro refuses, with an Avro converter on the connector: the connector's own
    // validation refuses the configuration (DOC-22, blocking). Before DOC-22 the converter failed
    // the task at the table's first record with a SchemaParseException, before anything of the
    // table was written; with the converter set only on the worker that is still what happens
    assertThatThrownBy(
            () ->
                cluster.register(
                    "avro-none-field",
                    config("none_field", "FREEPDB1\\." + schema + "\\.ORDER#", "none")))
        .hasMessageContaining("HTTP 400")
        .hasMessageContaining("DOC-22")
        .hasMessageContaining("ORDER#.ID#")
        .hasMessageContaining("cdc.field.name.adjustment.mode=avro");
    cluster.register(
        tableConnector, config("none_table", "FREEPDB1\\." + schema + "\\.ITEM#", "none"));
    AvroKafkaDeserializer<Object> keys = deserializer(true);
    AvroKafkaDeserializer<Object> values = deserializer(false);
    try {
      cluster.awaitRunning(tableConnector, Duration.ofMinutes(2));
      cluster.awaitOffsets(tableConnector, Duration.ofSeconds(90));
      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        w.setAutoCommit(false);
        try (Statement s = w.createStatement()) {
          s.execute("INSERT INTO \"ITEM#\" VALUES (1, 'one')");
        }
        w.commit();
      }

      // a table name Avro refuses, with valid field names: Avro builds the schema without checking
      // its namespace, and Apicurio Registry stores it as written (no validity rule is configured,
      // as by default), so the record is written. A consumer whose Avro parser checks namespaces
      // (Avro for Java 1.12 and later, as here) refuses it. With a validity rule such as
      // apicurio.rules.global.validity=FULL the registry answers 400 to the registration instead,
      // and the task fails here.
      String tableTopic = "none_table.FREEPDB1." + schema + ".ITEM_";
      try (KafkaConsumer<byte[], byte[]> c =
          cluster.byteConsumer("none-table-reader", tableTopic)) {
        List<ConsumerRecord<byte[], byte[]>> got =
            ConnectCluster.consume(c, 1, Duration.ofMinutes(3), Duration.ofSeconds(1));
        assertThat(got)
            .as("a record of %s; task status %s", tableTopic, cluster.status(tableConnector))
            .isNotEmpty();
        ConsumerRecord<byte[], byte[]> r = got.get(0);
        assertThatThrownBy(() -> values.deserialize(r.topic(), r.headers(), r.value()))
            .as("the value of %s", tableTopic)
            .satisfies(e -> assertRefusedName(e, "ITEM#"));
        assertThatThrownBy(() -> keys.deserialize(r.topic(), r.headers(), r.key()))
            .as("the key of %s", tableTopic)
            .satisfies(e -> assertRefusedName(e, "ITEM#"));
      }
    } finally {
      keys.close();
      values.close();
      cluster.delete(tableConnector);
    }
  }

  /** Somewhere in the cause chain Avro's parser refuses a name holding {@code name}. */
  private static void assertRefusedName(Throwable e, String name) {
    List<Throwable> chain = new ArrayList<>();
    for (Throwable t = e; t != null && !chain.contains(t); t = t.getCause()) {
      chain.add(t);
    }
    assertThat(chain)
        .as("cause chain of %s", e.toString())
        .anySatisfy(
            t -> {
              assertThat(t).isInstanceOf(SchemaParseException.class);
              assertThat(t.getMessage()).contains(name);
            });
  }
}
