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
package sh.oso.connect.oracle.envelope.confluent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RedoText;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.snapshot.SnapshotRow;
import sh.oso.connect.oracle.envelope.TopicRouter;

/**
 * CI specification from external lab output, with no Confluent software or converter dependency.
 */
class ConfluentFormatFixtureTest {
  static final ObjectMapper JSON = new ObjectMapper();
  static final Path FIXTURES =
      Path.of(System.getProperty("confluent.fixtures", "src/test/resources/confluent-reference"));
  static final List<String> PROFILES =
      List.of(
          "default",
          "renamed",
          "all-fields",
          "key-struct",
          "key-concat",
          "numeric-best-fit-or-decimal",
          "numeric-best-fit-or-double",
          "numeric-best-fit-or-string",
          "numeric-precision-only",
          "date-timestamp",
          "tombstones");
  static final Position OFFSET = Position.initial(1, new DatabaseIdentity(1, 1));
  static final TxKey TX = new TxKey(3, new Xid(7, 1, 42));
  static final Pattern VOLATILE = Pattern.compile("<(scn|ts|rowid):(\\d+)>");
  static final Pattern RAW_ROWID =
      Pattern.compile("(?<![A-Za-z0-9+/])AAAAAAA(\\d{11})(?![A-Za-z0-9+/])");

  @TestFactory
  Stream<DynamicTest> jsonFixtures() throws Exception {
    List<DynamicTest> tests = new ArrayList<>();
    for (String profile : PROFILES) {
      try (Stream<Path> files = Files.list(FIXTURES.resolve(profile))) {
        for (Path path : files.filter(f -> f.toString().endsWith(".jsonl")).sorted().toList()) {
          String table =
              path.getFileName().toString().replace("FREEPDB1.SCHEMA.", "").replace(".jsonl", "");
          tests.add(
              DynamicTest.dynamicTest(profile + "/" + table, () -> compare(profile, table, path)));
        }
      }
    }
    return tests.stream();
  }

  static Map<String, String> properties(String profile) {
    Map<String, String> p = new LinkedHashMap<>();
    if (profile.startsWith("numeric-") && !profile.equals("numeric-none-scale2"))
      p.put(
          ConfluentFormatConfig.NUMERIC_MAPPING,
          profile.substring("numeric-".length()).replace('-', '_'));
    switch (profile) {
      case "numeric-none-scale2" -> p.put(ConfluentFormatConfig.NUMERIC_DEFAULT_SCALE, "2");
      case "date-timestamp" -> p.put(ConfluentFormatConfig.DATE_MAPPING, "timestamp");
      case "key-struct" -> p.put(ConfluentFormatConfig.KEY_TEMPLATE, ConfluentSettings.KEY_STRUCT);
      case "key-concat" ->
          p.put(ConfluentFormatConfig.KEY_TEMPLATE, ConfluentSettings.KEY_CONCATENATED);
      case "tombstones" -> p.put(OracleCdcSourceConnectorConfig.TOMBSTONES_ON_DELETE, "true");
      case "before-state-avro" -> p.put(ConfluentFormatConfig.BEFORE_STATE_FIELD, "before");
      case "all-fields" -> {
        p.put(ConfluentFormatConfig.COMMIT_SCN_FIELD, "cscn");
        p.put(ConfluentFormatConfig.REDO_FIELD, "redo");
        p.put(ConfluentFormatConfig.UNDO_FIELD, "undo");
        p.put(ConfluentFormatConfig.BEFORE_STATE_FIELD, "before");
      }
      case "renamed" -> {
        p.put(ConfluentFormatConfig.TABLE_NAME_FIELD, "t_name");
        p.put(ConfluentFormatConfig.SCN_FIELD, "header:x_scn");
        p.put(ConfluentFormatConfig.COMMIT_SCN_FIELD, "x_cscn");
        p.put(ConfluentFormatConfig.OP_TYPE_FIELD, "x_op");
        p.put(ConfluentFormatConfig.OP_TS_FIELD, "x_meta.op_ts");
        p.put(ConfluentFormatConfig.CURRENT_TS_FIELD, "x_meta.current_ts");
        p.put(ConfluentFormatConfig.ROW_ID_FIELD, "header:x_rowid");
        p.put(ConfluentFormatConfig.USERNAME_FIELD, "x_user");
        p.put(ConfluentFormatConfig.REDO_FIELD, "x_redo");
        p.put(ConfluentFormatConfig.UNDO_FIELD, "x_undo");
        p.put(ConfluentFormatConfig.BEFORE_STATE_FIELD, "x_before");
        p.put(ConfluentFormatConfig.OP_READ, "SNAP");
        p.put(ConfluentFormatConfig.OP_INSERT, "INS");
        p.put(ConfluentFormatConfig.OP_UPDATE, "UPD");
        p.put(ConfluentFormatConfig.OP_DELETE, "DEL");
        p.put(ConfluentFormatConfig.OP_TRUNCATE, "TRN");
      }
      default -> {}
    }
    return p;
  }

  static ConfluentFormat format(String profile) {
    OracleCdcSourceConnectorConfig cfg = ConfluentFormatConfigTest.config(properties(profile));
    return new ConfluentFormat(
        cfg,
        new TopicRouter(cfg.topicTemplate(true), "cdc", "FREE"),
        "FREE",
        cfg.confluentSettings(),
        Clock.fixed(Instant.ofEpochMilli(3000), ZoneOffset.UTC));
  }

  static List<JsonNode> read(Path path) throws Exception {
    List<JsonNode> out = new ArrayList<>();
    for (String line : Files.readAllLines(path)) out.add(JSON.readTree(line));
    return out;
  }

  static void compare(String profile, String table, Path path) throws Exception {
    List<JsonNode> expected = read(path);
    List<SourceRecord> actual = records(profile, table, expected);
    assertThat(actual).as(profile + "/" + table + " sequence").hasSize(expected.size());
    for (int i = 0; i < expected.size(); i++) {
      assertThat(serialise(actual.get(i)))
          .as(profile + "/" + table + " record " + i)
          .isEqualTo(normalise(expected.get(i).deepCopy()));
    }
  }

  static List<SourceRecord> records(String profile, String table, List<JsonNode> expected) {
    ConfluentFixtureData.Data data = ConfluentFixtureData.data(table);
    ConfluentFormat format = format(profile);
    List<SourceRecord> out = new ArrayList<>();
    for (Map<String, Object> row : data.snapshots()) {
      long scn =
          number(
              meta(expected.get(out.size()), profile.equals("renamed") ? "x_scn" : "scn"),
              "scn",
              1);
      out.addAll(
          format.snapshotRecords(
              data.schema().table(),
              new SnapshotRow(row, null),
              data.schema(),
              scn,
              3000,
              "true",
              OFFSET));
    }
    int eventIndex = 0;
    for (ConfluentFixtureData.Input input : data.changes()) {
      JsonNode fixture = expected.get(out.size());
      if (profile.equals("tombstones")
          && input.op() == sh.oso.connect.oracle.core.model.Operation.UPDATE
          && fixture.path("value").isNull()) {
        // A key change starts with two tombstones. Its final insert carries the metadata
        // of the same UPDATE, so use those placeholder relations for the input event.
        fixture = expected.get(out.size() + 2);
      }
      long scn =
          number(
              meta(fixture, profile.equals("renamed") ? "x_scn" : "scn"), "scn", 50 + eventIndex);
      long commit =
          number(
              meta(fixture, profile.equals("renamed") ? "x_cscn" : "cscn"),
              "scn",
              100 + eventIndex);
      long time = number(meta(fixture, "op_ts"), "ts", 3);
      String rowId =
          raw(meta(fixture, profile.equals("renamed") ? "x_rowid" : "row_id").asText("<rowid:1>"));
      TableSchema schema =
          input.ddl() ? ConfluentFixtureData.afterDdl(data.schema()) : data.schema();
      RedoText sql =
          new RedoText(
              raw(meta(fixture, profile.equals("renamed") ? "x_redo" : "redo").asText(null)),
              raw(meta(fixture, profile.equals("renamed") ? "x_undo" : "undo").asText(null)));
      RowChange change =
          new RowChange(
              schema.table(),
              input.op(),
              input.before(),
              input.after(),
              false,
              rowId,
              new RedoRecordId(scn, "r" + eventIndex, 0),
              TX,
              Instant.ofEpochMilli(time),
              schema.version(),
              sql);
      CommittedTransaction tx =
          new CommittedTransaction(
              TX,
              change.id(),
              new RedoRecordId(scn - 1, "start", 0),
              new RedoRecordId(commit, "commit", 0),
              Instant.ofEpochMilli(time + 1000),
              1,
              "SCHEMA",
              null,
              List.of(change));
      out.addAll(format.records(tx, 0, ++eventIndex, schema, OFFSET));
    }
    return out;
  }

  static JsonNode meta(JsonNode record, String field) {
    JsonNode value = find(record.path("value").path("payload"), field);
    if (!value.isMissingNode()) return value;
    for (JsonNode header : record.path("headers"))
      if (header.path("key").asText().equals(field)) return header.path("value");
    return value;
  }

  static JsonNode find(JsonNode node, String field) {
    JsonNode direct = node.path(field);
    if (!direct.isMissingNode()) return direct;
    if (node.isObject())
      for (JsonNode child : node) {
        JsonNode nested = find(child, field);
        if (!nested.isMissingNode()) return nested;
      }
    return com.fasterxml.jackson.databind.node.MissingNode.getInstance();
  }

  static long number(JsonNode node, String kind, int fallback) {
    Matcher matcher = VOLATILE.matcher(node.asText());
    return (kind.equals("scn") ? 10000 : 10000000000L)
        + (matcher.matches() ? Long.parseLong(matcher.group(2)) : fallback);
  }

  static String raw(String value) {
    if (value == null) return null;
    Matcher matcher = VOLATILE.matcher(value);
    StringBuilder out = new StringBuilder();
    while (matcher.find()) {
      String replacement =
          matcher.group(1).equals("rowid")
              ? "AAAAAAA"
                  + String.format(java.util.Locale.ROOT, "%011d", Long.parseLong(matcher.group(2)))
              : Long.toString(
                  (matcher.group(1).equals("scn") ? 10000 : 10000000000L)
                      + Long.parseLong(matcher.group(2)));
      matcher.appendReplacement(out, Matcher.quoteReplacement(replacement));
    }
    return matcher.appendTail(out).toString();
  }

  static JsonNode serialise(SourceRecord record) throws Exception {
    ObjectNode out = JSON.createObjectNode();
    try (JsonConverter key = new JsonConverter();
        JsonConverter value = new JsonConverter()) {
      key.configure(Map.of("schemas.enable", true), true);
      value.configure(Map.of("schemas.enable", true), false);
      byte[] k = key.fromConnectData(record.topic(), record.keySchema(), record.key());
      byte[] v = value.fromConnectData(record.topic(), record.valueSchema(), record.value());
      out.set("key", k == null ? null : JSON.readTree(k));
      out.set("value", v == null ? null : JSON.readTree(v));
    }
    ArrayNode headers = out.putArray("headers");
    for (Header h : record.headers())
      if (!h.key().startsWith("cdc.")) {
        ObjectNode header = headers.addObject();
        header.put("key", h.key());
        header.put("value", (String) h.value());
      }
    return normalise(out);
  }

  static JsonNode normalise(JsonNode node) {
    if (node.isObject()) {
      ObjectNode out = JSON.createObjectNode();
      node.fields()
          .forEachRemaining(
              e -> {
                if (e.getKey().equals("timestamp") || e.getKey().equals("timestampType")) return;
                JsonNode value = e.getValue();
                String field = e.getKey();
                if (Set.of("current_ts").contains(field) && !value.isNull())
                  value = TextNode.valueOf("<current_ts>");
                else if (Set.of("scn", "cscn", "x_cscn", "x_scn").contains(field)
                    && value.isTextual()
                    && !value.asText().startsWith("<"))
                  value =
                      TextNode.valueOf("<scn:" + (Long.parseLong(value.asText()) - 10000) + ">");
                else if (field.equals("op_ts")
                    && value.isTextual()
                    && !value.asText().startsWith("<"))
                  value =
                      TextNode.valueOf(
                          "<ts:" + (Long.parseLong(value.asText()) - 10000000000L) + ">");
                // Header metadata is represented as {key, value}, rather than a body field.
                if (field.equals("value")
                    && node.has("key")
                    && node.path("key").isTextual()
                    && node.path("key").asText().equals("x_scn")
                    && !value.asText().startsWith("<"))
                  value =
                      TextNode.valueOf("<scn:" + (Long.parseLong(value.asText()) - 10000) + ">");
                out.set(field, normalise(value));
              });
      return out;
    }
    if (node.isArray()) {
      ArrayNode out = JSON.createArrayNode();
      node.forEach(n -> out.add(normalise(n)));
      return out;
    }
    if (node.isTextual()) {
      Matcher matcher = RAW_ROWID.matcher(node.asText());
      StringBuilder out = new StringBuilder();
      while (matcher.find())
        matcher.appendReplacement(
            out, Matcher.quoteReplacement("<rowid:" + Long.parseLong(matcher.group(1)) + ">"));
      return TextNode.valueOf(matcher.appendTail(out).toString());
    }
    return node;
  }

  @Test
  void numberRequiringRoundingStopsSnapshotAndStreamingInsteadOfChangingItsValue() {
    ConfluentFormat format = format("numeric-none-scale2");
    ConfluentFixtureData.Data data = ConfluentFixtureData.data("NUMS");
    for (Map<String, Object> row : data.snapshots()) {
      assertThatThrownBy(
              () ->
                  format.snapshotRecords(
                      data.schema().table(),
                      new SnapshotRow(row, null),
                      data.schema(),
                      100,
                      3000,
                      "true",
                      OFFSET))
          .isInstanceOf(DecodeException.class)
          .hasMessageContaining("scale");
    }
    RowChange change =
        new RowChange(
            data.schema().table(),
            sh.oso.connect.oracle.core.model.Operation.INSERT,
            null,
            ConfluentFixtureData.nums(11, "0.125"),
            false,
            "R1",
            new RedoRecordId(100, "r", 0),
            TX,
            Instant.EPOCH);
    CommittedTransaction tx =
        new CommittedTransaction(
            TX,
            change.id(),
            null,
            new RedoRecordId(200, "c", 0),
            Instant.EPOCH,
            1,
            "SCHEMA",
            null,
            List.of(change));
    assertThatThrownBy(() -> format.records(tx, 0, 1, data.schema(), OFFSET))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("scale");
    assertThat(
            format.snapshotRecords(
                data.schema().table(),
                new SnapshotRow(ConfluentFixtureData.nums(10, "0.12"), null),
                data.schema(),
                100,
                3000,
                "true",
                OFFSET))
        .hasSize(1);
  }

  @Test
  void scaleTwoRowsThatFitStillMatchTheRecordedSnapshotAndStreamingOutput() throws Exception {
    String profile = "numeric-none-scale2";
    ConfluentFormat format = format(profile);
    for (String table : List.of("COMPOSITE", "EXOTIC", "LOBS", "LOBS2", "NOPK")) {
      ConfluentFixtureData.Data data = ConfluentFixtureData.data(table);
      List<JsonNode> reference =
          read(FIXTURES.resolve(profile).resolve("FREEPDB1.SCHEMA." + table + ".jsonl"));
      assertThat(reference).hasSize(data.snapshots().size());
      for (int i = 0; i < reference.size(); i++) {
        long scn = number(meta(reference.get(i), "scn"), "scn", 1);
        SourceRecord actual =
            format
                .snapshotRecords(
                    data.schema().table(),
                    new SnapshotRow(data.snapshots().get(i), null),
                    data.schema(),
                    scn,
                    3000,
                    "true",
                    OFFSET)
                .get(0);
        assertThat(serialise(actual)).isEqualTo(normalise(reference.get(i)));
      }
    }
    ConfluentFixtureData.Data data = ConfluentFixtureData.data("NUMS");
    List<JsonNode> reference =
        read(FIXTURES.resolve(profile).resolve("FREEPDB1.SCHEMA.NUMS.jsonl"));
    assertThat(reference).hasSize(4);
    JsonNode exact = reference.get(3);
    long scn = number(meta(exact, "scn"), "scn", 1);
    long time = number(meta(exact, "op_ts"), "ts", 3);
    RowChange change =
        new RowChange(
            data.schema().table(),
            sh.oso.connect.oracle.core.model.Operation.INSERT,
            null,
            ConfluentFixtureData.nums(10, "0.12"),
            false,
            raw(meta(exact, "row_id").asText()),
            new RedoRecordId(scn, "r", 0),
            TX,
            Instant.ofEpochMilli(time));
    CommittedTransaction tx =
        new CommittedTransaction(
            TX,
            change.id(),
            new RedoRecordId(scn - 1, "start", 0),
            new RedoRecordId(scn + 1, "commit", 0),
            Instant.ofEpochMilli(time),
            1,
            "SCHEMA",
            null,
            List.of(change));
    assertThat(serialise(format.records(tx, 0, 1, data.schema(), OFFSET).get(0)))
        .isEqualTo(normalise(exact));
  }

  @TestFactory
  Stream<DynamicTest> avroSchemaFixtures() throws Exception {
    List<DynamicTest> tests = new ArrayList<>();
    for (String profile : List.of("default-avro", "before-state-avro")) {
      try (Stream<Path> files = Files.list(FIXTURES.resolve(profile).resolve("subjects"))) {
        List<Path> paths = files.sorted().toList();
        for (String table : ConfluentFixtureData.TABLES) {
          List<Path> subjects =
              paths.stream()
                  .filter(
                      p -> p.getFileName().toString().startsWith("FREEPDB1.SCHEMA." + table + "-"))
                  .toList();
          if (subjects.isEmpty()) continue;
          tests.add(
              DynamicTest.dynamicTest(
                  profile + "/" + table + " schemas",
                  () -> {
                    ConfluentFixtureData.Data data = ConfluentFixtureData.data(table);
                    ConfluentFormat format = format(profile);
                    Set<JsonNode> generated = new LinkedHashSet<>();
                    for (TableSchema schema :
                        table.equals("TYPES")
                            ? List.of(data.schema(), ConfluentFixtureData.afterDdl(data.schema()))
                            : List.of(data.schema())) {
                      ConfluentFormat.Schemas shapes = format.schemas(schema);
                      if (shapes.key() != null)
                        generated.add(avro(shapes.key(), new AtomicInteger()));
                      generated.add(avro(shapes.value(), new AtomicInteger()));
                      if (shapes.valueWithBefore() != null)
                        generated.add(avro(shapes.valueWithBefore(), new AtomicInteger()));
                    }
                    Set<JsonNode> reference = new LinkedHashSet<>();
                    for (Path path : subjects) reference.add(JSON.readTree(Files.readString(path)));
                    assertThat(generated).containsAll(reference);
                    // The post-DDL schema with before state is valid but the script does not emit
                    // an update
                    // after DDL, so only the schema versions actually observed are required here.
                  }));
        }
      }
    }
    return tests.stream();
  }

  /** The documented Connect-to-Avro shape; real converter compatibility is checked in the lab. */
  static JsonNode avro(Schema schema, AtomicInteger names) {
    JsonNode type;
    if (schema.type() == Schema.Type.STRUCT) {
      int ordinal = names.incrementAndGet();
      ObjectNode record = JSON.createObjectNode();
      record.put("type", "record");
      record.put("name", "ConnectDefault" + (ordinal == 1 ? "" : ordinal));
      if (ordinal == 1) record.put("namespace", "io.confluent.connect.avro");
      ArrayNode fields = record.putArray("fields");
      for (org.apache.kafka.connect.data.Field field : schema.fields()) {
        ObjectNode f = fields.addObject();
        f.put("name", field.name());
        f.set("type", avro(field.schema(), names));
        if (field.schema().isOptional()) f.putNull("default");
      }
      type = record;
    } else {
      String name =
          switch (schema.type()) {
            case INT8, INT16, INT32 -> "int";
            case INT64 -> "long";
            case FLOAT32 -> "float";
            case FLOAT64 -> "double";
            case STRING -> "string";
            case BYTES -> "bytes";
            case BOOLEAN -> "boolean";
            default -> throw new IllegalArgumentException(schema.type().toString());
          };
      if (schema.name() == null) type = TextNode.valueOf(name);
      else {
        ObjectNode logical = JSON.createObjectNode();
        logical.put("type", name);
        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
          logical.put("scale", Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD)));
          logical.put(
              "precision", Integer.parseInt(schema.parameters().get(ConfluentTypes.PRECISION)));
        }
        if (schema.version() != null) logical.put("connect.version", schema.version());
        if (schema.parameters() != null)
          logical.set("connect.parameters", JSON.valueToTree(schema.parameters()));
        logical.put("connect.name", schema.name());
        logical.put(
            "logicalType",
            Decimal.LOGICAL_NAME.equals(schema.name())
                ? "decimal"
                : org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())
                    ? "date"
                    : "timestamp-millis");
        type = logical;
      }
    }
    return schema.isOptional() ? JSON.createArrayNode().add("null").add(type) : type;
  }
}
