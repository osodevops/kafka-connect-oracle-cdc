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
package sh.oso.connect.oracle.e2e.qual;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/** Live external-output comparison, strictly local and opt-in (ADR-0028). */
@Tag("qual")
class ConfluentFormatParityQualIT {
  private final TestDatabase db = TestDatabase.get();

  @Test
  void bothConnectorsMatchRecordsAndRegisteredAvroSchemas() throws Exception {
    var plugin = ConfluentFormatLab.plugin();
    Evidence evidence = Evidence.of(getClass(), "Confluent format parity");
    Set<String> selected =
        Set.of(System.getProperty("e2e.confluent.parity.profiles", "").split(","));
    try (ConnectCluster cluster =
        ConnectCluster.confluentPlatform(ConfluentFormatRefQualIT.WORKER_IMAGE)
            .withPlugin(plugin, "confluent-oracle-cdc")
            .withConfluentSchemaRegistry()
            .start()) {
      int index = 0;
      for (ConfluentFormatRefQualIT.Profile profile : ConfluentFormatRefQualIT.profiles()) {
        // Rounding refusal is already proven against the reference and in fixture regressions.
        if (profile.name().equals("numeric-none-scale2")) continue;
        if (!selected.contains("") && !selected.contains(profile.name())) continue;
        String schema = "CFPAR_" + index++;
        String old = "parity.ref-" + profile.name();
        String next = "parity.oso-" + profile.name();
        db.recreateSchema(schema);
        try {
          try (Connection work = db.workload(schema)) {
            ConfluentFormatRefQualIT.createAndSeed(work);
          }
          db.archiveLogCurrent();
          Thread.sleep(3500);
          Map<String, String> source =
              new ConfluentFormatRefQualIT().config(cluster, profile, schema, old);
          cluster.register(old, source);
          cluster.register(next, ConfluentFormatLab.oso(db, source, next));
          cluster.awaitRunning(old, Duration.ofMinutes(3));
          cluster.awaitRunning(next, Duration.ofMinutes(3));
          ConfluentFormatRefQualIT.awaitTopic(cluster, old + ".", "TYPES", Duration.ofMinutes(3));
          ConfluentFormatRefQualIT.awaitTopic(cluster, next + ".", "TYPES", Duration.ofMinutes(3));
          // Read each snapshot before changing its rows, so both AS OF reads describe the same
          // data.
          ConfluentFormatLab.read(
              cluster,
              cluster.topics().stream()
                  .filter(t -> t.startsWith(old + ".") || t.startsWith(next + "."))
                  .filter(t -> !t.contains("redo-log") && !t.contains(".cdc."))
                  .toList(),
              24,
              Duration.ofMinutes(3));
          try (Connection work = db.workload(schema);
              Connection other = db.workload(schema)) {
            ConfluentFormatRefQualIT.script(work, other);
            Thread.sleep(40_000);
            ConfluentFormatRefQualIT.scriptAfterDdl(work);
            if (profile.name().equals("lob-topics"))
              ConfluentFormatRefQualIT.exec(work, "UPDATE lobs SET n = NULL WHERE id = 0");
          }
          db.archiveLogCurrent();
          Map<String, List<ConsumerRecord<byte[], byte[]>>> records =
              ConfluentFormatRefQualIT.readUntilIdle(cluster, "parity.", Duration.ofMinutes(5));
          records.keySet().removeIf(t -> t.contains(".cdc."));
          cluster.awaitRunning(old, Duration.ofSeconds(10));
          cluster.awaitRunning(next, Duration.ofSeconds(10));
          for (String topic : records.keySet()) {
            if (!topic.startsWith(old + ".")) continue;
            String counterpart = next + topic.substring(old.length());
            List<JsonNode> expected = comparable(records.get(topic), profile, cluster, schema);
            List<JsonNode> actual =
                comparable(records.getOrDefault(counterpart, List.of()), profile, cluster, schema);
            if (profile.name().equals("lob-topics") && topic.endsWith(".LOBS2.C")) {
              assertLobMoveDifference(expected, actual);
            } else {
              assertLobInsertRowIdDifference(expected, actual, topic, profile);
              assertThat(actual)
                  .as(profile.name() + " " + topic)
                  .containsExactlyElementsOf(expected);
            }
            if (profile.avro())
              for (String side : List.of("key", "value")) {
                if (cluster.topics().contains(counterpart)) {
                  JsonNode subjects =
                      ConfluentFormatRefQualIT.get(
                          cluster.confluentSchemaRegistryUrl() + "/subjects");
                  if (subjects.toString().contains(topic + "-" + side))
                    ConfluentFormatLab.compatibility(
                        cluster, topic + "-" + side, counterpart + "-" + side);
                }
              }
          }
          assertThat(
                  records.keySet().stream()
                      .filter(t -> t.startsWith(next + "."))
                      .map(t -> old + t.substring(next.length()))
                      .toList())
              .containsExactlyInAnyOrderElementsOf(
                  records.keySet().stream().filter(t -> t.startsWith(old + ".")).toList());
          evidence.param("profile." + profile.name(), "records and schemas compared");
          cluster.delete(old);
          cluster.delete(next);
        } finally {
          db.dropSchema(schema);
        }
      }
    } catch (Throwable failure) {
      evidence.failed(failure);
      throw failure;
    } finally {
      evidence.write();
    }
  }

  private static List<JsonNode> comparable(
      List<ConsumerRecord<byte[], byte[]>> records,
      ConfluentFormatRefQualIT.Profile profile,
      ConnectCluster cluster,
      String schema)
      throws Exception {
    List<JsonNode> out = new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> record : records) {
      var key =
          ConfluentFormatLab.decode(
              record.key(), profile.avro(), cluster.confluentSchemaRegistryUrl());
      var value =
          ConfluentFormatLab.decode(
              record.value(), profile.avro(), cluster.confluentSchemaRegistryUrl());
      ObjectNode node = ConfluentFormatRefQualIT.JSON.createObjectNode();
      node.set("keySchema", key.schema());
      node.set("key", key.value());
      node.set("valueSchema", value.schema());
      node.set("value", value.value());
      ArrayNode headers = node.putArray("headers");
      for (var header : record.headers())
        if (!header.key().startsWith("cdc.")) {
          headers
              .addObject()
              .put("key", header.key())
              .put("value", new String(header.value(), java.nio.charset.StandardCharsets.UTF_8));
        }
      String current = profile.props().getOrDefault("output.current.ts.field", "current_ts");
      erase(node.path("value"), current);
      boolean snapshot =
          value.value() != null
              && value
                  .value()
                  .path(profile.props().getOrDefault("output.op.type.field", "op_type"))
                  .asText()
                  .equals(profile.props().getOrDefault("output.op.type.read.value", "R"));
      if (snapshot) {
        erase(node.path("value"), profile.props().getOrDefault("output.scn.field", "scn"));
        erase(node.path("value"), profile.props().getOrDefault("output.commit.scn.field", ""));
        for (int i = 0; i < headers.size(); i++) {
          String name = headers.get(i).path("key").asText();
          if (("header:" + name).equals(profile.props().get("output.scn.field")))
            ((ObjectNode) headers.get(i)).put("value", "<snapshot_scn>");
        }
      }
      out.add(ConfluentFormatRefQualIT.JSON.readTree(node.toString().replace(schema, "SCHEMA")));
    }
    return out;
  }

  private static void erase(JsonNode value, String path) {
    if (path.isBlank() || path.startsWith("header:")) return;
    String[] names = path.split("\\.");
    for (int i = 0; i < names.length - 1; i++) value = value.path(names[i]);
    if (value instanceof ObjectNode object) object.put(names[names.length - 1], "<volatile>");
  }

  private static void assertLobInsertRowIdDifference(
      List<JsonNode> expected,
      List<JsonNode> actual,
      String topic,
      ConfluentFormatRefQualIT.Profile profile) {
    if (!topic.endsWith(".LOBS") && !topic.endsWith(".LOBS2")) return;
    String op = profile.props().getOrDefault("output.op.type.field", "op_type");
    String insert = profile.props().getOrDefault("output.op.type.insert.value", "I");
    String rowId = profile.props().getOrDefault("output.row.id.field", "row_id");
    for (int i = 0; i < Math.min(expected.size(), actual.size()); i++) {
      JsonNode source = expected.get(i).path("value");
      JsonNode target = actual.get(i).path("value");
      if (!source.path(op).asText().equals(insert)) continue;
      // Confluent exposes LogMiner's all-A placeholder for an out-of-row LOB insert.
      // OSO's assembly resolves the real locator when available; keep the difference explicit.
      if (source.path(rowId).asText().equals("AAAAAAAAAAAAAAAAAA")) {
        assertThat(target.path(rowId).asText()).matches("[A-Za-z0-9+/]{18}");
        ((ObjectNode) target).put(rowId, "AAAAAAAAAAAAAAAAAA");
      }
      if (rowId.startsWith("header:")) {
        String name = rowId.substring(7);
        for (JsonNode h : expected.get(i).path("headers"))
          if (h.path("key").asText().equals(name)
              && h.path("value").asText().equals("AAAAAAAAAAAAAAAAAA"))
            for (JsonNode a : actual.get(i).path("headers"))
              if (a.path("key").asText().equals(name)) {
                assertThat(a.path("value").asText()).matches("[A-Za-z0-9+/]{18}");
                ((ObjectNode) a).put("value", "AAAAAAAAAAAAAAAAAA");
              }
      }
    }
  }

  private static void assertLobMoveDifference(List<JsonNode> expected, List<JsonNode> actual) {
    // Confluent leaves the old composite LOB key behind. OSO adds old-key removal and new value.
    assertThat(actual).hasSize(expected.size() + 2);
    int move = actual.size() - 3;
    assertThat(actual.get(move).path("value").isNull()).isTrue();
    assertThat(actual.get(move + 1).path("value").isNull()).isFalse();
    List<JsonNode> withoutMove = new ArrayList<>(actual);
    withoutMove.remove(move + 1);
    withoutMove.remove(move);
    assertThat(withoutMove).containsExactlyElementsOf(expected);
  }
}
