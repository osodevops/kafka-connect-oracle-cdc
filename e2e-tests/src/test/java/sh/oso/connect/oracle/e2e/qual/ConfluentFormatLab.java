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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/** Only public APIs and Kafka wire data are observed; no proprietary dependency is loaded here. */
final class ConfluentFormatLab {
  static final String OSO_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";

  static Path plugin() {
    assumeTrue(
        !"true".equalsIgnoreCase(System.getenv("CI"))
            && !"true".equalsIgnoreCase(System.getenv("GITHUB_ACTIONS")),
        "Confluent's connector must never run in CI");
    String local = System.getProperty("e2e.confluent.plugin", "");
    assumeTrue(
        !local.isBlank() && Files.isDirectory(Path.of(local)),
        "-De2e.confluent.plugin must name a hand-placed local copy");
    return Path.of(local);
  }

  static Map<String, String> oso(TestDatabase db, Map<String, String> source, String prefix) {
    Map<String, String> config = new LinkedHashMap<>(db.connectorDatabaseProps());
    config.put("connector.class", OSO_CLASS);
    config.put("tasks.max", "1");
    config.put("cdc.topic.prefix", prefix);
    config.put("cdc.output.format", "confluent");
    config.put("cdc.tables.include", source.get("table.inclusion.regex"));
    config.put("cdc.snapshot.mode", "initial");
    config.put(
        "cdc.topic.template",
        prefix
            + "."
            + (config.getOrDefault("cdc.database.pdbs", "").isBlank() ? "${database}" : "${pdb}")
            + ".${schema}.${table}");
    config.put("cdc.poll.linger.ms", "100");
    config.put("cdc.key.missing", "none");
    for (Map.Entry<String, String> entry : source.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("output."))
        config.put("cdc.output.confluent." + key.substring(7), entry.getValue());
      if (key.startsWith("numeric.") || key.equals("key.template"))
        config.put("cdc.output.confluent." + key, entry.getValue());
      if (key.equals("oracle.date.mapping"))
        config.put("cdc.output.confluent.date.mapping", entry.getValue());
      if (key.startsWith("key.converter") || key.startsWith("value.converter"))
        config.put(key, entry.getValue());
    }
    if (source.containsKey("emit.tombstone.on.delete"))
      config.put("cdc.tombstones.on.delete", source.get("emit.tombstone.on.delete"));
    if (source.containsKey("lob.topic.name.template")) {
      config.put("cdc.lob.mode", "topic");
      config.put("cdc.lob.topic.template", prefix + ".lob.${pdb}.${schema}.${table}.${column}");
    }
    return config;
  }

  static List<ConsumerRecord<byte[], byte[]>> read(
      ConnectCluster cluster, List<String> topics, int atLeast, Duration timeout) {
    try (KafkaConsumer<byte[], byte[]> consumer =
        cluster.byteConsumer("format-" + System.nanoTime(), topics.toArray(String[]::new))) {
      long deadline = System.currentTimeMillis() + timeout.toMillis();
      long last = System.currentTimeMillis();
      List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofSeconds(1))) {
          out.add(record);
          last = System.currentTimeMillis();
        }
        if (out.size() >= atLeast && System.currentTimeMillis() - last > 10_000) return out;
      }
      assertThat(out.size()).as("records on " + topics).isGreaterThanOrEqualTo(atLeast);
      return out;
    }
  }

  record Decoded(JsonNode schema, JsonNode value) {}

  static Decoded decode(byte[] wire, boolean avro, String registry) throws Exception {
    if (wire == null) return new Decoded(null, null);
    if (!avro) {
      JsonNode document = ConfluentFormatRefQualIT.JSON.readTree(wire);
      return new Decoded(document.get("schema"), document.get("payload"));
    }
    assertThat(wire[0]).isZero();
    int id = ByteBuffer.wrap(wire, 1, 4).getInt();
    String text =
        ConfluentFormatRefQualIT.get(registry + "/schemas/ids/" + id).path("schema").asText();
    Schema schema = new Schema.Parser().parse(text);
    Object datum =
        new GenericDatumReader<>(schema)
            .read(null, DecoderFactory.get().binaryDecoder(wire, 5, wire.length - 5, null));
    // GenericData's JSON form represents primitives, records and bytes deterministically.
    if (datum instanceof ByteBuffer buffer) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
      return new Decoded(
          ConfluentFormatRefQualIT.JSON.readTree(text),
          ConfluentFormatRefQualIT.JSON.valueToTree(
              java.util.Base64.getEncoder().encodeToString(bytes)));
    }
    JsonNode value;
    if (datum instanceof org.apache.avro.generic.GenericRecord)
      value = ConfluentFormatRefQualIT.JSON.readTree(datum.toString());
    else
      value =
          ConfluentFormatRefQualIT.JSON.valueToTree(
              datum instanceof CharSequence ? datum.toString() : datum);
    return new Decoded(ConfluentFormatRefQualIT.JSON.readTree(text), value);
  }

  static void compatibility(ConnectCluster cluster, String oldSubject, String newSubject)
      throws Exception {
    String registry = cluster.confluentSchemaRegistryUrl();
    JsonNode oldVersions =
        ConfluentFormatRefQualIT.get(registry + "/subjects/" + oldSubject + "/versions");
    JsonNode newVersions =
        ConfluentFormatRefQualIT.get(registry + "/subjects/" + newSubject + "/versions");
    assertThat(newVersions.size()).isEqualTo(oldVersions.size());
    for (int i = 0; i < oldVersions.size(); i++) {
      String old =
          ConfluentFormatRefQualIT.get(
                  registry + "/subjects/" + oldSubject + "/versions/" + oldVersions.get(i).asInt())
              .path("schema")
              .asText();
      String next =
          ConfluentFormatRefQualIT.get(
                  registry + "/subjects/" + newSubject + "/versions/" + newVersions.get(i).asInt())
              .path("schema")
              .asText();
      assertThat(ConfluentFormatRefQualIT.JSON.readTree(next))
          .isEqualTo(ConfluentFormatRefQualIT.JSON.readTree(old));
      ObjectNode body = ConfluentFormatRefQualIT.JSON.createObjectNode().put("schema", next);
      HttpResponse<String> result =
          ConfluentFormatRefQualIT.HTTP.send(
              HttpRequest.newBuilder(
                      URI.create(
                          registry
                              + "/compatibility/subjects/"
                              + oldSubject
                              + "/versions/"
                              + oldVersions.get(i).asInt()))
                  .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                  .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(result.statusCode()).isEqualTo(200);
      assertThat(
              ConfluentFormatRefQualIT.JSON
                  .readTree(result.body())
                  .path("is_compatible")
                  .asBoolean())
          .isTrue();
    }
  }
}
