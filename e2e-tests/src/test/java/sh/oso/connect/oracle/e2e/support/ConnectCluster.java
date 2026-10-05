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
package sh.oso.connect.oracle.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

/**
 * A real Kafka broker and a real Connect worker (docker) on the Oracle container's network, with
 * the packaged plugin directory copied in. The worker runs exactly-once source support so the EOS
 * suites of Phase 1c reuse it. Not a singleton: restart suites kill and restart the worker.
 */
public final class ConnectCluster implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectCluster.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  public static final String KAFKA_IMAGE = System.getProperty("kafka.image", "apache/kafka:3.9.1");

  private final KafkaContainer kafka;
  private final GenericContainer<?> connect;
  private final HttpClient http = HttpClient.newHttpClient();

  public ConnectCluster() {
    kafka =
        new KafkaContainer(KAFKA_IMAGE)
            .withNetwork(OracleTestDatabase.NETWORK)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092")
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "900000")
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("kafka"));
    connect =
        new GenericContainer<>(KAFKA_IMAGE)
            .withNetwork(OracleTestDatabase.NETWORK)
            .withNetworkAliases("connect")
            .withExposedPorts(8083)
            .withCreateContainerCmdModifier(
                cmd ->
                    cmd.withEntrypoint(
                        "/opt/kafka/bin/connect-distributed.sh", "/connect.properties"))
            .withCopyToContainer(Transferable.of(workerProperties()), "/connect.properties")
            .withCopyFileToContainer(MountableFile.forHostPath(pluginDir()), "/plugins/oracle-cdc")
            .waitingFor(
                Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(Duration.ofMinutes(4)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("connect"));
  }

  public ConnectCluster start() {
    kafka.start();
    connect.start();
    LOG.info("Connect worker at {}", restUrl());
    return this;
  }

  static Path pluginDir() {
    String version = System.getProperty("project.version", "0.1.0-SNAPSHOT");
    Path root = Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
    Path dir =
        root.resolve("kafka-connect-oracle-cdc/target")
            .resolve("kafka-connect-oracle-cdc-" + version + "-kafka-connect-plugin")
            .resolve("osodevops-kafka-connect-oracle-cdc-" + version);
    if (!Files.isDirectory(dir)) {
      throw new IllegalStateException("plugin directory missing (run mvn package first): " + dir);
    }
    return dir;
  }

  static String workerProperties() {
    return String.join(
        "\n",
        "bootstrap.servers=kafka:19092",
        "group.id=oracle-cdc-e2e",
        "key.converter=org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable=false",
        "value.converter=org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable=false",
        "value.converter.decimal.format=NUMERIC",
        "config.storage.topic=_connect_configs",
        "offset.storage.topic=_connect_offsets",
        "status.storage.topic=_connect_status",
        "config.storage.replication.factor=1",
        "offset.storage.replication.factor=1",
        "status.storage.replication.factor=1",
        "offset.flush.interval.ms=1000",
        "exactly.once.source.support=enabled",
        "connector.client.config.override.policy=All",
        "plugin.path=/plugins",
        "listeners=HTTP://0.0.0.0:8083",
        "");
  }

  public String restUrl() {
    return "http://" + connect.getHost() + ":" + connect.getMappedPort(8083);
  }

  /** Runs a Kafka command-line tool inside the broker container and returns its output. */
  public String kafkaTool(String... command) throws Exception {
    var r = kafka.execInContainer(command);
    return r.getStdout() + r.getStderr();
  }

  public String bootstrapServers() {
    return kafka.getBootstrapServers();
  }

  /** Connector-side JDBC coordinates of the Oracle container on the shared network. */
  public static Map<String, String> oracleDatabaseProps(String pdbs) {
    return Map.of(
        "cdc.database.host", OracleTestDatabase.NETWORK_ALIAS,
        "cdc.database.port", "1521",
        "cdc.database.service", OracleTestDatabase.CDB_SERVICE,
        "cdc.database.user", OracleTestDatabase.CAPTURE_USER,
        "cdc.database.password", OracleTestDatabase.CAPTURE_PASSWORD,
        "cdc.database.pdbs", pdbs);
  }

  public void register(String name, Map<String, String> config)
      throws IOException, InterruptedException {
    String body = MAPPER.writeValueAsString(Map.of("name", name, "config", config));
    HttpResponse<String> r =
        http.send(
            HttpRequest.newBuilder(URI.create(restUrl() + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          "register " + name + ": HTTP " + r.statusCode() + " " + r.body());
    }
  }

  public JsonNode status(String name) throws IOException, InterruptedException {
    HttpResponse<String> r =
        http.send(
            HttpRequest.newBuilder(URI.create(restUrl() + "/connectors/" + name + "/status"))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    return r.statusCode() == 200 ? MAPPER.readTree(r.body()) : MAPPER.createObjectNode();
  }

  public JsonNode validate(String connectorClass, Map<String, String> config)
      throws IOException, InterruptedException {
    Map<String, String> payload = new java.util.HashMap<>(config);
    payload.putIfAbsent("name", "validate-" + Integer.toHexString(connectorClass.hashCode()));
    HttpResponse<String> r =
        http.send(
            HttpRequest.newBuilder(
                    URI.create(
                        restUrl() + "/connector-plugins/" + connectorClass + "/config/validate"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(payload)))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    return MAPPER.readTree(r.body());
  }

  /** Waits until the connector and its task report RUNNING; fails with the task trace otherwise. */
  public void awaitRunning(String name, Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    JsonNode last = null;
    while (System.currentTimeMillis() < deadline) {
      last = status(name);
      String c = last.path("connector").path("state").asText();
      JsonNode tasks = last.path("tasks");
      if ("RUNNING".equals(c)
          && tasks.size() > 0
          && "RUNNING".equals(tasks.get(0).path("state").asText())) {
        return;
      }
      if (tasks.size() > 0 && "FAILED".equals(tasks.get(0).path("state").asText())) {
        throw new AssertionError("task failed: " + tasks.get(0).path("trace").asText());
      }
      Thread.sleep(500);
    }
    throw new AssertionError("connector " + name + " not RUNNING: " + last);
  }

  /** Polls GET /connectors/{name}/offsets until an offset for the connector's partition exists. */
  public JsonNode awaitOffsets(String name, Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    JsonNode last = null;
    while (System.currentTimeMillis() < deadline) {
      HttpResponse<String> r =
          http.send(
              HttpRequest.newBuilder(URI.create(restUrl() + "/connectors/" + name + "/offsets"))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      if (r.statusCode() == 200) {
        last = MAPPER.readTree(r.body());
        if (last.path("offsets").size() > 0) {
          return last;
        }
      }
      Thread.sleep(500);
    }
    throw new AssertionError(
        "no committed offset for " + name + " within " + timeout + ": " + last);
  }

  public KafkaConsumer<String, String> consumer(String group, String... topics) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    KafkaConsumer<String, String> c = new KafkaConsumer<>(p);
    c.subscribe(List.of(topics));
    return c;
  }

  /**
   * Polls until {@code n} records arrive or {@code idle} passes with nothing new after at least
   * one.
   */
  public static List<ConsumerRecord<String, String>> consume(
      KafkaConsumer<String, String> c, int n, Duration timeout, Duration idle) {
    List<ConsumerRecord<String, String>> out = new ArrayList<>();
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    long lastNew = System.currentTimeMillis();
    while (System.currentTimeMillis() < deadline) {
      ConsumerRecords<String, String> batch = c.poll(Duration.ofMillis(500));
      if (!batch.isEmpty()) {
        batch.forEach(out::add);
        lastNew = System.currentTimeMillis();
      }
      if (out.size() >= n && System.currentTimeMillis() - lastNew > idle.toMillis()) {
        break;
      }
    }
    return out;
  }

  /** SIGKILL the worker, then start a fresh container that rejoins the same group and topics. */
  public void killAndRestartWorker() {
    connect.getDockerClient().killContainerCmd(connect.getContainerId()).withSignal("KILL").exec();
    connect.stop();
    connect.start();
    LOG.info("Connect worker restarted at {}", restUrl());
  }

  public static JsonNode json(String s) throws IOException {
    return s == null ? null : MAPPER.readTree(s);
  }

  @Override
  public void close() {
    connect.stop();
    kafka.stop();
  }
}
