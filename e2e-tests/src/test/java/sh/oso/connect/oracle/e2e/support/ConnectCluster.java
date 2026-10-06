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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

  /** Apicurio Registry (Apache-2.0); the e2e-tests build pins the tag to the converter version. */
  public static final String SCHEMA_REGISTRY_IMAGE =
      System.getProperty("apicurio.registry.image", "quay.io/apicurio/apicurio-registry:3.3.3");

  /** The registry's v3 API as the worker reaches it on the cluster network. */
  public static final String SCHEMA_REGISTRY_URL = "http://apicurio:8080/apis/registry/v3";

  private final KafkaContainer kafka;
  private final GenericContainer<?> connect;
  private GenericContainer<?> registry;
  private final HttpClient http = HttpClient.newHttpClient();

  public ConnectCluster() {
    this(Map.of());
  }

  /**
   * A cluster whose worker properties are the defaults below with {@code workerOverrides} applied,
   * for example {@code exactly.once.source.support=disabled} for an at-least-once worker.
   */
  public ConnectCluster(Map<String, String> workerOverrides) {
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
            .withCopyToContainer(
                Transferable.of(workerProperties(workerOverrides)), "/connect.properties")
            .withCopyFileToContainer(MountableFile.forHostPath(pluginDir()), "/plugins/oracle-cdc")
            .waitingFor(
                Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(Duration.ofMinutes(4)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("connect"));
  }

  /**
   * Mounts another plugin directory next to ours, as {@code /plugins/<name>}; call before {@link
   * #start}. The Debezium cutover suite mounts the Debezium Oracle connector this way, and the Avro
   * suite the Apicurio Avro converter ({@link #apicurioConverterDir()}).
   */
  public ConnectCluster withPlugin(Path dir, String name) {
    if (!Files.isDirectory(dir)) {
      throw new IllegalStateException("plugin directory missing: " + dir);
    }
    connect.withCopyFileToContainer(MountableFile.forHostPath(dir), "/plugins/" + name);
    return this;
  }

  /**
   * The Debezium Oracle connector plugin unpacked by the e2e-tests build (pre-integration-test).
   */
  public static Path debeziumPluginDir() {
    String dir = System.getProperty("debezium.plugin.dir");
    if (dir == null) {
      throw new IllegalStateException(
          "debezium.plugin.dir is not set; run the suite through Maven failsafe");
    }
    Path p = Path.of(dir);
    if (!Files.isDirectory(p)) {
      throw new IllegalStateException(
          "Debezium plugin directory missing (the e2e-tests build unpacks it in"
              + " pre-integration-test): "
              + p);
    }
    return p;
  }

  /**
   * The Apicurio Avro converter and its runtime jars, staged by the e2e-tests build
   * (pre-integration-test) for {@link #withPlugin}.
   */
  public static Path apicurioConverterDir() {
    String dir = System.getProperty("apicurio.converter.dir");
    if (dir == null) {
      throw new IllegalStateException(
          "apicurio.converter.dir is not set; run the suite through Maven failsafe");
    }
    Path p = Path.of(dir);
    if (!Files.isDirectory(p)) {
      throw new IllegalStateException(
          "Apicurio converter directory missing (the e2e-tests build stages it in"
              + " pre-integration-test): "
              + p);
    }
    return p;
  }

  /**
   * Adds an Apicurio Registry with in-memory storage on the cluster network, reachable from the
   * worker at {@link #SCHEMA_REGISTRY_URL} and from the test at {@link #schemaRegistryUrl()}; call
   * before {@link #start}. It starts after the broker and before the worker, and nothing in the
   * worker's own configuration changes: connectors opt in through their converter settings.
   */
  public ConnectCluster withSchemaRegistry() {
    registry =
        new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
            .withNetwork(OracleTestDatabase.NETWORK)
            .withNetworkAliases("apicurio")
            .withExposedPorts(8080)
            // the default storage, stated: an H2 database in memory, gone with the container
            .withEnv("APICURIO_STORAGE_KIND", "sql")
            .withEnv("APICURIO_STORAGE_SQL_KIND", "h2")
            .waitingFor(
                Wait.forHttp("/apis/registry/v3/system/info")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("apicurio"));
    return this;
  }

  /** The registry's v3 API from the test JVM; needs {@link #withSchemaRegistry()}. */
  public String schemaRegistryUrl() {
    if (registry == null) {
      throw new IllegalStateException("no schema registry: call withSchemaRegistry() first");
    }
    return "http://"
        + registry.getHost()
        + ":"
        + registry.getMappedPort(8080)
        + "/apis/registry/v3";
  }

  public ConnectCluster start() {
    kafka.start();
    if (registry != null) {
      registry.start();
      LOG.info("Schema registry at {}", schemaRegistryUrl());
    }
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

  static String workerProperties(Map<String, String> overrides) {
    java.util.LinkedHashMap<String, String> props = new java.util.LinkedHashMap<>();
    for (String line : defaultWorkerProperties().split("\n")) {
      int eq = line.indexOf('=');
      if (eq > 0) {
        props.put(line.substring(0, eq), line.substring(eq + 1));
      }
    }
    props.putAll(overrides);
    StringBuilder sb = new StringBuilder();
    props.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
    return sb.toString();
  }

  private static String defaultWorkerProperties() {
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

  /**
   * Mounts a tmpfs of the given size (for example {@code 8m}) in the worker container, writable by
   * the worker user; call before {@link #start()}. A full spill volume is then a real ENOSPC.
   */
  public ConnectCluster withWorkerTmpFs(String path, String size) {
    connect.withTmpFs(Map.of(path, "rw,size=" + size + ",mode=1777"));
    return this;
  }

  /**
   * Binds the broker's host listener to a fixed free port instead of an ephemeral one; call before
   * {@link #start()}. {@link #restartBroker} keeps the container, and with an ephemeral port Docker
   * may publish a different one after the restart, while the advertised listener still names the
   * first: host-side consumers would lose the broker.
   */
  public ConnectCluster withPinnedKafkaHostPort() {
    int port;
    try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
      port = probe.getLocalPort();
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    final int hostPort = port;
    kafka.withCreateContainerCmdModifier(
        cmd -> {
          // Testcontainers binds every exposed port to an ephemeral host port: replace the
          // broker's binding rather than add a second one
          com.github.dockerjava.api.model.ExposedPort broker =
              com.github.dockerjava.api.model.ExposedPort.tcp(9092);
          com.github.dockerjava.api.model.Ports ports = new com.github.dockerjava.api.model.Ports();
          com.github.dockerjava.api.model.Ports existing = cmd.getHostConfig().getPortBindings();
          if (existing != null) {
            existing
                .getBindings()
                .forEach(
                    (exposed, bindings) -> {
                      if (!broker.equals(exposed) && bindings != null) {
                        for (com.github.dockerjava.api.model.Ports.Binding b : bindings) {
                          ports.bind(exposed, b);
                        }
                      }
                    });
          }
          ports.bind(broker, com.github.dockerjava.api.model.Ports.Binding.bindPort(hostPort));
          cmd.getHostConfig().withPortBindings(ports);
        });
    return this;
  }

  /**
   * Restarts the broker container in place (its log directory survives), with SIGKILL when {@code
   * crash} is true and a graceful stop otherwise, then waits until a host-side admin client sees
   * the broker again. Needs {@link #withPinnedKafkaHostPort()} for host-side clients.
   */
  public void restartBroker(boolean crash, Duration timeout) throws Exception {
    var docker = kafka.getDockerClient();
    String id = kafka.getContainerId();
    if (crash) {
      docker.killContainerCmd(id).withSignal("KILL").exec();
    } else {
      docker.stopContainerCmd(id).withTimeout(30).exec();
    }
    docker.startContainerCmd(id).exec();
    awaitBroker(timeout);
    LOG.info("Kafka broker restarted ({})", crash ? "SIGKILL" : "graceful stop");
  }

  /** Waits until a host-side admin client can describe the cluster. */
  public void awaitBroker(Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    Exception last = null;
    while (System.currentTimeMillis() < deadline) {
      Properties p = new Properties();
      p.put("bootstrap.servers", kafka.getBootstrapServers());
      p.put("request.timeout.ms", "5000");
      p.put("default.api.timeout.ms", "5000");
      try (org.apache.kafka.clients.admin.Admin admin =
          org.apache.kafka.clients.admin.Admin.create(p)) {
        admin.describeCluster().nodes().get(10, java.util.concurrent.TimeUnit.SECONDS);
        admin.listTopics().names().get(10, java.util.concurrent.TimeUnit.SECONDS);
        return;
      } catch (Exception e) {
        last = e;
        Thread.sleep(1000);
      }
    }
    throw new IllegalStateException("broker not reachable within " + timeout, last);
  }

  /**
   * PUT /connectors/{name}/config: Connect restarts the connector and tasks with the new config.
   */
  public void updateConfig(String name, Map<String, String> config)
      throws IOException, InterruptedException {
    HttpResponse<String> r =
        http.send(
            HttpRequest.newBuilder(URI.create(restUrl() + "/connectors/" + name + "/config"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(config)))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          "update config " + name + ": HTTP " + r.statusCode() + " " + r.body());
    }
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

  /** PUT /connectors/{name}/stop, /resume, or POST /restart?includeTasks=true, as an operator. */
  public void lifecycle(String name, String action) throws IOException, InterruptedException {
    String path = "restart".equals(action) ? "restart?includeTasks=true" : action;
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create(restUrl() + "/connectors/" + name + "/" + path));
    b =
        "restart".equals(action)
            ? b.POST(HttpRequest.BodyPublishers.noBody())
            : b.PUT(HttpRequest.BodyPublishers.noBody());
    HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          action + " " + name + ": HTTP " + r.statusCode() + " " + r.body());
    }
  }

  /** PATCH /connectors/{name}/offsets with one offset for the connector's partition. */
  public void patchOffset(String name, Map<String, ?> partition, Map<String, ?> offset)
      throws IOException, InterruptedException {
    String body =
        MAPPER.writeValueAsString(
            Map.of("offsets", List.of(Map.of("partition", partition, "offset", offset))));
    HttpResponse<String> r =
        http.send(
            HttpRequest.newBuilder(URI.create(restUrl() + "/connectors/" + name + "/offsets"))
                .header("Content-Type", "application/json")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          "patch offsets " + name + ": HTTP " + r.statusCode() + " " + r.body());
    }
  }

  /** Waits until the task reports {@code state}; returns the status. */
  public JsonNode awaitTaskState(String name, String state, Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    JsonNode last = null;
    while (System.currentTimeMillis() < deadline) {
      last = status(name);
      JsonNode tasks = last.path("tasks");
      if (tasks.size() > 0 && state.equals(tasks.get(0).path("state").asText())) {
        return last;
      }
      if ("STOPPED".equals(state)
          && "STOPPED".equals(last.path("connector").path("state").asText())) {
        return last;
      }
      Thread.sleep(500);
    }
    throw new AssertionError("connector " + name + " task not " + state + ": " + last);
  }

  /** The connector's own state (RUNNING, PAUSED, STOPPED, FAILED), or null when unknown. */
  public String connectorState(String name) throws IOException, InterruptedException {
    JsonNode s = status(name);
    return s.path("connector").path("state").isMissingNode()
        ? null
        : s.path("connector").path("state").asText();
  }

  /** Waits until the connector itself (not its task) reports {@code state}. */
  public void awaitConnectorState(String name, String state, Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    String last = null;
    while (System.currentTimeMillis() < deadline) {
      last = connectorState(name);
      if (state.equals(last)) {
        return;
      }
      Thread.sleep(500);
    }
    throw new AssertionError("connector " + name + " not " + state + ": " + last);
  }

  /** GET /connectors/{name}: the name and the configuration as the worker holds them. */
  public JsonNode connectorInfo(String name) throws IOException, InterruptedException {
    HttpResponse<String> r =
        http.send(
            HttpRequest.newBuilder(URI.create(restUrl() + "/connectors/" + name)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() != 200) {
      throw new IllegalStateException("GET connector " + name + ": HTTP " + r.statusCode());
    }
    return MAPPER.readTree(r.body());
  }

  /** GET /connectors/{name}/offsets as returned, whatever it holds. */
  public JsonNode offsets(String name) throws IOException, InterruptedException {
    HttpResponse<String> r =
        http.send(
            HttpRequest.newBuilder(URI.create(restUrl() + "/connectors/" + name + "/offsets"))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() != 200) {
      throw new IllegalStateException(
          "GET offsets " + name + ": HTTP " + r.statusCode() + " " + r.body());
    }
    return MAPPER.readTree(r.body());
  }

  /** DELETE /connectors/{name}. */
  public void delete(String name) throws IOException, InterruptedException {
    HttpResponse<String> r =
        http.send(
            HttpRequest.newBuilder(URI.create(restUrl() + "/connectors/" + name)).DELETE().build(),
            HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() / 100 != 2 && r.statusCode() != 404) {
      throw new IllegalStateException(
          "delete " + name + ": HTTP " + r.statusCode() + " " + r.body());
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

  /** Produces one string record, as an operator would with a console producer. */
  public void produce(String topic, String key, String value) throws Exception {
    Properties p = new Properties();
    p.put("bootstrap.servers", kafka.getBootstrapServers());
    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer =
        new org.apache.kafka.clients.producer.KafkaProducer<>(p)) {
      producer
          .send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, value))
          .get();
    }
  }

  /** A read_committed consumer from the earliest offset that leaves keys and values as bytes. */
  public KafkaConsumer<byte[], byte[]> byteConsumer(String group, String... topics) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p);
    c.subscribe(List.of(topics));
    return c;
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
  public static <K, V> List<ConsumerRecord<K, V>> consume(
      KafkaConsumer<K, V> c, int n, Duration timeout, Duration idle) {
    List<ConsumerRecord<K, V>> out = new ArrayList<>();
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    long lastNew = System.currentTimeMillis();
    while (System.currentTimeMillis() < deadline) {
      ConsumerRecords<K, V> batch = c.poll(Duration.ofMillis(500));
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
    if (registry != null) {
      registry.stop();
    }
    kafka.stop();
  }
}
