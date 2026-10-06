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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.EngineMetrics;
import sh.oso.connect.oracle.core.engine.EngineSettings;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.metrics.SinkMetrics;
import sh.oso.connect.oracle.core.metrics.TaskMetrics;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;
import sh.oso.connect.oracle.delivery.RecordQueueSink;
import sh.oso.connect.oracle.dlq.DecodeDlqWriter;
import sh.oso.connect.oracle.doctor.admin.Database;
import sh.oso.connect.oracle.doctor.admin.Environment;
import sh.oso.connect.oracle.doctor.connect.ConnectApi;
import sh.oso.connect.oracle.doctor.kafka.KafkaPort;
import sh.oso.connect.oracle.doctor.metrics.MetricsSource;
import sh.oso.connect.oracle.e2e.support.Cli;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;
import sh.oso.connect.oracle.envelope.DebeziumEnvelope;
import sh.oso.connect.oracle.envelope.TopicRouter;
import sh.oso.connect.oracle.heartbeat.HeartbeatEmitter;
import sh.oso.connect.oracle.journal.JournalRecords;
import sh.oso.connect.oracle.ops.OpsEventWriter;

/**
 * PRD-05 {@code explain-lag} end to end: the real engine mines Oracle Database Free, its task
 * MXBean is served over a JMX connector in this JVM, and the CLI reads it twice and names the
 * bottleneck the suite scripted.
 *
 * <ul>
 *   <li>A large open transaction: an uncommitted 20,000-row insert, mined into the buffer.
 *   <li>Mining slower than its target: an uncaptured 250,000-row update fills more than one
 *       archived log, and the engine mines it with a tiny cdc.mining.target.latency.ms and a
 *       one-second LogMiner query timeout (the product's floor is ten seconds; the suite shortens
 *       it so a small amount of redo is enough), so its steps time out while the CLI watches. The
 *       explainer only names mining for a step of five seconds or more, or a step timeout, so a
 *       tiny target alone cannot produce it.
 *   <li>The Kafka side: the task's own record queue, sized as the task sizes it, holding a
 *       committed 900-row transaction that no worker drains, as when Connect takes records more
 *       slowly than the connector produces them. The queue is left just short of full: a full queue
 *       blocks the engine inside the sink, and the sink's synchronised metrics would then block the
 *       JMX read as well.
 * </ul>
 *
 * The explainer's arithmetic for each signature, including a full queue, is covered with fakes in
 * {@code LagExplainerTest} and {@code DoctorCommandsTest}.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExplainLagEngineIT {

  static final int LARGE_ROWS = 20_000;
  static final int NOISE_ROWS = 250_000;
  static final int QUEUE_ROWS = 900;
  static final long BUDGET = 1L << 20; // cdc.buffer.memory.max.bytes of the scripted task

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private JMXConnectorServer jmx;
  private String jmxUrl;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      exec(
          w,
          "CREATE TABLE big (id NUMBER(9) PRIMARY KEY, pad VARCHAR2(200))",
          "ALTER TABLE big ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
          "CREATE TABLE queued (id NUMBER(9) PRIMARY KEY, v VARCHAR2(40))",
          "ALTER TABLE queued ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
          "CREATE TABLE noise (id NUMBER(9) PRIMARY KEY, n NUMBER(12))");
    }
    // an RMI connector over the platform MBean server, on the loopback address, as a worker
    // started with com.sun.management.jmxremote would serve its task MXBeans
    if (System.getProperty("java.rmi.server.hostname") == null) {
      System.setProperty("java.rmi.server.hostname", "127.0.0.1");
    }
    jmx =
        JMXConnectorServerFactory.newJMXConnectorServer(
            new JMXServiceURL("service:jmx:rmi://127.0.0.1"),
            null,
            ManagementFactory.getPlatformMBeanServer());
    jmx.start();
    jmxUrl = jmx.getAddress().toString();
  }

  @AfterAll
  void down() throws Exception {
    if (jmx != null) {
      jmx.stop();
    }
    SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
  }

  @Test
  void aLargeOpenTransactionIsNamed(@TempDir Path dir) throws Exception {
    ObjectName name = null;
    Connection open = db.connect(OracleTestDatabase.PDB1, schema, schema);
    try (Connection meta = metadata();
        Connection mining = mining()) {
      long start = LogMinerHelper.currentScn(meta);
      open.setAutoCommit(false);
      String xid;
      try (Statement s = open.createStatement()) {
        s.execute(
            "INSERT INTO big SELECT LEVEL, RPAD('p', 200, 'p') FROM dual CONNECT BY LEVEL <= "
                + LARGE_ROWS);
        try (ResultSet rs =
            s.executeQuery("SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM dual")) {
          rs.next();
          xid = rs.getString(1);
        }
      }
      OracleSql.archiveLogCurrent(db); // binds the open session's strand, so its redo is mined
      long end = LogMinerHelper.currentScn(meta);
      try (EngineDriver d =
          new EngineDriver(meta, mining, null, include("BIG"), start, LobAssembler.Mode.SKIP)) {
        d.runTo(end);
        assertThat(d.engine.cursor().scn()).as("mined to the end").isGreaterThanOrEqualTo(end);
        assertThat(d.committed).isEmpty();
        name =
            new TaskMetrics(d.engine.metrics(), SinkMetrics.NONE, BUDGET, 1L << 30, Instant::now)
                .register("lag-large");
        Cli.Result r =
            explain(
                new Hooked(() -> {}),
                config(
                    dir,
                    "lag-large",
                    Map.of(CoreConfig.BUFFER_MEMORY_MAX_BYTES, Long.toString(BUDGET))));
        assertThat(r.exit()).as(r.toString()).isZero();
        String top = top(r.out());
        assertThat(top)
            .as(r.out())
            .startsWith("Transaction ")
            .contains(xid)
            .contains("of user " + schema)
            .contains("Its records are published only when it commits");
        Matcher changes = Pattern.compile("with (\\d+) changes").matcher(top);
        assertThat(changes.find()).as(top).isTrue();
        assertThat(Long.parseLong(changes.group(1))).isGreaterThanOrEqualTo(LARGE_ROWS);
        assertThat(r.out()).contains("| Buffer | 1 open transactions, ");
        System.out.println("explain-lag: large transaction " + xid + " named");
      }
    } finally {
      TaskMetrics.unregister(name);
      try {
        open.rollback();
        open.close();
      } catch (SQLException ignore) {
        // the session is gone already
      }
    }
  }

  @Test
  void miningSlowerThanItsTargetIsNamed(@TempDir Path dir) throws Exception {
    ObjectName name = null;
    try (Connection meta = metadata();
        // every step's LogMiner query waits six seconds first: mining stays behind its target
        // whatever the host's speed (an awake workstation mines the batch job in milliseconds)
        Connection mining = slowContents(mining(), 6_000);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      exec(w, "INSERT INTO noise SELECT LEVEL, LEVEL FROM dual CONNECT BY LEVEL <= " + NOISE_ROWS);
      OracleSql.archiveLogCurrent(db); // the batch job starts in a fresh log
      long start = LogMinerHelper.currentScn(meta);
      exec(w, "INSERT INTO big VALUES (-1, 'before the batch job')", "UPDATE noise SET n = n + 1");
      OracleSql.archiveLogCurrent(db);
      long end = LogMinerHelper.currentScn(meta);

      // the target the explainer reads from the configuration is the one the engine runs with
      long target = 100;
      EngineSettings settings =
          new EngineSettings(
              Duration.ofMillis(target),
              8,
              Duration.ofHours(1),
              Duration.ofMillis(50),
              3,
              CoreConfig.DecodeErrorAction.FAIL);
      try (EngineDriver d =
          new EngineDriver(
              meta,
              mining,
              null,
              include("BIG"),
              start,
              LobAssembler.Mode.SKIP,
              new InMemorySchemaStore(),
              EngineDriver.Options.defaults()
                  .withQueryTimeout(Duration.ofSeconds(1))
                  .withSettings(settings))) {
        EngineMetrics m = d.engine.metrics();
        name =
            new TaskMetrics(m, SinkMetrics.NONE, BUDGET, 1L << 30, Instant::now)
                .register("lag-mining");
        CountDownLatch go = new CountDownLatch(1);
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<Throwable> stopped = new AtomicReference<>();
        Thread engine =
            new Thread(
                () -> {
                  try {
                    go.await();
                    while (!Thread.currentThread().isInterrupted()
                        && d.engine.cursor().scn() < end) {
                      if (d.engine.runOnce() == CaptureEngine.Progress.IDLE) {
                        Thread.sleep(100);
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } catch (Throwable e) {
                    stopped.set(e);
                  } finally {
                    done.set(true);
                  }
                },
                "explain-lag-engine");
        engine.setDaemon(true);
        engine.start();
        // the CLI takes its first reading before the engine mines anything; between its two
        // readings the engine runs until a step times out or runs long
        Hooked env =
            new Hooked(
                () -> {
                  go.countDown();
                  long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
                  while (!done.get()
                      && m.stepTimeouts.get() == 0
                      && m.lastStepMillis.get() < 5_000
                      && System.currentTimeMillis() < deadline) {
                    sleepQuietly(200);
                  }
                });
        Cli.Result r;
        try {
          r =
              explain(
                  env,
                  config(
                      dir,
                      "lag-mining",
                      Map.of(CoreConfig.MINING_TARGET_LATENCY_MS, Long.toString(target))));
        } finally {
          engine.interrupt();
          engine.join(Duration.ofMinutes(1).toMillis());
        }
        String figures =
            "timeouts "
                + m.stepTimeouts.get()
                + ", last step "
                + m.lastStepMillis.get()
                + " ms, steps "
                + m.steps.get()
                + ", engine "
                + (stopped.get() == null ? "running or done" : stopped.get().toString());
        assertThat(r.exit()).as(r.toString()).isZero();
        String top = top(r.out());
        assertThat(top).as("%s\n%s", figures, r.out()).startsWith("Mining is the bottleneck");
        if (m.stepTimeouts.get() > 0) {
          assertThat(top).contains("steps hit cdc.mining.query.timeout.ms");
        }
        assertThat(top).contains("against a target of " + target + " ms");
        assertThat(r.out()).contains("| Mining | last step ");
        System.out.println("explain-lag: mining named (" + figures + ")");
      }
    } finally {
      TaskMetrics.unregister(name);
    }
  }

  @Test
  void aRecordQueueNoWorkerDrainsIsNamedAsTheKafkaSide(@TempDir Path dir) throws Exception {
    ObjectName name = null;
    Map<String, String> props = new HashMap<>();
    props.put(CoreConfig.DATABASE_URL, db.jdbcUrl(OracleTestDatabase.CDB_SERVICE));
    props.put(CoreConfig.DATABASE_USER, OracleTestDatabase.CAPTURE_USER);
    props.put(CoreConfig.DATABASE_PASSWORD, OracleTestDatabase.CAPTURE_PASSWORD);
    props.put(CoreConfig.DATABASE_PDBS, OracleTestDatabase.PDB1);
    props.put(OracleCdcSourceConnectorConfig.TOPIC_PREFIX, "lag-kafka");
    props.put("cdc.tables.include", include("QUEUED"));
    props.put(OracleCdcSourceConnectorConfig.POLL_MAX_RECORDS, "250");
    OracleCdcSourceConnectorConfig config = new OracleCdcSourceConnectorConfig(props);
    int capacity = Math.max(1000, config.pollMaxRecords() * 4); // as the task sizes its queue
    try (Connection meta = metadata();
        Connection mining = mining();
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      long start = LogMinerHelper.currentScn(meta);
      exec(
          w,
          "INSERT INTO queued SELECT LEVEL, 'q ' || LEVEL FROM dual CONNECT BY LEVEL <= "
              + QUEUE_ROWS);
      long end = LogMinerHelper.currentScn(meta);
      DatabaseInfo info = new JdbcCatalogSource(() -> meta).database();
      Position base =
          Position.initial(start, new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn()));
      AtomicReference<RecordQueueSink> queue = new AtomicReference<>();
      EngineDriver.Options options =
          EngineDriver.Options.defaults()
              .withSink(
                  registry -> {
                    DebeziumEnvelope envelope =
                        new DebeziumEnvelope(
                            config,
                            new TopicRouter(
                                config.topicTemplate(info.cdb()),
                                config.topicPrefix(),
                                info.name()),
                            info.name());
                    RecordQueueSink sink =
                        new RecordQueueSink(
                            envelope,
                            registry,
                            base,
                            capacity,
                            new HeartbeatEmitter(
                                config.heartbeatTopic(),
                                config.topicPrefix(),
                                envelope.partition()),
                            new OpsEventWriter(
                                config.opsTopic(), config.topicPrefix(), envelope.partition()),
                            new JournalRecords(
                                config.journalTopic(), config.topicPrefix(), envelope.partition()),
                            new DecodeDlqWriter(
                                config.dlqTopic(), config.topicPrefix(), envelope.partition()),
                            0, // no heartbeats: only the transaction's records wait in the queue
                            System::currentTimeMillis);
                    queue.set(sink);
                    return sink;
                  });
      try (EngineDriver d =
          new EngineDriver(
              meta,
              mining,
              null,
              include("QUEUED"),
              start,
              LobAssembler.Mode.SKIP,
              new InMemorySchemaStore(),
              options)) {
        d.runTo(end);
        RecordQueueSink sink = queue.get();
        assertThat(sink.queued()).as("records waiting for Connect").isEqualTo(QUEUE_ROWS);
        name =
            new TaskMetrics(d.engine.metrics(), sink, BUDGET, 1L << 30, Instant::now)
                .register("lag-kafka");
        Map<String, String> file = new LinkedHashMap<>();
        file.put(OracleCdcSourceConnectorConfig.POLL_MAX_RECORDS, "250");
        Cli.Result r = explain(new Hooked(() -> {}), config(dir, "lag-kafka", file));
        assertThat(r.exit()).as(r.toString()).isZero();
        assertThat(top(r.out()))
            .as(r.out())
            .startsWith(
                "The record queue is full ("
                    + QUEUE_ROWS
                    + " of "
                    + capacity
                    + " records): Kafka Connect takes records more slowly");
        assertThat(r.out())
            .contains("| Record queue | " + QUEUE_ROWS + " of " + capacity + " records waiting");
        // the queue holds the transaction's change records, in commit order
        assertThat(sink.drain(QUEUE_ROWS, 100))
            .hasSize(QUEUE_ROWS)
            .allMatch(rec -> rec.topic().endsWith(schema + ".QUEUED"));
        System.out.println("explain-lag: a full record queue named as the Kafka side");
      }
    } finally {
      TaskMetrics.unregister(name);
    }
  }

  /** Runs explain-lag over the JMX connector, bounded so a blocked JMX read fails the test. */
  private Cli.Result explain(Environment env, Path config) throws Exception {
    ExecutorService ex = Executors.newSingleThreadExecutor();
    try {
      Future<Cli.Result> f =
          ex.submit(
              () ->
                  Cli.doctor(
                      env,
                      "explain-lag",
                      "--jmx-url",
                      jmxUrl,
                      "--config",
                      config.toString(),
                      "--interval",
                      "1s"));
      return f.get(5, TimeUnit.MINUTES);
    } finally {
      ex.shutdownNow();
    }
  }

  /**
   * A connector configuration file naming the metrics server and the settings explain-lag reads.
   */
  private static Path config(Path dir, String server, Map<String, String> settings)
      throws IOException {
    Map<String, String> c = new LinkedHashMap<>(settings);
    c.put(OracleCdcSourceConnectorConfig.TOPIC_PREFIX, server);
    Path file = dir.resolve(server + ".json");
    Files.writeString(
        file, new ObjectMapper().writeValueAsString(Map.of("name", server, "config", c)));
    return file;
  }

  /** The explanation's first paragraph: the named cause. */
  private static String top(String out) {
    String[] parts = out.split("\n\n");
    assertThat(parts.length).as(out).isGreaterThan(1);
    return parts[1];
  }

  private String include(String table) {
    return "FREEPDB1\\." + schema + "\\." + table;
  }

  private Connection metadata() throws SQLException {
    Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
    SessionInitializer.apply(c, ConnectionRole.METADATA);
    return c;
  }

  /**
   * {@code real}, whose statements on {@code V$LOGMNR_CONTENTS} sleep {@code millis} before each
   * execution.
   */
  private static Connection slowContents(Connection real, long millis) {
    return (Connection)
        java.lang.reflect.Proxy.newProxyInstance(
            ExplainLagEngineIT.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              Object out = invoke(method, real, args);
              if (out instanceof java.sql.PreparedStatement ps
                  && args != null
                  && args.length > 0
                  && args[0] instanceof String sql
                  && sql.toUpperCase(java.util.Locale.ROOT).contains("V$LOGMNR_CONTENTS")) {
                return java.lang.reflect.Proxy.newProxyInstance(
                    ExplainLagEngineIT.class.getClassLoader(),
                    new Class<?>[] {java.sql.PreparedStatement.class},
                    (p2, m2, a2) -> {
                      if (m2.getName().startsWith("execute")) {
                        try {
                          Thread.sleep(millis);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new SQLException("interrupted while mining", "08000", 17008);
                        }
                      }
                      return invoke(m2, ps, a2);
                    });
              }
              return out;
            });
  }

  private static Object invoke(java.lang.reflect.Method m, Object target, Object[] args)
      throws Throwable {
    try {
      return m.invoke(target, args);
    } catch (java.lang.reflect.InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private Connection mining() throws SQLException {
    Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
    SessionInitializer.apply(c, ConnectionRole.MINING);
    return c;
  }

  private static void exec(Connection c, String... statements) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : statements) {
        s.execute(q);
      }
    }
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * The standard environment, with a hook that runs between the CLI's two readings (inside its
   * sleep), so a suite can make the engine work exactly in the interval the CLI measures.
   */
  static final class Hooked implements Environment {
    private final Environment real = Environment.standard();
    private final Runnable between;

    Hooked(Runnable between) {
      this.between = between;
    }

    @Override
    public ConnectApi connect(String url) {
      return real.connect(url);
    }

    @Override
    public KafkaPort kafka(Properties clientProps) {
      return real.kafka(clientProps);
    }

    @Override
    public Database database(CoreConfig config) throws Exception {
      return real.database(config);
    }

    @Override
    public MetricsSource jmx(String url) throws IOException {
      return real.jmx(url);
    }

    @Override
    public MetricsSource prometheus(String url) {
      return real.prometheus(url);
    }

    @Override
    public Clock clock() {
      return real.clock();
    }

    @Override
    public void sleep(Duration d) throws InterruptedException {
      between.run();
      real.sleep(d);
    }
  }
}
