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
package sh.oso.connect.oracle.e2e.nightly;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.check.CorrectnessCheck;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;

/**
 * What the T2 suites share: the connector configuration they start from, an open-ended seeded
 * workload, the wait for the connector to catch up, the correctness oracle over the workload's
 * topics, and readers for the ops topic and the task's failure trace. Not a test.
 */
final class NightlyRun {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  private static final Pattern CDC_CODE = Pattern.compile("CDC-\\d{4}");

  private NightlyRun() {}

  /** Seed for a suite: {@code -Dnightly.seed} when given, else the suite's own default. */
  static long seed(long dflt) {
    return Long.getLong("nightly.seed", dflt);
  }

  /**
   * A mixed workload (inserts, updates, key changes, deletes, savepoint and full rollbacks over
   * several sessions and tables) that runs until {@link WorkloadGenerator#requestStop()}; the
   * duration is only a safety bound.
   */
  static WorkloadSpec openEnded(long seed) {
    WorkloadSpec spec = WorkloadSpec.defaults();
    spec.seed = seed;
    spec.sessions = 2;
    spec.tables = 3;
    spec.transactionsPerSession = 0;
    spec.durationSeconds = 4 * 3600;
    spec.maxRowsPerTransaction = 6;
    spec.insertWeight = 4;
    spec.updateWeight = 3;
    spec.deleteWeight = 1.5;
    spec.keyChangeWeight = 0.5;
    spec.lobWeight = 0;
    spec.savepointRollbackProbability = 0.1;
    spec.fullRollbackProbability = 0.05;
    return spec;
  }

  /** The workload's parameters for the evidence file. */
  static Map<String, Object> describe(WorkloadSpec spec) {
    Map<String, Object> m = new java.util.LinkedHashMap<>();
    m.put("seed", spec.seed);
    m.put("sessions", spec.sessions);
    m.put("tables", spec.tables);
    m.put("tablePrefix", spec.tablePrefix);
    m.put("maxRowsPerTransaction", spec.maxRowsPerTransaction);
    m.put("largeTransactionEvery", spec.largeTransactionEvery);
    m.put("largeTransactionRows", spec.largeTransactionRows);
    m.put("savepointRollbackProbability", spec.savepointRollbackProbability);
    m.put("fullRollbackProbability", spec.fullRollbackProbability);
    m.put("keyChangeWeight", spec.keyChangeWeight);
    m.put("lobWeight", spec.lobWeight);
    return m;
  }

  /**
   * The connector configuration every suite starts from: one task, the given tables of FREEPDB1, no
   * initial snapshot (the tables are empty when the connector starts), heartbeats every second so
   * the committed offset moves on a quiet database.
   */
  static Map<String, String> connector(
      String prefix, String schema, String tableRegex, Map<String, String> database) {
    Map<String, String> c = new HashMap<>(database);
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", prefix);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\." + tableRegex);
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "1000");
    c.put("cdc.snapshot.mode", "none");
    return c;
  }

  /** {@link #connector} for the data tables of a workload spec (its ledger is not captured). */
  static Map<String, String> connector(
      String prefix, String schema, WorkloadSpec spec, Map<String, String> database) {
    return connector(prefix, schema, spec.tablePrefix + "[0-9]+", database);
  }

  static List<String> tables(WorkloadSpec spec) {
    List<String> out = new ArrayList<>();
    for (int i = 1; i <= spec.tables; i++) {
      out.add(spec.tableName(i));
    }
    return out;
  }

  static String topic(String prefix, String schema, String table) {
    return prefix + ".FREEPDB1." + schema + "." + table;
  }

  /** Runs the generator on a thread of its own. */
  static CompletableFuture<WorkloadResult> start(WorkloadGenerator g) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return g.run();
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        },
        r -> {
          Thread t = new Thread(r, "nightly-workload");
          t.setDaemon(true);
          t.start();
        });
  }

  /** Ends an open-ended workload; every transaction it began is committed or rolled back. */
  static WorkloadResult stop(WorkloadGenerator g, CompletableFuture<WorkloadResult> run)
      throws Exception {
    g.requestStop();
    g.resume();
    return run.get(5, TimeUnit.MINUTES);
  }

  /** The committed offset of the connector's partition. */
  static JsonNode offset(ConnectCluster cluster, String name) throws Exception {
    return cluster.awaitOffsets(name, Duration.ofSeconds(60)).path("offsets").get(0).path("offset");
  }

  /**
   * Waits until the committed resume SCN reaches {@code scn}: everything committed before it has
   * been delivered and acknowledged (heartbeats move the offset on a quiet database). Returns false
   * on timeout, so the caller can record that and let the oracle judge.
   */
  static boolean awaitResumePast(ConnectCluster cluster, String name, long scn, Duration timeout)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    long last = -1;
    while (System.currentTimeMillis() < deadline) {
      try {
        last = offset(cluster, name).path("resume_scn").asLong();
        if (last >= scn) {
          return true;
        }
      } catch (AssertionError | RuntimeException e) {
        // no offset yet, or the REST call failed during a fault: try again
      }
      Thread.sleep(1000);
    }
    System.out.println(
        "nightly: committed resume_scn " + last + " did not reach " + scn + " in " + timeout);
    return false;
  }

  /** The correctness oracle over a workload's tables and ledger. */
  static CheckReport check(
      ConnectCluster cluster,
      Connection w,
      String schema,
      WorkloadSpec spec,
      String prefix,
      Duration timeout)
      throws Exception {
    List<String> tables = tables(spec);
    List<String> topics = tables.stream().map(t -> topic(prefix, schema, t)).toList();
    CorrectnessCheck check =
        new CorrectnessCheck(
            cluster.bootstrapServers(),
            topics,
            w,
            schema,
            tables,
            spec.ledgerTable,
            timeout,
            Duration.ofSeconds(20));
    return check.run();
  }

  /** Committed XIDs in a workload ledger whose transactions changed at least one row. */
  static Set<String> ledgerXids(Connection w, String schema, WorkloadSpec spec) throws Exception {
    Set<String> out = new LinkedHashSet<>();
    try (PreparedStatement ps =
            w.prepareStatement(
                "SELECT xid FROM "
                    + schema
                    + "."
                    + spec.ledgerTable
                    + " WHERE ops > 0 ORDER BY seq");
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        out.add(rs.getString(1));
      }
    }
    return out;
  }

  /** Every ops event of the connector, oldest first. */
  static List<JsonNode> ops(ConnectCluster cluster, String prefix) throws Exception {
    List<JsonNode> out = new ArrayList<>();
    try (KafkaConsumer<String, String> c =
        cluster.consumer("nightly-ops-" + System.nanoTime(), prefix + ".cdc.ops")) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(c, 1, Duration.ofSeconds(30), Duration.ofSeconds(5))) {
        if (r.value() != null) {
          out.add(ConnectCluster.json(r.value()));
        }
      }
    }
    return out;
  }

  static List<JsonNode> ofType(List<JsonNode> ops, String type) {
    return ops.stream().filter(j -> type.equals(j.path("type").asText())).toList();
  }

  /** The task's trace when it is FAILED, else null. */
  static String failedTrace(ConnectCluster cluster, String name) throws Exception {
    JsonNode tasks = cluster.status(name).path("tasks");
    if (tasks.size() > 0 && "FAILED".equals(tasks.get(0).path("state").asText())) {
      return tasks.get(0).path("trace").asText();
    }
    return null;
  }

  /**
   * Restarts a FAILED task (after a fix or a framework failure) and waits until it is RUNNING. The
   * status read just after a restart request can still be the old FAILED one, and a request during
   * a rebalance is refused with 409, so the request is repeated while the task stays FAILED. Fails
   * with the last trace when it never runs.
   */
  static void restartUntilRunning(ConnectCluster cluster, String name, Duration timeout)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    long lastRequest = 0;
    String trace = null;
    while (System.currentTimeMillis() < deadline) {
      JsonNode st = cluster.status(name);
      JsonNode tasks = st.path("tasks");
      String task = tasks.size() > 0 ? tasks.get(0).path("state").asText() : "";
      if ("RUNNING".equals(st.path("connector").path("state").asText()) && "RUNNING".equals(task)) {
        return;
      }
      if ("FAILED".equals(task)) {
        trace = tasks.get(0).path("trace").asText();
        if (System.currentTimeMillis() - lastRequest > 15_000) {
          try {
            cluster.lifecycle(name, "restart");
          } catch (IllegalStateException e) {
            // a rebalance is in progress: ask again on the next round
          }
          lastRequest = System.currentTimeMillis();
        }
      }
      Thread.sleep(1000);
    }
    throw new AssertionError("task of " + name + " not RUNNING within " + timeout + ": " + trace);
  }

  /** The first CDC error code in a trace, or null when the failure is not one of ours. */
  static String cdcCode(String trace) {
    if (trace == null) {
      return null;
    }
    Matcher m = CDC_CODE.matcher(trace);
    return m.find() ? m.group() : null;
  }

  static String firstLine(String trace) {
    if (trace == null) {
      return null;
    }
    int nl = trace.indexOf('\n');
    String line = nl < 0 ? trace : trace.substring(0, nl);
    return line.length() > 500 ? line.substring(0, 500) : line;
  }

  /**
   * Watches the task for {@code window}. A task FAILED with one of our CDC codes is a product stop
   * and fails the suite. A task FAILED by the framework (a Kafka-side error, no CDC code) is
   * restarted as an operator or Strimzi's auto-restart would, recorded in the evidence, at most
   * {@code maxRestarts} times over the suite. Returns the restarts made in this window.
   */
  static int supervise(
      ConnectCluster cluster,
      String name,
      Duration window,
      Evidence ev,
      int[] restartsSoFar,
      int maxRestarts)
      throws Exception {
    long until = System.currentTimeMillis() + window.toMillis();
    int made = 0;
    while (System.currentTimeMillis() < until) {
      String trace = failedTrace(cluster, name);
      if (trace != null) {
        String code = cdcCode(trace);
        if (code != null) {
          throw new AssertionError("the task stopped with " + code + ": " + trace);
        }
        if (restartsSoFar[0] >= maxRestarts) {
          throw new AssertionError(
              "the task failed again after " + restartsSoFar[0] + " restarts: " + trace);
        }
        restartsSoFar[0]++;
        made++;
        ev.fault("task-restart", "cause", firstLine(trace));
        restartUntilRunning(cluster, name, Duration.ofMinutes(4));
      }
      Thread.sleep(1000);
    }
    return made;
  }

  static String header(ConsumerRecord<String, String> r, String name) {
    Header h = r.headers().lastHeader(name);
    return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }

  /**
   * A numeric field from a record in any of the converter's shapes: a JSON number, a string
   * (cdc.decimal.mode=string) or a variable-scale decimal object.
   */
  static BigDecimal number(JsonNode v) {
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    if (v.isNumber()) {
      return v.decimalValue();
    }
    if (v.isObject() && v.has("scale") && v.has("value")) {
      byte[] unscaled = Base64.getDecoder().decode(v.get("value").asText());
      return new BigDecimal(new BigInteger(unscaled), v.get("scale").asInt());
    }
    return new BigDecimal(v.asText());
  }

  /** The current SCN read on a connection the caller owns. */
  static long scn(Connection c) throws Exception {
    return sh.oso.connect.oracle.e2e.support.OracleSql.currentScn(c);
  }
}
