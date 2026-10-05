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
package sh.oso.connect.oracle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.ChangeDecoder;
import sh.oso.connect.oracle.core.engine.EngineSettings;
import sh.oso.connect.oracle.core.engine.EventSink;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.DictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;

/**
 * Drives {@link OracleCdcSourceTask} against the fake engine, simulating Connect's offset storage:
 * the offset of every polled record that the harness acknowledges becomes visible to a restarted
 * task, exactly as the framework commits offsets of acknowledged records.
 */
final class TaskHarness implements AutoCloseable {

  static final TableId T = FakeLogMiner.DEFAULT_TABLE;
  static final DatabaseIdentity IDENTITY = new DatabaseIdentity(77, 1);

  /** The fake's SQL text becomes the row's single column; a numeric ID is derived from it. */
  static final ChangeDecoder DECODER =
      (d, schema) -> {
        if ("bad".equals(d.sqlRedo())) {
          throw new sh.oso.connect.oracle.core.errors.DecodeException("bad row", "none");
        }
        Map<String, Object> after = new java.util.LinkedHashMap<>();
        after.put("ID", new java.math.BigDecimal(d.sqlRedo().hashCode() & 0x7fffffff));
        after.put("SQL", d.sqlRedo());
        return new RowChange(
            d.table(),
            d.op(),
            d.op() == sh.oso.connect.oracle.core.model.Operation.INSERT ? null : after,
            d.op() == sh.oso.connect.oracle.core.model.Operation.DELETE ? null : after,
            false,
            d.rowId(),
            d.id(),
            d.tx(),
            d.timestamp());
      };

  final FakeLogMiner fake = new FakeLogMiner().startAt(1000);

  /** With exactly-once on, the worker's transaction context records the commit requests. */
  boolean exactlyOnce;

  private volatile boolean commitRequested;

  final List<SourceRecord> kafkaCommits = java.util.Collections.synchronizedList(new ArrayList<>());
  final FakeCatalog catalog = new FakeCatalog().archivedRun(1, 1, 50, 1000, 100);
  long safeEnd = 1000;

  /** What the fake database reports as its current SCN when a task starts without an offset. */
  long currentScn = 1000;

  final Map<String, String> props = new HashMap<>(OracleCdcSourceConnectorConfigTest.minimal());
  private final Map<Map<String, Object>, Map<String, Object>> committedOffsets = new HashMap<>();
  private OracleCdcSourceTask task;
  int sessionsOpened;

  /** What a snapshot reads (PRD-02); the captured tables a snapshot covers. */
  final sh.oso.connect.oracle.core.testkit.FakeSnapshotSource snapshots =
      new sh.oso.connect.oracle.core.testkit.FakeSnapshotSource();

  volatile List<TableId> captured = List.of(T);

  /** Runs on the engine thread when the ids are refreshed (after a CREATE TABLE in the fake). */
  volatile Runnable onRefresh;

  /** What the session reports to the task when the captured set changes (SRC-SEL-4). */
  volatile java.util.function.BiConsumer<java.util.Set<TableId>, java.util.Set<TableId>>
      tablesListener;

  /** The signal topic as Kafka would hold it; offsets are list positions. */
  final List<sh.oso.connect.oracle.signals.SignalReader.RawSignal> signalTopic =
      java.util.Collections.synchronizedList(new ArrayList<>());

  /** Appends a signal for this connector (keyed by its topic prefix). */
  void signal(String json) {
    synchronized (signalTopic) {
      signalTopic.add(
          new sh.oso.connect.oracle.signals.SignalReader.RawSignal(
              signalTopic.size(), "cdc", json));
    }
  }

  /** Stands in for the Kafka admin client when cdc.kafka.bootstrap.servers is set. */
  final sh.oso.connect.oracle.topics.FakeTopicAdmin topicAdmin =
      new sh.oso.connect.oracle.topics.FakeTopicAdmin();

  /**
   * The journal topic as Kafka would hold it: acknowledged journal records, converted with the
   * worker's JsonConverter, served back to the task's loader on restart.
   */
  final List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> journalTopic =
      new ArrayList<>();

  /** The schema topic, held the same way. */
  final List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> schemaTopic =
      new ArrayList<>();

  private final org.apache.kafka.connect.json.JsonConverter journalKeys =
      new org.apache.kafka.connect.json.JsonConverter();
  private final org.apache.kafka.connect.json.JsonConverter journalValues =
      new org.apache.kafka.connect.json.JsonConverter();

  TaskHarness() {
    props.put(OracleCdcSourceConnectorConfig.POLL_LINGER_MS, "20");
    // streaming tests; snapshot tests set cdc.snapshot.mode and give the fake rows
    props.put(sh.oso.connect.oracle.core.config.CoreConfig.SNAPSHOT_MODE, "none");
    journalKeys.configure(Map.of("schemas.enable", "true"), true);
    journalValues.configure(Map.of("schemas.enable", "true"), false);
  }

  /** The test table as the data dictionary describes it; tests change it to model a DDL. */
  static TableSchema tableSchema(ColumnSpec... extra) {
    List<ColumnSpec> cols = new ArrayList<>();
    cols.add(new ColumnSpec("ID", 1, OracleType.NUMBER, "NUMBER", 0, 18, 0, false));
    cols.add(ColumnSpec.of("SQL", 2, OracleType.VARCHAR2));
    cols.addAll(List.of(extra));
    return new TableSchema(T, cols, List.of("ID"), KeySource.PRIMARY_KEY, true, false);
  }

  /** What the fake data dictionary holds; a table missing here reads as dropped. */
  final Map<TableId, TableSchema> dictionary =
      new java.util.concurrent.ConcurrentHashMap<>(Map.of(T, tableSchema()));

  /** The time the fake dictionary reports for every table's last DDL, and for every SCN. */
  java.time.Instant lastDdlTime;

  java.time.Instant scnTime;

  SchemaRegistry registry(sh.oso.connect.oracle.core.schema.SchemaStore store) {
    DictionaryReader fakeDictionary =
        new DictionaryReader() {
          public java.util.Optional<TableSchema> read(TableId t) {
            return java.util.Optional.ofNullable(dictionary.get(t));
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            return new KeySelector.Candidates(
                dictionary.containsKey(t) ? List.of("ID") : List.of(), List.of());
          }

          @Override
          public java.util.Optional<java.time.Instant> lastDdlTime(TableId t) {
            return java.util.Optional.ofNullable(lastDdlTime);
          }

          @Override
          public java.util.Optional<java.time.Instant> timeOfScn(long scn) {
            return java.util.Optional.ofNullable(scnTime);
          }
        };
    return new SchemaRegistry(
        store, fakeDictionary, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE));
  }

  final EngineFactory factory =
      (cfg, store) -> {
        sessionsOpened++;
        SchemaRegistry registry = registry(store);
        return new EngineFactory.Session() {
          public DatabaseIdentity identity() {
            return IDENTITY;
          }

          public long currentScn() {
            return currentScn;
          }

          public boolean cdb() {
            return true;
          }

          public String databaseName() {
            return "FREE";
          }

          public SchemaRegistry schemas() {
            return registry;
          }

          public List<TableId> capturedTables() {
            return captured;
          }

          public void onTablesChanged(
              java.util.function.BiConsumer<java.util.Set<TableId>, java.util.Set<TableId>> l) {
            tablesListener = l;
          }

          public sh.oso.connect.oracle.core.snapshot.SnapshotSource openSnapshotSource() {
            return snapshots;
          }

          public CaptureEngine engine(
              Position start, EventSink sink, sh.oso.connect.oracle.journal.BufferSetup setup) {
            HeapTransactionBuffer buffer =
                new HeapTransactionBuffer(
                    Long.MAX_VALUE,
                    null,
                    setup.policy(),
                    setup.journal(),
                    setup.generation(),
                    Instant::now);
            for (var chunks : setup.restored().values()) {
              buffer.restore(chunks);
            }
            return new CaptureEngine(
                start,
                fake,
                new LogInventory(catalog, CoreConfig.CaptureMode.ONLINE, 1),
                () -> safeEnd,
                buffer,
                registry,
                DECODER,
                sink,
                new EngineSettings(
                    java.time.Duration.ofSeconds(2),
                    8,
                    java.time.Duration.ofHours(1),
                    java.time.Duration.ofMillis(10),
                    5,
                    cfg.core().decodeErrorAction(),
                    EngineSettings.from(cfg.core()).transactionMaxAge(),
                    EngineSettings.from(cfg.core()).maxAgeAction()),
                new OraErrorClassifier(),
                Set.of("APP"),
                () -> {
                  if (onRefresh != null) {
                    onRefresh.run();
                  }
                  return Set.of("APP");
                },
                cause ->
                    new CaptureEngine.Sources(
                        fake,
                        new LogInventory(catalog, CoreConfig.CaptureMode.ONLINE, 1),
                        () -> safeEnd),
                Instant::now);
          }

          public void close() {}
        };
      };

  OracleCdcSourceTask start() {
    task =
        new OracleCdcSourceTask(factory) {
          @Override
          sh.oso.connect.oracle.topics.TopicAdmin topicAdmin(java.util.Properties clientProps) {
            topicAdmin.closed = false;
            return topicAdmin;
          }

          @Override
          sh.oso.connect.oracle.signals.SignalReader signalReader(
              java.util.Properties clientProps, String topic, long after) {
            long[] next = {after + 1};
            return () -> {
              List<sh.oso.connect.oracle.signals.SignalReader.RawSignal> out = new ArrayList<>();
              synchronized (signalTopic) {
                while (next[0] < signalTopic.size()) {
                  out.add(signalTopic.get((int) next[0]++));
                }
              }
              return out;
            };
          }

          @Override
          sh.oso.connect.oracle.journal.JournalReader journalReader(
              java.util.Properties clientProps) {
            return topic ->
                new ArrayList<>(topic.endsWith(".cdc.schema") ? schemaTopic : journalTopic);
          }
        };
    task.initialize(
        new SourceTaskContext() {
          public Map<String, String> configs() {
            return props;
          }

          public org.apache.kafka.connect.source.TransactionContext transactionContext() {
            if (!exactlyOnce) {
              return null;
            }
            return new org.apache.kafka.connect.source.TransactionContext() {
              public void commitTransaction() {
                commitRequested = true; // the batch poll() is returning ends the transaction
              }

              public void commitTransaction(SourceRecord record) {
                throw new AssertionError("the task commits per batch");
              }

              public void abortTransaction() {}

              public void abortTransaction(SourceRecord record) {}
            };
          }

          public OffsetStorageReader offsetStorageReader() {
            return new OffsetStorageReader() {
              @SuppressWarnings("unchecked")
              public <V> Map<String, Object> offset(Map<String, V> partition) {
                return committedOffsets.get((Map<String, Object>) partition);
              }

              @SuppressWarnings("unchecked")
              public <V> Map<Map<String, V>, Map<String, Object>> offsets(
                  Collection<Map<String, V>> partitions) {
                Map<Map<String, V>, Map<String, Object>> out = new HashMap<>();
                for (Map<String, V> p : partitions) {
                  out.put(p, committedOffsets.get((Map<String, Object>) p));
                }
                return out;
              }
            };
          }
        });
    task.start(props);
    return task;
  }

  /**
   * Polls until {@code n} change records or the deadline; heartbeats and ops events are dropped
   * from the result.
   */
  List<SourceRecord> pollUntil(int n, long timeoutMillis) throws InterruptedException {
    return pollUntil(n, timeoutMillis, false);
  }

  /** With {@code keepInternal} the heartbeat and ops records count and are returned too. */
  List<SourceRecord> pollUntil(int n, long timeoutMillis, boolean keepInternal)
      throws InterruptedException {
    List<SourceRecord> out = new ArrayList<>();
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (out.size() < n && System.currentTimeMillis() < deadline) {
      List<SourceRecord> batch = task.poll();
      if (commitRequested) {
        commitRequested = false;
        if (batch == null || batch.isEmpty()) {
          throw new AssertionError("a commit was requested for an empty batch");
        }
        kafkaCommits.add(batch.get(batch.size() - 1));
      }
      if (batch != null) {
        for (SourceRecord r : batch) {
          if (keepInternal || !isInternal(r)) {
            out.add(r);
          }
        }
      }
    }
    return out;
  }

  /** Acknowledges records in order like the framework: their offsets become committed. */
  @SuppressWarnings("unchecked")
  void acknowledge(List<SourceRecord> records) {
    for (SourceRecord r : records) {
      task.commitRecord(r, null);
      if (r.sourceOffset() != null) {
        committedOffsets.put(
            (Map<String, Object>) r.sourcePartition(), (Map<String, Object>) r.sourceOffset());
      }
      if (isJournal(r) || isSchema(r)) {
        List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> topic =
            isJournal(r) ? journalTopic : schemaTopic;
        byte[] k = journalKeys.fromConnectData(r.topic(), r.keySchema(), r.key());
        byte[] v =
            r.value() == null
                ? null
                : journalValues.fromConnectData(r.topic(), r.valueSchema(), r.value());
        topic.add(
            new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                r.topic(), 0, topic.size(), k, v));
      }
    }
  }

  Map<String, Object> committedOffset() {
    return committedOffsets.get(Map.of("server", "cdc"));
  }

  void restart() {
    task.stop();
    start();
  }

  OracleCdcSourceTask task() {
    return task;
  }

  static boolean isHeartbeat(SourceRecord r) {
    return r.topic().endsWith(".cdc.heartbeat");
  }

  static boolean isOps(SourceRecord r) {
    return r.topic().endsWith(".cdc.ops");
  }

  static boolean isJournal(SourceRecord r) {
    return r.topic().endsWith(".cdc.txjournal");
  }

  static boolean isSchema(SourceRecord r) {
    return r.topic().endsWith(".cdc.schema");
  }

  static boolean isDlq(SourceRecord r) {
    return r.topic().endsWith(".cdc.dlq");
  }

  static boolean isSnapshot(SourceRecord r) {
    return r.value() instanceof org.apache.kafka.connect.data.Struct v
        && v.schema().field("op") != null
        && "r".equals(v.getString("op"));
  }

  static boolean isInternal(SourceRecord r) {
    return isHeartbeat(r) || isOps(r) || isJournal(r) || isDlq(r) || isSchema(r);
  }

  static String opsType(SourceRecord r) {
    return ((org.apache.kafka.connect.data.Struct) r.value()).getString("type");
  }

  static Map<String, String> opsDetails(SourceRecord r) {
    Map<?, ?> raw = ((org.apache.kafka.connect.data.Struct) r.value()).getMap("details");
    Map<String, String> out = new java.util.LinkedHashMap<>();
    raw.forEach((k, v) -> out.put(String.valueOf(k), String.valueOf(v)));
    return out;
  }

  static String sql(SourceRecord r) {
    if (isHeartbeat(r)) {
      return "<heartbeat>";
    }
    if (isOps(r)) {
      return "<ops:" + opsType(r) + ">";
    }
    if (isJournal(r)) {
      org.apache.kafka.connect.data.Struct k = (org.apache.kafka.connect.data.Struct) r.key();
      return (r.value() == null ? "<tombstone:" : "<chunk:") + k.getInt32("chunk") + ">";
    }
    if (isSchema(r)) {
      org.apache.kafka.connect.data.Struct k = (org.apache.kafka.connect.data.Struct) r.key();
      return (r.value() == null ? "<schema-removed:" : "<schema:") + k.getString("table") + ">";
    }
    if (isDlq(r)) {
      return "<dlq:" + ((org.apache.kafka.connect.data.Struct) r.value()).getString("kind") + ">";
    }
    org.apache.kafka.connect.data.Struct v = (org.apache.kafka.connect.data.Struct) r.value();
    if (v == null) {
      return "<tombstone>";
    }
    org.apache.kafka.connect.data.Struct img =
        v.getStruct("after") != null ? v.getStruct("after") : v.getStruct("before");
    return v.getString("op") + ":" + img.getString("SQL");
  }

  static List<String> sqls(List<SourceRecord> records) {
    List<String> out = new ArrayList<>();
    for (SourceRecord r : records) {
      out.add(sql(r));
    }
    return out;
  }

  MiningEvent lastEvent() {
    return fake.events().get(fake.events().size() - 1);
  }

  @Override
  public void close() {
    if (task != null) {
      task.stop();
    }
  }
}
