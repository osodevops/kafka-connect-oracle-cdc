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
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;

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
  final FakeCatalog catalog = new FakeCatalog().archivedRun(1, 1, 50, 1000, 100);
  long safeEnd = 1000;
  final Map<String, String> props = new HashMap<>(OracleCdcSourceConnectorConfigTest.minimal());
  private final Map<Map<String, Object>, Map<String, Object>> committedOffsets = new HashMap<>();
  private OracleCdcSourceTask task;
  int sessionsOpened;

  TaskHarness() {
    props.put(OracleCdcSourceConnectorConfig.POLL_LINGER_MS, "20");
  }

  SchemaRegistry registry() {
    InMemorySchemaStore store = new InMemorySchemaStore();
    store.save(
        new TableSchema(
            T,
            List.of(
                new ColumnSpec("ID", 1, OracleType.NUMBER, "NUMBER", 0, 18, 0, false),
                ColumnSpec.of("SQL", 2, OracleType.VARCHAR2)),
            List.of("ID"),
            KeySource.PRIMARY_KEY,
            true,
            false));
    DictionaryReader none =
        new DictionaryReader() {
          public java.util.Optional<TableSchema> read(TableId t) {
            return java.util.Optional.empty();
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            return new KeySelector.Candidates(List.of(), List.of());
          }
        };
    return new SchemaRegistry(
        store, none, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE));
  }

  final EngineFactory factory =
      cfg -> {
        sessionsOpened++;
        SchemaRegistry registry = registry();
        return new EngineFactory.Session() {
          public DatabaseIdentity identity() {
            return IDENTITY;
          }

          public long currentScn() {
            return 1000;
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

          public CaptureEngine engine(Position start, EventSink sink) {
            return new CaptureEngine(
                start,
                fake,
                new LogInventory(catalog, CoreConfig.CaptureMode.ONLINE, 1),
                () -> safeEnd,
                new HeapTransactionBuffer(),
                registry,
                DECODER,
                sink,
                new EngineSettings(
                    java.time.Duration.ofSeconds(2),
                    8,
                    java.time.Duration.ofHours(1),
                    java.time.Duration.ofMillis(10),
                    5,
                    CoreConfig.DecodeErrorAction.FAIL),
                new OraErrorClassifier(),
                Set.of("APP"),
                () -> {},
                Instant::now);
          }

          public void close() {}
        };
      };

  OracleCdcSourceTask start() {
    task = new OracleCdcSourceTask(factory);
    task.initialize(
        new SourceTaskContext() {
          public Map<String, String> configs() {
            return props;
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

  /** Polls until {@code n} records or the deadline; nothing is acknowledged yet. */
  List<SourceRecord> pollUntil(int n, long timeoutMillis) throws InterruptedException {
    List<SourceRecord> out = new ArrayList<>();
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (out.size() < n && System.currentTimeMillis() < deadline) {
      List<SourceRecord> batch = task.poll();
      if (batch != null) {
        out.addAll(batch);
      }
    }
    return out;
  }

  /** Acknowledges records in order like the framework: their offsets become committed. */
  @SuppressWarnings("unchecked")
  void acknowledge(List<SourceRecord> records) {
    for (SourceRecord r : records) {
      task.commitRecord(r, null);
      committedOffsets.put(
          (Map<String, Object>) r.sourcePartition(), (Map<String, Object>) r.sourceOffset());
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

  static String sql(SourceRecord r) {
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
