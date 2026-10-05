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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.EngineLifecycle;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.errors.TopologyException;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.delivery.AckTracker;
import sh.oso.connect.oracle.delivery.RecordQueueSink;
import sh.oso.connect.oracle.envelope.DebeziumEnvelope;
import sh.oso.connect.oracle.envelope.TopicRouter;

/**
 * The single capture task (SRC-LC-2). start() loads the position from the offset store or takes the
 * current SCN, verifies the database identity, builds the engine and runs it on its own thread;
 * poll() drains the record queue and rethrows the engine's first failure; stop() ends the engine
 * within the shutdown timeout and closes the sessions (SRC-LC-4).
 */
public class OracleCdcSourceTask extends SourceTask {

  private static final Logger LOG = LoggerFactory.getLogger(OracleCdcSourceTask.class);

  private final EngineFactory factory;
  private OracleCdcSourceConnectorConfig config;
  private EngineFactory.Session session;
  private RecordQueueSink sink;
  private EngineLifecycle lifecycle;
  // the task thread starts and stops it; signals on the engine thread replace it
  private volatile sh.oso.connect.oracle.core.snapshot.SnapshotCoordinator snapshot;
  private Thread snapshotOnly;
  private CaptureEngine engine;
  private sh.oso.connect.oracle.signals.SignalReader signals;
  private long lastSignalPoll;

  /** SRC-SEL-4: tables that joined the captured set and still need their snapshot. */
  private final java.util.Set<sh.oso.connect.oracle.core.model.TableId> pendingNewTables =
      java.util.concurrent.ConcurrentHashMap.newKeySet();

  private volatile Throwable snapshotOnlyFailure;
  private sh.oso.connect.oracle.schema.SchemaTopicStore schemaStore;
  private javax.management.ObjectName metricsName;
  private org.apache.kafka.connect.source.TransactionContext transactions;
  private sh.oso.connect.oracle.delivery.EosBoundaries boundaries;
  private final AckTracker acks = new AckTracker();
  private volatile Position startPosition;

  public OracleCdcSourceTask() {
    this(new JdbcEngineFactory());
  }

  OracleCdcSourceTask(EngineFactory factory) {
    this.factory = factory;
  }

  @Override
  public String version() {
    return Version.VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    config = new OracleCdcSourceConnectorConfig(props);
    try {
      // PRD-03: versions live in the schema topic. Only the position this start read back is
      // known to be committed (Connect flushes offsets after acknowledging records), so versions
      // are pruned below that one and no further
      schemaStore =
          new sh.oso.connect.oracle.schema.SchemaTopicStore(
              () -> startPosition == null ? 0 : startPosition.resumeScn());
      session = factory.open(config, schemaStore);
      DatabaseIdentity identity = session.identity();
      Map<String, Object> partition = Map.of("server", config.topicPrefix());
      Map<String, Object> stored =
          context == null || context.offsetStorageReader() == null
              ? null
              : context.offsetStorageReader().offset(partition);
      Position position;
      if (stored == null || stored.isEmpty()) {
        long current = session.currentScn();
        Long configured = config.core().startScn();
        if (configured != null && configured > current) {
          throw new TopologyException(
              "cdc.start.scn " + configured + " is ahead of the database's current SCN " + current,
              "Set cdc.start.scn to an SCN the database has reached, or leave it empty.");
        }
        position = Position.initial(configured != null ? configured : current, identity);
        LOG.info(
            "No stored offset; starting at SCN {}{}",
            position.resumeScn(),
            configured != null ? " (cdc.start.scn)" : "");
      } else {
        position = PositionCodec.read(stored);
        if (!position.identity().equals(identity)) {
          throw new TopologyException(
              "The stored offset belongs to database "
                  + position.identity()
                  + " but the connector"
                  + " is connected to "
                  + identity,
              "Point the connector at the original database or reset its offsets with"
                  + " oracle-cdc-admin after a resnapshot.");
        }
        LOG.info(
            "Resuming from SCN {} after commit {} (event index {})",
            position.resumeScn(),
            position.lastCommitKey(),
            position.eventIndex());
      }
      // ADR-0003: every chunk this start writes carries a new generation; chunks of a newer
      // generation than a committed position are unacknowledged writes
      long generation = position.journalGeneration() + 1;
      position = position.withJournalGeneration(generation);
      // PRD-02 SNAP-1: an initial snapshot is recorded in the very first offset, so a crash before
      // its first chunk still resumes it
      CoreConfig.SnapshotMode snapshotMode = config.core().snapshotMode();
      sh.oso.connect.oracle.core.snapshot.SnapshotProgress progress =
          sh.oso.connect.oracle.core.snapshot.SnapshotProgress.of(position.snapshot());
      if (progress == null
          && (stored == null || stored.isEmpty())
          && (snapshotMode == CoreConfig.SnapshotMode.INITIAL
              || snapshotMode == CoreConfig.SnapshotMode.SNAPSHOT_ONLY)) {
        progress = sh.oso.connect.oracle.core.snapshot.SnapshotProgress.begin();
        position = position.withSnapshot(progress.toMap());
      }
      startPosition = position;
      TopicRouter router =
          new TopicRouter(
              config.topicTemplate(session.cdb()), config.topicPrefix(), session.databaseName());
      DebeziumEnvelope envelope = new DebeziumEnvelope(config, router, session.databaseName());
      sink =
          new RecordQueueSink(
              envelope,
              session.schemas(),
              position,
              Math.max(1000, config.pollMaxRecords() * 4),
              new sh.oso.connect.oracle.heartbeat.HeartbeatEmitter(
                  config.heartbeatTopic(), config.topicPrefix(), envelope.partition()),
              new sh.oso.connect.oracle.ops.OpsEventWriter(
                  config.opsTopic(), config.topicPrefix(), envelope.partition()),
              new sh.oso.connect.oracle.journal.JournalRecords(
                  config.journalTopic(), config.topicPrefix(), envelope.partition()),
              new sh.oso.connect.oracle.dlq.DecodeDlqWriter(
                  config.dlqTopic(), config.topicPrefix(), envelope.partition()),
              config.heartbeatIntervalMs(),
              System::currentTimeMillis);
      sink.schemaTopic(
          new sh.oso.connect.oracle.schema.SchemaRecords.Writer(
              config.schemaTopic(), config.topicPrefix(), envelope.partition()));
      if (config.kafkaBootstrapServers() != null) {
        RecordQueueSink queue = sink;
        schemaStore.writeTo(
            new sh.oso.connect.oracle.schema.SchemaTopicStore.Writer() {
              public void versions(
                  sh.oso.connect.oracle.core.model.TableId t,
                  List<sh.oso.connect.oracle.core.schema.TableSchema> v) {
                queue.schemaVersions(t, v);
              }

              public void removed(sh.oso.connect.oracle.core.model.TableId t) {
                queue.schemaRemoved(t);
              }
            });
      }
      // SRC-EOS: the worker hands out a transaction context only with
      // transaction.boundary=connector
      transactions = context.transactionContext();
      if (transactions != null) {
        sink.exactlyOnce(config.eosSplitMaxRecords(), config.eosSplitMaxBytes());
        boundaries =
            new sh.oso.connect.oracle.delivery.EosBoundaries(
                config.eosBatchMaxRecords(), config.eosBatchMaxBytes(), config.eosBatchMaxMs());
        LOG.info("Exactly-once: Kafka transactions end only at Oracle commit boundaries");
      }
      ensureInternalTopics();
      sh.oso.connect.oracle.journal.BufferSetup bufferSetup = loadJournal(position, generation);
      loadSchemas(position);
      // SRC-HB-1: the start position becomes durable with the first offset flush, before any
      // change record; a task killed before that would otherwise restart from a later SCN
      sink.heartbeatAtStart();
      sink.ops(
          sh.oso.connect.oracle.ops.OpsEvent.Type.STARTUP,
          "resume_scn",
          Long.toString(position.resumeScn()),
          "last_commit",
          position.lastCommitKey() == null ? null : position.lastCommitKey().toString(),
          "database",
          session.databaseName(),
          "version",
          Version.VERSION);
      CaptureEngine engine = session.engine(position, sink, bufferSetup);
      RecordQueueSink opsSink = sink;
      session.startDictionaryBuilds(
          new sh.oso.connect.oracle.core.logs.DictionaryBuildScheduler.Events() {
            public void built(Duration took) {
              opsSink.ops(
                  sh.oso.connect.oracle.ops.OpsEvent.Type.DICTIONARY_BUILD,
                  "status",
                  "built",
                  "millis",
                  Long.toString(took.toMillis()));
            }

            public void failed(String message) {
              opsSink.ops(
                  sh.oso.connect.oracle.ops.OpsEvent.Type.DICTIONARY_BUILD,
                  "status",
                  "failed",
                  "message",
                  message);
            }

            public void disabled(String message) {
              opsSink.ops(
                  sh.oso.connect.oracle.ops.OpsEvent.Type.DICTIONARY_BUILD,
                  "status",
                  "disabled",
                  "message",
                  message);
            }
          });
      this.engine = engine;
      if (progress != null && !progress.complete()) {
        startSnapshot(progress, engine);
      }
      Object pending = position.extras().get(RecordQueueSink.SNAPSHOT_PENDING);
      if (pending != null && !pending.toString().isBlank()) {
        for (String fqn : pending.toString().split(",")) {
          pendingNewTables.add(tableId(fqn.trim()));
        }
        startPendingSnapshot();
      }
      session.onTablesChanged(this::tablesChanged);
      if (config.kafkaBootstrapServers() != null
          && snapshotMode != CoreConfig.SnapshotMode.SNAPSHOT_ONLY) {
        java.util.Properties p = config.kafkaClientProperties();
        p.put("bootstrap.servers", config.kafkaBootstrapServers());
        Object last = position.extras().get(RecordQueueSink.SIGNAL_OFFSET);
        signals =
            signalReader(p, config.signalsTopic(), last instanceof Number n ? n.longValue() : -1);
      }
      if (snapshotMode == CoreConfig.SnapshotMode.SNAPSHOT_ONLY) {
        startSnapshotOnly();
        registerMetrics(engine);
        return;
      }
      lifecycle =
          new EngineLifecycle(
              engine,
              Duration.ofMillis(Math.max(100, config.pollLingerMs() * 2)),
              t -> {
                LOG.error("Capture engine stopped: {}", t.getMessage(), t);
                try {
                  sink.stopped(t);
                } catch (RuntimeException ignore) {
                  // the queue may be full or closed; the log line above is the record of last
                  // resort
                }
              });
      lifecycle.start("oracle-cdc-engine-" + config.topicPrefix());
      registerMetrics(engine);
    } catch (OracleCdcException e) {
      closeQuietly();
      throw new ConnectException(e.getMessage(), e);
    } catch (Exception e) {
      closeQuietly();
      throw new ConnectException("Starting the capture task failed: " + e.getMessage(), e);
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    pollSignals();
    Throwable failure = lifecycle != null ? lifecycle.failure() : snapshotOnlyFailure;
    List<SourceRecord> records =
        transactions == null
            ? sink.drain(config.pollMaxRecords(), config.pollLingerMs())
            : boundaries.next(
                boundaries.holding()
                    ? List.of()
                    : sink.drainQueued(config.pollMaxRecords(), config.pollLingerMs()),
                transactions,
                System.currentTimeMillis(),
                sink.queued() == 0);
    if (!records.isEmpty()) {
      return records;
    }
    if (failure != null) {
      throw new ConnectException(failure.getMessage(), failure);
    }
    return null;
  }

  @Override
  public void commitRecord(SourceRecord record, RecordMetadata metadata) {
    acks.acknowledged(record);
  }

  @Override
  public void stop() {
    if (signals != null) {
      try {
        signals.close();
      } catch (Exception e) {
        LOG.debug("closing the signal reader: {}", e.getMessage());
      }
      signals = null;
    }
    if (snapshot != null) {
      snapshot.close();
    }
    if (snapshotOnly != null) {
      snapshotOnly.interrupt();
    }
    if (lifecycle != null) {
      try {
        if (!lifecycle.stop(Duration.ofMillis(config.shutdownTimeoutMs()))) {
          LOG.warn("Capture engine did not stop within {} ms", config.shutdownTimeoutMs());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    closeQuietly();
  }

  /**
   * CORE-TX-5: reload journaled transactions before mining resumes. Without broker access the
   * journal cannot be read back, so it is not written either and long transactions pin the position
   * as before.
   */
  /**
   * PRD-03: versions are read back from the schema topic and checked against the dictionary
   * (SCH-6); from here on every change is written to the topic. Without broker access the topic is
   * not read, versions start from the dictionary, and nothing is written either.
   */
  private void loadSchemas(Position position) throws java.sql.SQLException {
    if (config.kafkaBootstrapServers() == null) {
      return;
    }
    java.util.Properties p = config.kafkaClientProperties();
    p.put("bootstrap.servers", config.kafkaBootstrapServers());
    Map<
            sh.oso.connect.oracle.core.model.TableId,
            List<sh.oso.connect.oracle.core.schema.TableSchema>>
        latest = new java.util.LinkedHashMap<>();
    try (sh.oso.connect.oracle.journal.JournalReader reader = journalReader(p)) {
      org.apache.kafka.connect.storage.Converter keys = converter(true);
      org.apache.kafka.connect.storage.Converter values = converter(false);
      String topic = config.schemaTopic();
      for (org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> r :
          reader.readAll(topic)) {
        sh.oso.connect.oracle.core.model.TableId t =
            sh.oso.connect.oracle.schema.SchemaRecords.table(
                keys.toConnectData(topic, r.key()).value(), config.topicPrefix());
        if (t != null) {
          Object value = r.value() == null ? null : values.toConnectData(topic, r.value()).value();
          latest.put(t, sh.oso.connect.oracle.schema.SchemaRecords.versions(t, value));
        }
      }
    } catch (OracleCdcException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectException("Reading the schema topic failed: " + e.getMessage(), e);
    }
    latest.forEach(schemaStore::seed);
    for (sh.oso.connect.oracle.core.model.TableId t : schemaStore.tables()) {
      session.schemas().validate(t, position.resumeScn());
    }
    LOG.info("Schema topic {}: {} tables", config.schemaTopic(), schemaStore.tables().size());
  }

  private sh.oso.connect.oracle.journal.BufferSetup loadJournal(
      Position position, long generation) {
    sh.oso.connect.oracle.core.buffer.JournalPolicy policy = config.journalPolicy();
    if (!policy.enabled()) {
      return sh.oso.connect.oracle.journal.BufferSetup.none();
    }
    if (config.kafkaBootstrapServers() == null) {
      LOG.warn(
          "cdc.kafka.bootstrap.servers not set: the transaction journal is disabled and long"
              + " transactions pin the resume position (CORE-TX-4)");
      return sh.oso.connect.oracle.journal.BufferSetup.none();
    }
    java.util.Properties p = config.kafkaClientProperties();
    p.put("bootstrap.servers", config.kafkaBootstrapServers());
    sh.oso.connect.oracle.journal.JournalTopicLoader.Loaded loaded;
    LOG.info(
        "Reading the transaction journal {} for generation {}", config.journalTopic(), generation);
    try (sh.oso.connect.oracle.journal.JournalReader reader = journalReader(p)) {
      org.apache.kafka.connect.storage.Converter keys = converter(true);
      org.apache.kafka.connect.storage.Converter values = converter(false);
      loaded =
          new sh.oso.connect.oracle.journal.JournalTopicLoader(keys, values, config.topicPrefix())
              .load(reader.readAll(config.journalTopic()), position);
    } catch (OracleCdcException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectException("Reading the transaction journal failed: " + e.getMessage(), e);
    }
    for (sh.oso.connect.oracle.journal.JournalRecords.ChunkKey k : loaded.stale()) {
      sink.tombstone(k);
    }
    if (!loaded.restore().isEmpty() || !loaded.stale().isEmpty()) {
      LOG.info(
          "Journal: restoring {} transactions ({} chunks), tombstoning {} stale chunks",
          loaded.restore().size(),
          loaded.chunks(),
          loaded.stale().size());
    }
    return new sh.oso.connect.oracle.journal.BufferSetup(
        policy, sink, generation, loaded.restore());
  }

  private org.apache.kafka.connect.storage.Converter converter(boolean isKey) {
    try {
      org.apache.kafka.connect.storage.Converter c =
          (org.apache.kafka.connect.storage.Converter)
              Class.forName(config.journalConverter()).getDeclaredConstructor().newInstance();
      c.configure(config.journalConverterProperties(), isKey);
      return c;
    } catch (ReflectiveOperationException e) {
      throw new ConnectException(
          "cdc.journal.converter " + config.journalConverter() + " cannot be instantiated", e);
    }
  }

  /** Overridable for tests. */
  sh.oso.connect.oracle.journal.JournalReader journalReader(java.util.Properties clientProps) {
    return new sh.oso.connect.oracle.journal.KafkaJournalReader(clientProps);
  }

  /** SRC-TOP-6: create the internal topics when broker access is configured. */
  private void ensureInternalTopics() {
    if (config.kafkaBootstrapServers() == null) {
      LOG.info(
          "cdc.kafka.bootstrap.servers not set; internal topics rely on worker topic creation");
      return;
    }
    java.util.Properties p = config.kafkaClientProperties();
    p.put("bootstrap.servers", config.kafkaBootstrapServers());
    try (sh.oso.connect.oracle.topics.TopicAdmin admin = topicAdmin(p)) {
      java.util.List<String> created =
          new sh.oso.connect.oracle.topics.InternalTopicManager(
                  admin, config.internalTopicReplication())
              .ensure(sh.oso.connect.oracle.topics.InternalTopics.of(config));
      if (!created.isEmpty()) {
        LOG.info("Created internal topics {}", created);
      } else {
        LOG.info("Internal topics present");
      }
    } catch (Exception e) {
      throw new ConnectException("Creating the internal topics failed: " + e.getMessage(), e);
    }
  }

  /**
   * PRD-02: reads the tables the progress has not finished, in cdc.snapshot.tables.order then by
   * name. Their versions are loaded here, on the task thread, so the reader threads only ever use
   * the registry's cache.
   */
  private void startSnapshot(
      sh.oso.connect.oracle.core.snapshot.SnapshotProgress progress, CaptureEngine engine) {
    List<sh.oso.connect.oracle.core.model.TableId> captured =
        new java.util.ArrayList<>(session.capturedTables());
    captured.sort(java.util.Comparator.comparing(sh.oso.connect.oracle.core.model.TableId::fqn));
    List<sh.oso.connect.oracle.core.model.TableId> ordered = new java.util.ArrayList<>();
    for (String first : config.core().getList(CoreConfig.SNAPSHOT_TABLES_ORDER)) {
      for (sh.oso.connect.oracle.core.model.TableId t : captured) {
        if (t.fqnUpper().equals(first.trim().toUpperCase(java.util.Locale.ROOT))
            && !ordered.contains(t)) {
          ordered.add(t);
        }
      }
    }
    for (sh.oso.connect.oracle.core.model.TableId t : captured) {
      if (!ordered.contains(t)) {
        ordered.add(t);
      }
    }
    ordered.removeIf(t -> !progress.covers(t)); // a snapshot by signal reads its tables only
    Map<sh.oso.connect.oracle.core.model.TableId, String> overrides = new java.util.HashMap<>();
    Map<String, String> wanted = config.core().snapshotSelectOverrides();
    for (sh.oso.connect.oracle.core.model.TableId t : ordered) {
      String configured = wanted.get(t.fqnUpper());
      String signalled = progress.where();
      if (configured != null || signalled != null) {
        overrides.put(
            t,
            configured == null
                ? signalled
                : signalled == null ? configured : configured + ") AND (" + signalled);
      }
      if (!progress.done(t)) {
        try {
          session.schemas().current(t);
        } catch (java.sql.SQLException | OracleCdcException e) {
          LOG.warn("Snapshot: no schema for {} ({}); it is skipped", t.fqn(), e.getMessage());
        }
      }
    }
    if (snapshot != null) {
      snapshot.close(); // a finished or stopped one
    }
    snapshot =
        new sh.oso.connect.oracle.core.snapshot.SnapshotCoordinator(
            ordered,
            progress,
            t -> session.schemas().cached(t),
            session::openSnapshotSource,
            new sh.oso.connect.oracle.core.snapshot.SnapshotCoordinator.Settings(
                config.core().getInt(CoreConfig.SNAPSHOT_THREADS),
                config.core().getInt(CoreConfig.SNAPSHOT_CHUNK_ROWS),
                config.core().getInt(CoreConfig.SNAPSHOT_CHUNK_RETRIES),
                config.core().getInt(CoreConfig.SNAPSHOT_MAX_PENDING_CHUNKS),
                overrides,
                config.core().decodeErrorAction() == CoreConfig.DecodeErrorAction.DLQ),
            engine.metrics(),
            new sh.oso.connect.oracle.core.errors.OraErrorClassifier(
                java.util.Set.copyOf(config.core().extraRetryErrorCodes())));
    sink.snapshot(snapshot, progress);
    snapshot.start();
    LOG.info(
        "Snapshot of {} tables{}",
        snapshot.tables().size(),
        progress.untouched() ? "" : ", resumed from the stored offset");
  }

  /** Overridable for tests. */
  sh.oso.connect.oracle.signals.SignalReader signalReader(
      java.util.Properties clientProps, String topic, long after) {
    return new sh.oso.connect.oracle.signals.KafkaSignalReader(clientProps, topic, after);
  }

  /**
   * SRC-SIG-1: reads new signals at most once a second and hands each to the engine thread, which
   * may use the metadata connection and the buffer.
   */
  private void pollSignals() {
    if (engine == null || lifecycle == null) {
      return;
    }
    long now = System.currentTimeMillis();
    if (now - lastSignalPoll < 1000) {
      return;
    }
    lastSignalPoll = now;
    if (!pendingNewTables.isEmpty()) {
      engine.submit(this::startPendingSnapshot); // once the running snapshot has finished
    }
    if (signals == null) {
      return;
    }
    List<sh.oso.connect.oracle.signals.SignalReader.RawSignal> got;
    try {
      got = signals.poll();
    } catch (RuntimeException e) {
      LOG.warn("Reading the signal topic failed: {}", e.getMessage());
      return;
    }
    for (sh.oso.connect.oracle.signals.SignalReader.RawSignal r : got) {
      engine.submit(() -> handleSignal(r));
    }
  }

  /**
   * SRC-SEL-4, on the engine thread: a refresh of the object ids changed the captured set, after a
   * CREATE TABLE, DROP TABLE or RENAME, or a refresh-tables signal. A new table may already hold
   * rows (CREATE TABLE AS SELECT, a rename into the include pattern), so under
   * cdc.snapshot.mode=initial it is snapshotted while streaming continues; the offsets record it as
   * pending until its snapshot has started.
   */
  private void tablesChanged(
      java.util.Set<sh.oso.connect.oracle.core.model.TableId> added,
      java.util.Set<sh.oso.connect.oracle.core.model.TableId> removed) {
    if (config.core().snapshotMode() == CoreConfig.SnapshotMode.INITIAL) {
      sh.oso.connect.oracle.core.mining.event.MiningEvent.Ddl cause = engine.refreshCause();
      for (sh.oso.connect.oracle.core.model.TableId t : added) {
        if (!createdEmpty(cause, t)) {
          pendingNewTables.add(t);
        }
      }
      pendingNewTables.removeAll(removed);
      sink.pendingSnapshot(pendingNewTables);
    }
    for (sh.oso.connect.oracle.core.model.TableId t : removed) {
      sink.ops(sh.oso.connect.oracle.ops.OpsEvent.Type.TABLE_REMOVED, "table", t.fqn());
    }
    for (sh.oso.connect.oracle.core.model.TableId t : added) {
      sink.ops(
          sh.oso.connect.oracle.ops.OpsEvent.Type.TABLE_ADDED,
          "table",
          t.fqn(),
          "snapshot",
          Boolean.toString(pendingNewTables.contains(t)));
    }
    startPendingSnapshot();
  }

  /**
   * A plain CREATE TABLE of {@code t} (not CREATE TABLE ... AS SELECT) made the table empty, and
   * streaming has captured it since that DDL: nothing to snapshot.
   */
  static boolean createdEmpty(
      sh.oso.connect.oracle.core.mining.event.MiningEvent.Ddl cause,
      sh.oso.connect.oracle.core.model.TableId t) {
    if (cause == null
        || cause.sql() == null
        || !t.table().equalsIgnoreCase(cause.objectName())
        || !t.schema().equalsIgnoreCase(cause.owner())) {
      return false;
    }
    String sql = cause.sql();
    return sql.matches("(?is)\\s*CREATE\\s+(GLOBAL\\s+TEMPORARY\\s+)?TABLE\\b.*")
        && !sql.matches("(?is).*\\bAS\\s*\\(?\\s*(SELECT|WITH)\\b.*");
  }

  /** Starts a snapshot of the pending new tables unless another snapshot is running. */
  private void startPendingSnapshot() {
    if (pendingNewTables.isEmpty() || sink.snapshotRunning()) {
      return;
    }
    List<sh.oso.connect.oracle.core.model.TableId> tables =
        new java.util.ArrayList<>(pendingNewTables);
    startSnapshot(
        sh.oso.connect.oracle.core.snapshot.SnapshotProgress.scoped(tables, null), engine);
    pendingNewTables.removeAll(tables);
    sink.pendingSnapshot(pendingNewTables);
  }

  private static sh.oso.connect.oracle.core.model.TableId tableId(String fqn) {
    String[] p = fqn.split("\\.");
    return p.length == 3
        ? new sh.oso.connect.oracle.core.model.TableId(p[0], p[1], p[2])
        : new sh.oso.connect.oracle.core.model.TableId(null, p[0], p[1]);
  }

  /** On the engine thread: one signal, ending in a signal-ack ops event (SRC-SIG-3). */
  private void handleSignal(sh.oso.connect.oracle.signals.SignalReader.RawSignal r) {
    sink.signalProcessed(r.offset());
    String name = config.originalsStrings().getOrDefault("name", config.topicPrefix());
    if (r.key() == null || !(r.key().equals(name) || r.key().equals(config.topicPrefix()))) {
      return; // another connector's signal
    }
    sh.oso.connect.oracle.signals.Signal s;
    try {
      s = sh.oso.connect.oracle.signals.Signal.parse(r.value());
    } catch (IllegalArgumentException e) {
      ack(null, null, "invalid", e.getMessage());
      return;
    }
    try {
      switch (s.type()) {
        case "snapshot" -> signalSnapshot(s);
        case "snapshot-pause", "snapshot-resume" -> {
          if (sink.snapshotRunning() && snapshot != null) {
            snapshot.pause(s.type().equals("snapshot-pause"));
            ack(s, "ok", null);
          } else {
            ack(s, "rejected", "no snapshot is running");
          }
        }
        case "snapshot-stop" -> {
          if (sink.snapshotRunning() && snapshot != null) {
            snapshot.close();
            snapshot = null;
            sink.snapshotStopped();
            ack(s, "ok", null);
          } else {
            ack(s, "rejected", "no snapshot is running");
          }
        }
        case "refresh-tables" -> {
          engine.requestRefresh();
          ack(s, "ok", null);
        }
        case "log-state" -> logState(s);
        default -> ack(s, "unknown", "unknown signal type " + s.type());
      }
    } catch (RuntimeException e) {
      LOG.warn("Signal {} failed: {}", s.type(), e.getMessage(), e);
      ack(s, "failed", e.getMessage());
    }
  }

  private void signalSnapshot(sh.oso.connect.oracle.signals.Signal s) {
    if (sink.snapshotRunning()) {
      ack(s, "rejected", "a snapshot is running");
      return;
    }
    List<sh.oso.connect.oracle.core.model.TableId> tables = new java.util.ArrayList<>();
    List<String> unknown = new java.util.ArrayList<>();
    for (String wanted : s.tables()) {
      sh.oso.connect.oracle.core.model.TableId match = null;
      for (sh.oso.connect.oracle.core.model.TableId t : session.capturedTables()) {
        if (t.fqnUpper().equals(wanted.trim().toUpperCase(java.util.Locale.ROOT))) {
          match = t;
        }
      }
      if (match == null) {
        unknown.add(wanted);
      } else {
        tables.add(match);
      }
    }
    if (tables.isEmpty()) {
      ack(s, "rejected", "none of " + s.tables() + " is a captured table");
      return;
    }
    startSnapshot(
        sh.oso.connect.oracle.core.snapshot.SnapshotProgress.scoped(tables, s.predicate()), engine);
    ack(s, "ok", unknown.isEmpty() ? null : "not captured, skipped: " + unknown);
  }

  /** SRC-SIG-1 log-state: the buffer and the position, on the ops topic. */
  private void logState(sh.oso.connect.oracle.signals.Signal s) {
    sh.oso.connect.oracle.core.buffer.BufferMetricsSnapshot b = engine.metrics().buffer;
    StringBuilder largest = new StringBuilder();
    List<sh.oso.connect.oracle.core.buffer.TransactionBuffer.OpenTransaction> top =
        engine.metrics().largest;
    if (top != null) {
      for (sh.oso.connect.oracle.core.buffer.TransactionBuffer.OpenTransaction t : top) {
        largest.append(largest.length() == 0 ? "" : ",").append(t.key()).append(':');
        largest.append(t.firstScn());
      }
    }
    sink.ops(
        sh.oso.connect.oracle.ops.OpsEvent.Type.SIGNAL_ACK,
        "id",
        s.id(),
        "type",
        s.type(),
        "outcome",
        "ok",
        "mined_to_scn",
        Long.toString(engine.cursor().scn()),
        "open_transactions",
        b == null ? "0" : Integer.toString(b.openTransactions()),
        "buffered_events",
        b == null ? "0" : Long.toString(b.bufferedEvents()),
        "oldest_open_scn",
        b == null ? "-1" : Long.toString(b.oldestOpenScn()),
        "largest",
        largest.toString(),
        "snapshot_running",
        Boolean.toString(sink.snapshotRunning()));
  }

  private void ack(sh.oso.connect.oracle.signals.Signal s, String outcome, String message) {
    ack(s == null ? null : s.id(), s == null ? null : s.type(), outcome, message);
  }

  private void ack(String id, String type, String outcome, String message) {
    sink.ops(
        sh.oso.connect.oracle.ops.OpsEvent.Type.SIGNAL_ACK,
        "id",
        id,
        "type",
        type,
        "outcome",
        outcome,
        "message",
        message);
  }

  /** SNAP-1 snapshot_only: no streaming; chunks are published as soon as they are read. */
  private void startSnapshotOnly() {
    snapshotOnly =
        new Thread(
            () -> {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  if (sink.publishSnapshot()) {
                    LOG.info(
                        "Snapshot complete; cdc.snapshot.mode=snapshot_only, so the task idles");
                    return;
                  }
                  Thread.sleep(100);
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (RuntimeException e) {
                LOG.error("Snapshot stopped: {}", e.getMessage(), e);
                snapshotOnlyFailure = e;
                try {
                  sink.stopped(e);
                } catch (RuntimeException ignore) {
                  // the log line above is the record of last resort
                }
              }
            },
            "oracle-cdc-snapshot-only-" + config.topicPrefix());
    snapshotOnly.setDaemon(true);
    snapshotOnly.start();
  }

  /** Overridable for tests. */
  sh.oso.connect.oracle.topics.TopicAdmin topicAdmin(java.util.Properties clientProps) {
    return new sh.oso.connect.oracle.topics.KafkaTopicAdmin(clientProps);
  }

  /** ADR-0013: one MXBean per task; a failure to register is logged, never fatal. */
  private void registerMetrics(CaptureEngine engine) {
    try {
      metricsName =
          new sh.oso.connect.oracle.core.metrics.TaskMetrics(
                  engine.metrics(),
                  sink,
                  config
                      .core()
                      .getLong(
                          sh.oso.connect.oracle.core.config.CoreConfig.BUFFER_MEMORY_MAX_BYTES),
                  config
                      .core()
                      .getLong(sh.oso.connect.oracle.core.config.CoreConfig.BUFFER_SPILL_MAX_BYTES),
                  java.time.Instant::now)
              .register(config.topicPrefix());
    } catch (javax.management.JMException | RuntimeException e) {
      LOG.warn("Task metrics could not be registered: {}", e.getMessage());
    }
  }

  private void closeQuietly() {
    sh.oso.connect.oracle.core.metrics.TaskMetrics.unregister(metricsName);
    metricsName = null;
    if (session != null) {
      try {
        session.close();
      } catch (Exception e) {
        LOG.warn("Closing the database sessions: {}", e.getMessage());
      }
      session = null;
    }
  }

  /** The position the task started from; tests and diagnostics. */
  public Position startPosition() {
    return startPosition;
  }

  public AckTracker acks() {
    return acks;
  }

  RecordQueueSink sink() {
    return sink;
  }
}
