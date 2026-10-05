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
      session = factory.open(config);
      DatabaseIdentity identity = session.identity();
      Map<String, Object> partition = Map.of("server", config.topicPrefix());
      Map<String, Object> stored =
          context == null || context.offsetStorageReader() == null
              ? null
              : context.offsetStorageReader().offset(partition);
      Position position;
      if (stored == null || stored.isEmpty()) {
        position = Position.initial(session.currentScn(), identity);
        LOG.info("No stored offset; starting at SCN {}", position.resumeScn());
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
              config.heartbeatIntervalMs(),
              System::currentTimeMillis);
      ensureInternalTopics();
      sh.oso.connect.oracle.journal.BufferSetup bufferSetup = loadJournal(position, generation);
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
    Throwable failure = lifecycle.failure();
    List<SourceRecord> records = sink.drain(config.pollMaxRecords(), config.pollLingerMs());
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

  /** Overridable for tests. */
  sh.oso.connect.oracle.topics.TopicAdmin topicAdmin(java.util.Properties clientProps) {
    return new sh.oso.connect.oracle.topics.KafkaTopicAdmin(clientProps);
  }

  private void closeQuietly() {
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
