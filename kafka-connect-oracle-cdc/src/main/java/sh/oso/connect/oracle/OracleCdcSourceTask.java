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
      startPosition = position;
      TopicRouter router =
          new TopicRouter(
              config.topicTemplate(session.cdb()), config.topicPrefix(), session.databaseName());
      DebeziumEnvelope envelope = new DebeziumEnvelope(config, router, session.databaseName());
      sink =
          new RecordQueueSink(
              envelope, session.schemas(), position, Math.max(1000, config.pollMaxRecords() * 4));
      CaptureEngine engine = session.engine(position, sink);
      lifecycle =
          new EngineLifecycle(
              engine,
              Duration.ofMillis(Math.max(100, config.pollLingerMs() * 2)),
              t -> LOG.error("Capture engine stopped: {}", t.getMessage(), t));
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
