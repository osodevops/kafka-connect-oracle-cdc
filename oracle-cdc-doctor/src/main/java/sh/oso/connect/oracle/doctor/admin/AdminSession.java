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
package sh.oso.connect.oracle.doctor.admin;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.doctor.RedoAvailability;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.TopologyProbe;
import sh.oso.connect.oracle.doctor.connect.ConnectApi;
import sh.oso.connect.oracle.doctor.kafka.KafkaPort;

/**
 * One admin command's view of a connector: its configuration (from the worker, or a file when the
 * worker holds externalised secrets), its stored offset, and lazily opened database and Kafka
 * access. Passwords are only ever handed to the clients, never printed.
 */
public final class AdminSession implements AutoCloseable {

  static final Duration STOP_TIMEOUT = Duration.ofSeconds(60);

  private final Environment env;
  private final ConnectApi api;
  private final String connector;
  private final Map<String, String> props;
  private final OracleCdcSourceConnectorConfig config;
  private final Properties kafkaProps;
  private Database db;
  private KafkaPort kafka;

  private AdminSession(
      Environment env,
      ConnectApi api,
      String connector,
      Map<String, String> props,
      OracleCdcSourceConnectorConfig config,
      Properties kafkaProps) {
    this.env = env;
    this.api = api;
    this.connector = connector;
    this.props = props;
    this.config = config;
    this.kafkaProps = kafkaProps;
  }

  /**
   * Opens a session. {@code configFile} replaces the worker's copy of the configuration; {@code
   * bootstrap} and {@code commandConfig} override the connector's {@code cdc.kafka.*} access.
   */
  public static AdminSession open(
      Environment env,
      String connectUrl,
      String connector,
      Path configFile,
      String bootstrap,
      Path commandConfig) {
    ConnectApi api = env.connect(connectUrl);
    Map<String, String> props;
    try {
      props = configFile != null ? ConfigFiles.connector(configFile) : api.config(connector);
    } catch (IOException e) {
      throw new AdminException(
          AdminException.REFUSED,
          "Reading the connector configuration failed: " + e.getMessage(),
          e);
    }
    OracleCdcSourceConnectorConfig config;
    try {
      config = new OracleCdcSourceConnectorConfig(props);
    } catch (ConfigException e) {
      throw AdminException.usage("The connector configuration is not valid: " + e.getMessage());
    }
    Properties kafkaProps = null;
    String servers = bootstrap != null ? bootstrap : config.kafkaBootstrapServers();
    if (servers != null) {
      kafkaProps = config.kafkaClientProperties();
      kafkaProps.put("bootstrap.servers", servers);
      if (commandConfig != null) {
        try {
          kafkaProps.putAll(ConfigFiles.properties(commandConfig));
        } catch (IOException e) {
          throw AdminException.usage("Reading " + commandConfig + " failed: " + e.getMessage());
        }
      }
    }
    return new AdminSession(env, api, connector, props, config, kafkaProps);
  }

  public Environment env() {
    return env;
  }

  public ConnectApi api() {
    return api;
  }

  public String connector() {
    return connector;
  }

  public Map<String, String> props() {
    return props;
  }

  public OracleCdcSourceConnectorConfig config() {
    return config;
  }

  /** The connector's source partition, the key of its one offset. */
  public Map<String, Object> partition() {
    return Map.of("server", config.topicPrefix());
  }

  /** The stored offset map, or null when the connector has none. */
  public Map<String, Object> storedOffset() {
    List<ConnectApi.OffsetEntry> entries;
    try {
      entries = api.offsets(connector);
    } catch (IOException e) {
      throw new AdminException(
          AdminException.REFUSED, "Reading the connector's offsets failed: " + e.getMessage(), e);
    }
    for (ConnectApi.OffsetEntry e : entries) {
      if (e.partition() != null
          && config.topicPrefix().equals(String.valueOf(e.partition().get("server")))) {
        return e.offset();
      }
    }
    return null;
  }

  /** The stored position, or null when the connector has no offset. */
  public Position storedPosition() {
    Map<String, Object> offset = storedOffset();
    return offset == null || offset.isEmpty() ? null : PositionCodec.read(offset);
  }

  public Database db() {
    if (db == null) {
      try {
        db = env.database(config.core());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AdminException(AdminException.REFUSED, "Interrupted while connecting", e);
      } catch (Exception e) {
        throw new AdminException(
            AdminException.REFUSED, "The database could not be reached: " + e.getMessage(), e);
      }
    }
    return db;
  }

  /** Whether broker access is configured. */
  public boolean hasKafka() {
    return kafkaProps != null;
  }

  /** Kafka access; {@code purpose} explains the refusal when none is configured. */
  public KafkaPort kafka(String purpose) {
    if (kafkaProps == null) {
      throw AdminException.usage(
          purpose
              + " needs broker access: set "
              + OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS
              + " in the connector configuration or pass --bootstrap-servers.");
    }
    if (kafka == null) {
      kafka = env.kafka(kafkaProps);
    }
    return kafka;
  }

  /** The archive destination the engine mines from. */
  public int archiveDestId() throws SQLException {
    return new TopologyProbe(
            db().catalog(), config.core().getString(CoreConfig.ARCHIVE_DESTINATION))
        .probe()
        .archiveDestId();
  }

  public RedoAvailability redo() throws SQLException {
    return new RedoAvailability(db().catalog(), config.core().captureMode(), archiveDestId())
        .withProbe(db().logProbe());
  }

  /** The connected database's identity; refuses a position from another database. */
  public DatabaseIdentity identity(Position position) throws SQLException {
    DatabaseInfo info = db().catalog().database();
    DatabaseIdentity id = new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn());
    if (position != null && !position.identity().equals(id)) {
      throw new AdminException(
          "The stored offset belongs to database "
              + position.identity().dbid()
              + " (resetlogs SCN "
              + position.identity().resetlogsScn()
              + ") but the configuration connects to "
              + id.dbid()
              + " (resetlogs SCN "
              + id.resetlogsScn()
              + "). Point the configuration at the original database.");
    }
    return id;
  }

  /** Stops the connector (KIP-875 offsets can only change while it is STOPPED) and waits. */
  public void ensureStopped(PrintWriter out) {
    try {
      String state = api.state(connector);
      if ("STOPPED".equals(state)) {
        return;
      }
      out.println("Stopping connector " + connector + " (state " + state + ").");
      out.flush();
      api.stop(connector);
      Instant deadline = env.clock().instant().plus(STOP_TIMEOUT);
      while (!"STOPPED".equals(api.state(connector))) {
        if (env.clock().instant().isAfter(deadline)) {
          throw new AdminException(
              "Connector "
                  + connector
                  + " did not reach STOPPED within "
                  + STOP_TIMEOUT.toSeconds()
                  + " s; nothing was changed.");
        }
        env.sleep(Duration.ofMillis(500));
      }
    } catch (IOException e) {
      throw new AdminException(
          AdminException.REFUSED, "Stopping the connector failed: " + e.getMessage(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AdminException(AdminException.REFUSED, "Interrupted while stopping", e);
    }
  }

  @Override
  public void close() {
    if (db != null) {
      db.close();
    }
    if (kafka != null) {
      kafka.close();
    }
  }
}
