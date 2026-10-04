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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.ChangeDecoder;
import sh.oso.connect.oracle.core.engine.EngineSettings;
import sh.oso.connect.oracle.core.engine.EventSink;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.jdbc.ConnectionFactory;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.OracleConnectionSpec;
import sh.oso.connect.oracle.core.jdbc.RetryPolicy;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.JdbcLogMinerSession;
import sh.oso.connect.oracle.core.mining.JdbcObjectCatalog;
import sh.oso.connect.oracle.core.mining.ObjectIdResolver;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;
import sh.oso.connect.oracle.core.mining.event.LogMinerEventSource;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.JdbcDictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.topology.ArchiveDestination;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;

/** Wires the real engine: metadata and mining connections, inventory, resolver, registry. */
public final class JdbcEngineFactory implements EngineFactory {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcEngineFactory.class);

  @Override
  public Session open(OracleCdcSourceConnectorConfig config) throws Exception {
    CoreConfig core = config.core();
    OraErrorClassifier classifier = new OraErrorClassifier(Set.copyOf(core.extraRetryErrorCodes()));
    ConnectionFactory connections =
        new ConnectionFactory(
            OracleConnectionSpec.from(core),
            new RetryPolicy(Duration.ofMillis(core.getLong(CoreConfig.RETRY_MAX_TIME_MS))),
            classifier);
    JdbcSession session = new JdbcSession(config, core, classifier, connections);
    session.meta = connections.open(ConnectionRole.METADATA);
    session.info = session.catalog.database();
    return session;
  }

  private static final class JdbcSession implements Session {
    private final OracleCdcSourceConnectorConfig config;
    private final CoreConfig core;
    private final OraErrorClassifier classifier;
    private final ConnectionFactory connections;
    private volatile Connection meta;
    private volatile Connection mining;
    private final JdbcCatalogSource catalog;
    private DatabaseInfo info;
    private final SchemaRegistry schemas;
    private volatile LogMinerEventSource source;
    private volatile ResolvedObjects objects;
    private final Set<String> excludedUsers;
    private final int inlistMax;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    JdbcSession(
        OracleCdcSourceConnectorConfig config,
        CoreConfig core,
        OraErrorClassifier classifier,
        ConnectionFactory connections) {
      this.config = config;
      this.core = core;
      this.classifier = classifier;
      this.connections = connections;
      this.excludedUsers = Set.copyOf(config.usersExclude());
      this.inlistMax = core.getInt(CoreConfig.MINING_INLIST_MAX);
      // the catalogs read the connection on every call so a reconnect swaps it underneath them
      this.catalog = new JdbcCatalogSource(() -> meta);
      this.schemas =
          new SchemaRegistry(
              new MapSchemaStore(),
              new JdbcDictionaryReader(() -> meta),
              new KeySelector(config.keyOverrides(), config.keyMissing()));
    }

    private static void closeQuietly(AutoCloseable c) {
      if (c != null) {
        try {
          c.close();
        } catch (Exception ignore) {
          // the connection is already gone
        }
      }
    }

    private LogMinerEventSource newSource(LogInventory inventory) {
      return new LogMinerEventSource(
          inventory,
          new JdbcLogMinerSession(
              mining,
              core.getInt(CoreConfig.MINING_FETCH_SIZE),
              Duration.ofMillis(core.getLong(CoreConfig.MINING_QUERY_TIMEOUT_MS))),
          objects,
          objects.filter(excludedUsers, inlistMax),
          DictionaryMode.ONLINE_CATALOG);
    }

    @Override
    public DatabaseIdentity identity() {
      return new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn());
    }

    @Override
    public long currentScn() {
      try {
        return catalog.currentScn();
      } catch (SQLException e) {
        throw classifier.toException(e, "reading the current SCN");
      }
    }

    @Override
    public boolean cdb() {
      return info.cdb();
    }

    @Override
    public String databaseName() {
      return info.name();
    }

    @Override
    public SchemaRegistry schemas() {
      return schemas;
    }

    @Override
    public CaptureEngine engine(Position start, EventSink sink) throws Exception {
      int destId = archiveDestination();
      LogInventory inventory = new LogInventory(catalog, core.captureMode(), destId);
      ObjectIdResolver resolver =
          new ObjectIdResolver(
              new JdbcObjectCatalog(() -> meta),
              config.tablesInclude(),
              config.tablesExclude(),
              core.pdbs(),
              config.tablesCaseSensitive());
      objects = resolver.resolve();
      LOG.info(
          "Capturing {} tables ({} object ids) for owners {}",
          objects.tables().size(),
          objects.byObjectId().size(),
          objects.owners());
      mining = connections.open(ConnectionRole.MINING);
      source = newSource(inventory);
      java.util.function.Supplier<Long> safeEnd =
          () -> {
            try {
              return core.captureMode() == CoreConfig.CaptureMode.ARCHIVE_ONLY
                  ? inventory.archiveOnlySafeEnd(start.resumeScn())
                  : catalog.currentScn();
            } catch (SQLException e) {
              throw classifier.toException(e, "reading the safe end SCN");
            }
          };
      return new CaptureEngine(
          start,
          source,
          inventory,
          safeEnd,
          new HeapTransactionBuffer(),
          schemas,
          ChangeDecoder.rowDecoder(),
          sink,
          EngineSettings.from(core),
          classifier,
          objects.owners(),
          () -> {
            objects = resolver.resolve();
            source.update(objects, objects.filter(excludedUsers, inlistMax));
            LOG.info(
                "Object ids refreshed after DDL: {} tables ({} ids) for owners {}",
                objects.tables().size(),
                objects.byObjectId().size(),
                objects.owners());
            return objects.owners();
          },
          cause -> {
            // CORE-CONN-6: drop both sessions, reopen them with the factory's backoff and
            // continue; the engine re-mines the failed step from the same cursor
            LOG.warn("Reconnecting to the database after: {}", cause.getMessage());
            closeQuietly(source);
            closeQuietly(meta);
            meta = connections.open(ConnectionRole.METADATA);
            mining = connections.open(ConnectionRole.MINING);
            source = newSource(inventory);
            LOG.info("Reconnected; mining resumes from the last applied step");
            return new CaptureEngine.Sources(source, inventory, safeEnd);
          },
          Instant::now);
    }

    /** The configured destination by name, else the lowest valid local one (DOC-12). */
    private int archiveDestination() throws SQLException {
      String wanted = core.getString(CoreConfig.ARCHIVE_DESTINATION);
      List<ArchiveDestination> dests = catalog.archiveDestinations();
      if (wanted != null && !wanted.isBlank()) {
        for (ArchiveDestination d : dests) {
          if (d.name().equalsIgnoreCase(wanted)) {
            return d.destId();
          }
        }
        throw new sh.oso.connect.oracle.core.errors.TopologyException(
            "Archive destination " + wanted + " does not exist",
            "Set cdc.archive.destination to a name from V$ARCHIVE_DEST_STATUS or leave it empty.");
      }
      return dests.stream()
          .filter(ArchiveDestination::validLocal)
          .mapToInt(ArchiveDestination::destId)
          .min()
          .orElseThrow(
              () ->
                  new sh.oso.connect.oracle.core.errors.TopologyException(
                      "No valid local archive destination",
                      "Configure log_archive_dest_n with a LOCATION (doctor rule DOC-12)."));
    }

    @Override
    public void close() throws Exception {
      closeQuietly(source); // ends the LogMiner session and closes the mining connection
      Connection m = meta;
      meta = null;
      if (m != null) {
        m.close();
      }
    }
  }
}
