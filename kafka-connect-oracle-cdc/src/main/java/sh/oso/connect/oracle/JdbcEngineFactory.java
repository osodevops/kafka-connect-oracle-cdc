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
    private volatile Connection reselect; // cdc.lob.mode=reselect only, opened on first use
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
    public CaptureEngine engine(
        Position start, EventSink sink, sh.oso.connect.oracle.journal.BufferSetup setup)
        throws Exception {
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
          objects.objectCount(),
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
      CaptureEngine engine =
          new CaptureEngine(
              start,
              source,
              inventory,
              safeEnd,
              buffer(setup),
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
                    objects.objectCount(),
                    objects.owners());
                return objects.owners();
              },
              cause -> {
                // CORE-CONN-6: drop both sessions, reopen them with the factory's backoff and
                // continue; the engine re-mines the failed step from the same cursor
                LOG.warn("Reconnecting to the database after: {}", cause.getMessage());
                closeQuietly(source);
                closeQuietly(meta);
                closeQuietly(reselect);
                reselect = null;
                meta = connections.open(ConnectionRole.METADATA);
                mining = connections.open(ConnectionRole.MINING);
                source = newSource(inventory);
                LOG.info("Reconnected; mining resumes from the last applied step");
                return new CaptureEngine.Sources(source, inventory, safeEnd);
              },
              Instant::now);
      // CORE-MINE-6: 0 means the number of cores minus one, at most 8
      int threads = core.getInt(CoreConfig.MINING_DECODE_THREADS);
      engine.withDecodeThreads(
          threads > 0
              ? threads
              : Math.min(8, Math.max(1, Runtime.getRuntime().availableProcessors() - 1)));
      if (core.lobMode() == CoreConfig.LobMode.RESELECT) {
        // CORE-DEC-7: AS OF queries on a connection of their own, which switches container
        engine.withReselector(
            new sh.oso.connect.oracle.core.engine.JdbcLobReselector(
                () -> {
                  if (reselect == null) {
                    try {
                      reselect = connections.open(ConnectionRole.METADATA);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new IllegalStateException(
                          "interrupted opening the reselect session", e);
                    }
                  }
                  return reselect;
                }));
      }
      // CORE-TX-7: orphan checks against GV$TRANSACTION on the metadata connection
      engine.withOrphanDetector(
          new sh.oso.connect.oracle.core.orphan.OrphanDetector(
              new sh.oso.connect.oracle.core.orphan.JdbcTransactionProbe(() -> meta),
              Duration.ofMillis(core.getLong(CoreConfig.TRANSACTION_ORPHAN_CHECK_INTERVAL_MS)),
              "fail".equalsIgnoreCase(core.getString(CoreConfig.TRANSACTION_ORPHAN_ACTION))
                  ? sh.oso.connect.oracle.core.orphan.OrphanDetector.Action.FAIL
                  : sh.oso.connect.oracle.core.orphan.OrphanDetector.Action.RELEASE,
              start.released()));
      return engine;
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

    /**
     * CORE-TX-3: heap up to the budget, then the largest transactions spill under the spill dir.
     */
    private sh.oso.connect.oracle.core.buffer.TransactionBuffer buffer(
        sh.oso.connect.oracle.journal.BufferSetup setup) throws java.io.IOException {
      String configured = core.getString(CoreConfig.BUFFER_SPILL_DIR);
      String name = config.originalsStrings().getOrDefault("name", config.topicPrefix());
      java.nio.file.Path dir =
          configured == null || configured.isBlank()
              ? java.nio.file.Path.of(
                  System.getProperty("java.io.tmpdir"), "oracle-cdc-spill", name)
              : java.nio.file.Path.of(configured);
      sh.oso.connect.oracle.core.buffer.SpillStore store =
          new sh.oso.connect.oracle.core.buffer.SpillStore(
              dir, core.getLong(CoreConfig.BUFFER_SPILL_MAX_BYTES));
      closeables.add(store);
      LOG.info(
          "Transaction buffer: {} bytes on heap, spill under {}",
          core.getLong(CoreConfig.BUFFER_MEMORY_MAX_BYTES),
          dir);
      HeapTransactionBuffer buffer =
          new HeapTransactionBuffer(
              core.getLong(CoreConfig.BUFFER_MEMORY_MAX_BYTES),
              store,
              setup.policy(),
              setup.journal(),
              setup.generation(),
              java.time.Instant::now);
      for (var chunks : setup.restored().values()) {
        buffer.restore(chunks); // CORE-TX-5: journal state reloads before mining resumes
      }
      if (!setup.restored().isEmpty()) {
        LOG.info(
            "Restored {} journaled transactions from the journal topic", setup.restored().size());
      }
      return buffer;
    }

    @Override
    public void close() throws Exception {
      closeQuietly(source); // ends the LogMiner session and closes the mining connection
      closeQuietly(reselect);
      reselect = null;
      Connection m = meta;
      meta = null;
      if (m != null) {
        m.close();
      }
    }
  }
}
