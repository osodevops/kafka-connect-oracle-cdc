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
package sh.oso.connect.oracle.e2e.support;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.ChangeDecoder;
import sh.oso.connect.oracle.core.engine.EngineSettings;
import sh.oso.connect.oracle.core.engine.EventSink;
import sh.oso.connect.oracle.core.engine.JdbcLobReselector;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.JdbcLogMinerSession;
import sh.oso.connect.oracle.core.mining.JdbcObjectCatalog;
import sh.oso.connect.oracle.core.mining.ObjectIdResolver;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;
import sh.oso.connect.oracle.core.mining.event.LogMinerEventSource;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.JdbcDictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;

/**
 * The real engine over Oracle for engine-tier suites, wired as the connector wires it (online mode,
 * online catalog, ids re-resolved after a DDL step cut), collecting what it commits. Run it to an
 * SCN, change the database, run it further: the way a live task follows the redo.
 */
public final class EngineDriver implements AutoCloseable {

  public final CaptureEngine engine;
  public final List<CommittedTransaction> committed = new ArrayList<>();
  public final List<TableSchema> schemaChanges = new ArrayList<>();
  public final SchemaRegistry registry;
  private final LogMinerEventSource source;
  private volatile ResolvedObjects objects;
  private volatile Set<String> excludedUsers = Set.of();

  /** No step mines past this SCN, so a suite can force many small steps (each its own window). */
  public volatile long safeEndCap = Long.MAX_VALUE;

  /**
   * What a suite may change in the engine the driver builds: the LogMiner query timeout, the engine
   * settings (null for the driver's), and the sink (null for the collecting sink, which fills
   * {@link #committed}); the sink is built from the driver's schema registry, as the task builds
   * its record queue.
   */
  public record Options(
      Duration queryTimeout,
      EngineSettings settings,
      java.util.function.Function<SchemaRegistry, EventSink> sink) {

    public static Options defaults() {
      return new Options(Duration.ofMinutes(5), null, null);
    }

    public Options withQueryTimeout(Duration d) {
      return new Options(d, settings, sink);
    }

    public Options withSettings(EngineSettings s) {
      return new Options(queryTimeout, s, sink);
    }

    public Options withSink(java.util.function.Function<SchemaRegistry, EventSink> f) {
      return new Options(queryTimeout, settings, f);
    }
  }

  public EngineDriver(
      Connection meta,
      Connection mining,
      Connection reselect,
      String include,
      long startScn,
      LobAssembler.Mode mode)
      throws SQLException {
    this(meta, mining, reselect, include, startScn, mode, new InMemorySchemaStore());
  }

  /**
   * With {@code store} shared between drivers, a second driver resumes with the versions the first
   * stored, as a restarted task reads them back from the schema topic.
   */
  public EngineDriver(
      Connection meta,
      Connection mining,
      Connection reselect,
      String include,
      long startScn,
      LobAssembler.Mode mode,
      sh.oso.connect.oracle.core.schema.SchemaStore store)
      throws SQLException {
    this(meta, mining, reselect, include, startScn, mode, store, Options.defaults());
  }

  /** As the other constructors, with the query timeout, settings or sink of {@code options}. */
  public EngineDriver(
      Connection meta,
      Connection mining,
      Connection reselect,
      String include,
      long startScn,
      LobAssembler.Mode mode,
      sh.oso.connect.oracle.core.schema.SchemaStore store,
      Options options)
      throws SQLException {
    JdbcCatalogSource catalog = new JdbcCatalogSource(() -> meta);
    ObjectIdResolver resolver =
        new ObjectIdResolver(
            new JdbcObjectCatalog(() -> meta),
            List.of(include),
            List.of(),
            List.of("FREEPDB1"),
            false);
    objects = resolver.resolve();
    var info = catalog.database();
    LogInventory inventory = new LogInventory(catalog, CaptureMode.ONLINE, 1);
    source =
        new LogMinerEventSource(
            inventory,
            new JdbcLogMinerSession(mining, 2000, options.queryTimeout()),
            objects,
            objects.filter(Set.of(), 1000),
            DictionaryMode.ONLINE_CATALOG);
    registry =
        new SchemaRegistry(
            store,
            new JdbcDictionaryReader(() -> meta),
            new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.ROWID));
    EventSink collecting =
        new EventSink() {
          public void committed(CommittedTransaction tx, int skip, RedoRecordId resume) {
            // read now, as RecordQueueSink does: reselect runs inside the engine's step
            committed.add(
                new CommittedTransaction(
                    tx.key(),
                    tx.firstCaptured(),
                    tx.startId(),
                    tx.commitId(),
                    tx.commitTimestamp(),
                    tx.thread(),
                    tx.username(),
                    tx.clientId(),
                    new ArrayList<>(tx.events())));
          }

          public void stepApplied(long minedTo, RedoRecordId resume) {}

          public void schemaChanged(TableSchema schema, MiningEvent.Ddl ddl) {
            schemaChanges.add(schema);
          }
        };
    EventSink sink = options.sink() == null ? collecting : options.sink().apply(registry);
    EngineSettings settings =
        options.settings() != null
            ? options.settings()
            : new EngineSettings(
                Duration.ofSeconds(2),
                8,
                Duration.ofHours(1),
                Duration.ofMillis(50),
                3,
                CoreConfig.DecodeErrorAction.FAIL);
    java.util.function.Supplier<Long> safeEnd =
        () -> {
          try {
            return Math.min(safeEndCap, catalog.currentScn());
          } catch (SQLException e) {
            throw new OraErrorClassifier().toException(e, "safe end");
          }
        };
    engine =
        new CaptureEngine(
                Position.initial(
                    startScn, new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn())),
                source,
                inventory,
                safeEnd,
                new HeapTransactionBuffer(),
                registry,
                ChangeDecoder.rowDecoder(),
                sink,
                settings.withLobs(mode, 1L << 20, true),
                new OraErrorClassifier(),
                objects.owners(),
                () -> {
                  objects = resolver.resolve();
                  source.update(objects, objects.filter(excludedUsers, 1000));
                  return objects.owners();
                },
                cause -> {
                  throw new SQLException("unexpected reconnect", cause);
                },
                Instant::now)
            .withReselector(new JdbcLobReselector(() -> reselect))
            .withCapturedTables(t -> objects.tables().contains(t));
  }

  /** {@code cdc.users.exclude}: these users' transactions are dropped in the mining query. */
  public EngineDriver excludeUsers(Set<String> users) {
    this.excludedUsers = Set.copyOf(users);
    source.update(objects, objects.filter(excludedUsers, 1000));
    return this;
  }

  /** Mines until the cursor reaches {@code scn} (at most two minutes). */
  public void runTo(long scn) throws Exception {
    long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
    while (engine.cursor().scn() < scn && System.currentTimeMillis() < deadline) {
      if (engine.runOnce() == CaptureEngine.Progress.IDLE) {
        Thread.sleep(100);
      }
    }
  }

  @Override
  public void close() throws SQLException {
    source.close();
  }
}
