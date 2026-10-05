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
package sh.oso.connect.oracle.core.snapshot;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.core.engine.EngineMetrics;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.errors.SnapshotTooOldException;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * PRD-02 section 3: reads each table in batches of up to {@code threads} contiguous chunks, all as
 * of one SCN read just before the batch, on reader threads with connections of their own. A batch
 * is ready once read; the sink takes ready batches in order as streaming passes their SCN ({@link
 * #ready}), so every change committed after a chunk's SCN follows the chunk. Batches of a table
 * come in key order, which keeps a table's progress a single frontier.
 *
 * <p>Reads pause while {@code maxPendingChunks} chunks wait for the sink. A chunk that fails is
 * retried from its own start with a fresh SCN, the chunks before it kept (SNAP-3); ORA-01555 and
 * ORA-08181 also halve the chunk size, down to {@link #MIN_CHUNK_ROWS} (SNAP-4).
 */
public final class SnapshotCoordinator implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotCoordinator.class);

  /** SNAP-4: the floor below which a chunk is not split further. */
  public static final int MIN_CHUNK_ROWS = 1000;

  /** ORA-01466 after a recent DDL: the SCN-to-time mapping needs this long to move past it. */
  static final long TABLE_CHANGED_WAIT_MS = 3500;

  /** Opens a source on a new connection. */
  public interface SourceFactory {
    SnapshotSource open() throws SQLException;
  }

  public record Settings(
      int threads,
      int chunkRows,
      int retries,
      int maxPendingChunks,
      Map<TableId, String> overrides) {}

  /** A chunk and its rows. */
  public record Chunk(ChunkRange range, List<SnapshotRow> rows) {}

  /**
   * Chunks of one table read as of {@code scn}, in key order. {@code tableDone}: the table has no
   * more chunks; {@code last}: no table has. {@code schema} is null for a table that no longer
   * exists.
   */
  public record Batch(
      TableId table,
      TableSchema schema,
      long scn,
      List<Chunk> chunks,
      boolean tableDone,
      boolean last) {}

  private final List<TableId> tables;
  private final SnapshotProgress progress;
  private final Function<TableId, Optional<TableSchema>> schemas;
  private final SourceFactory sources;
  private final Settings settings;
  private final EngineMetrics metrics;
  private final OraErrorClassifier classifier;
  private final ArrayDeque<Batch> ready = new ArrayDeque<>();
  private final List<SnapshotSource> opened = new ArrayList<>();
  private final ThreadLocal<SnapshotSource> local = new ThreadLocal<>();
  private int pendingChunks;

  /** No batch is being read. */
  private static final long IDLE = Long.MAX_VALUE;

  /**
   * The SCN of the batch being read, 0 while that SCN is being taken: streaming may not publish a
   * commit at or after it until the batch is ready, or the commit would precede older row images.
   */
  private long inFlight = IDLE;

  private volatile RuntimeException failure;
  private volatile boolean finished;
  private volatile boolean closed;
  private boolean paused;
  private Thread thread;
  private ExecutorService readers;

  /**
   * {@code tables}: the captured tables in snapshot order, done ones included (they are skipped).
   * {@code schemas}: the registry's cached version, never a database read (the reader threads must
   * not share the engine's metadata connection); empty for a table that has gone.
   */
  public SnapshotCoordinator(
      List<TableId> tables,
      SnapshotProgress progress,
      Function<TableId, Optional<TableSchema>> schemas,
      SourceFactory sources,
      Settings settings,
      EngineMetrics metrics,
      OraErrorClassifier classifier) {
    List<TableId> todo = new ArrayList<>();
    for (TableId t : tables) {
      if (!progress.done(t)) {
        todo.add(t);
      }
    }
    this.tables = List.copyOf(todo);
    this.progress = progress;
    this.schemas = schemas;
    this.sources = sources;
    this.settings = settings;
    this.metrics = metrics;
    this.classifier = classifier;
    metrics.snapshotTablesRemaining.set(this.tables.size());
  }

  public List<TableId> tables() {
    return tables;
  }

  public synchronized void start() {
    readers =
        Executors.newFixedThreadPool(
            Math.max(1, settings.threads()),
            r -> {
              Thread t = new Thread(r, "oracle-cdc-snapshot-reader");
              t.setDaemon(true);
              return t;
            });
    thread = new Thread(this::run, "oracle-cdc-snapshot");
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * The batches whose SCN is at or below {@code maxScn}, oldest first; streaming has emitted every
   * commit below that SCN. Rethrows a failure of the reader.
   */
  public synchronized List<Batch> ready(long maxScn) {
    // the caller is about to publish a commit at maxScn, or passed it: a batch read as of an SCN at
    // or below it must go first, so wait for it (one chunk read: the PRD's flashback window)
    while (failure == null && !closed && inFlight != IDLE && inFlight <= maxScn) {
      try {
        wait(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    if (failure != null) {
      throw failure;
    }
    List<Batch> out = new ArrayList<>();
    while (!ready.isEmpty() && ready.peek().scn() <= maxScn) {
      Batch b = ready.poll();
      pendingChunks -= b.chunks().size();
      metrics.snapshotChunksEmitted.addAndGet(b.chunks().size());
      for (Chunk c : b.chunks()) {
        metrics.snapshotRowsEmitted.addAndGet(c.rows().size());
      }
      metrics.snapshotChunksPending.set(pendingChunks);
      out.add(b);
    }
    if (!out.isEmpty()) {
      notifyAll();
    }
    return out;
  }

  /** Every table has been read and every batch taken. */
  public synchronized boolean finished() {
    return finished && ready.isEmpty();
  }

  private void run() {
    SnapshotSource planner = null;
    try {
      if (tables.isEmpty()) {
        finished = true; // nothing to read: no connection needed
        return;
      }
      planner = sources.open();
      for (int i = 0; i < tables.size() && !closed; i++) {
        planner = snapshot(planner, tables.get(i), i == tables.size() - 1);
        metrics.snapshotTablesRemaining.set(tables.size() - i - 1);
      }
      finished = !closed;
    } catch (OracleCdcException e) {
      failure = e;
    } catch (SQLException e) {
      failure = classifier.toException(e, "snapshot");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      failure = e; // not a database error: rethrown as it is on the engine thread
    } finally {
      if (failure != null) {
        LOG.error("Snapshot stopped: {}", failure.getMessage(), failure);
      }
      inFlight(IDLE);
      quietly(planner);
      readers.shutdownNow();
      // every read has returned: the reader connections are not needed after the snapshot
      synchronized (opened) {
        for (SnapshotSource s : opened) {
          quietly(s);
        }
        opened.clear();
      }
    }
  }

  /** Reads one table to its end; returns the planner, reopened if it failed. */
  private SnapshotSource snapshot(SnapshotSource planner, TableId t, boolean lastTable)
      throws SQLException, InterruptedException {
    Optional<TableSchema> found = schemas.apply(t);
    if (found.isEmpty()) {
      LOG.info("Snapshot: {} no longer exists; nothing to read", t.fqn());
      enqueue(new Batch(t, null, 0, List.of(), true, lastTable));
      return planner;
    }
    TableSchema schema = found.get();
    SnapshotSource.Kind kind = planner.kind(schema);
    List<String> lower = progress.frontier(t);
    int rows = settings.chunkRows();
    int failures = 0;
    String where = settings.overrides().get(t);
    LOG.info("Snapshot of {} by {} from {}", t.fqn(), kind, lower == null ? "the start" : lower);
    while (!closed) {
      awaitRoom();
      List<ChunkRange> ranges;
      long scn;
      try {
        ranges =
            planner.plan(
                schema,
                kind,
                lower,
                kind == SnapshotSource.Kind.ALL ? 1 : Math.max(1, settings.threads()),
                rows);
        inFlight(0); // hold streaming while the SCN is taken
        scn = planner.currentScn();
        inFlight(scn);
      } catch (SQLException e) {
        inFlight(IDLE);
        failures = failed(e, failures);
        quietly(planner);
        planner = sources.open();
        continue;
      }
      List<Future<List<SnapshotRow>>> reads = new ArrayList<>();
      for (ChunkRange r : ranges) {
        reads.add(readers.submit(() -> read(schema, kind, r, scn, where)));
      }
      List<Chunk> chunks = new ArrayList<>();
      Throwable error = null;
      for (int i = 0; i < reads.size(); i++) {
        try {
          List<SnapshotRow> got = reads.get(i).get();
          if (error == null) {
            chunks.add(new Chunk(ranges.get(i), got));
          }
        } catch (ExecutionException e) {
          if (error == null) {
            error = e.getCause();
          }
        }
      }
      metrics.snapshotChunksRead.addAndGet(chunks.size());
      boolean done = error == null && ranges.get(ranges.size() - 1).last();
      if (!chunks.isEmpty() || done) {
        enqueue(new Batch(t, schema, scn, chunks, done, done && lastTable));
      }
      inFlight(IDLE);
      if (done) {
        return planner;
      }
      if (!chunks.isEmpty()) {
        lower = chunks.get(chunks.size() - 1).range().upper();
      }
      if (error == null) {
        failures = 0;
        continue;
      }
      if (error instanceof OracleCdcException oe) {
        throw oe;
      }
      if (!(error instanceof SQLException sql)) {
        throw new IllegalStateException("snapshot read of " + t.fqn() + " failed", error);
      }
      int code = OraErrorClassifier.oraCode(sql);
      metrics.snapshotChunkRetries.incrementAndGet();
      if (code == 1555 || code == 8181) {
        if (rows <= MIN_CHUNK_ROWS) {
          throw new SnapshotTooOldException(
              "Snapshot of "
                  + t.fqn()
                  + " could not read a chunk of "
                  + rows
                  + " rows as of its SCN: "
                  + firstLine(sql),
              "Raise UNDO_RETENTION (and the undo tablespace) so a chunk read of a few seconds"
                  + " finds its undo, then restart; the snapshot resumes at this chunk.",
              sql);
        }
        rows = Math.max(MIN_CHUNK_ROWS, rows / 2);
        LOG.warn("Snapshot of {}: {}; retrying with {} rows per chunk", t, firstLine(sql), rows);
      } else if (code == 1466) {
        // a DDL just before the SCN: wait for a later SCN whose time is past it
        Thread.sleep(TABLE_CHANGED_WAIT_MS);
        failures = failed(sql, failures);
      } else {
        failures = failed(sql, failures);
      }
    }
    return planner;
  }

  /** SNAP-3: counts a retry; past cdc.snapshot.chunk.retries the error stops the snapshot. */
  private int failed(SQLException e, int failures) {
    if (failures + 1 > settings.retries()) {
      throw classifier.toException(e, "snapshot chunk");
    }
    LOG.warn("Snapshot chunk failed, retrying with a fresh SCN: {}", firstLine(e));
    return failures + 1;
  }

  private List<SnapshotRow> read(
      TableSchema schema, SnapshotSource.Kind kind, ChunkRange range, long scn, String where)
      throws SQLException {
    SnapshotSource s = local.get();
    if (s == null) {
      s = sources.open();
      synchronized (opened) {
        opened.add(s);
      }
      local.set(s);
    }
    try {
      return s.read(schema, kind, range, scn, where);
    } catch (SQLException e) {
      local.remove(); // the connection may be broken: the next read opens a fresh one
      synchronized (opened) {
        opened.remove(s);
      }
      quietly(s);
      throw e;
    }
  }

  private synchronized void inFlight(long scn) {
    inFlight = scn;
    notifyAll();
  }

  private synchronized void enqueue(Batch b) {
    ready.add(b);
    pendingChunks += b.chunks().size();
    metrics.snapshotChunksPending.set(pendingChunks);
  }

  private synchronized void awaitRoom() throws InterruptedException {
    while (!closed && (paused || pendingChunks >= Math.max(1, settings.maxPendingChunks()))) {
      wait(500);
    }
  }

  /** SRC-SIG-1 snapshot-pause and snapshot-resume: no new batch is read while paused. */
  public synchronized void pause(boolean pause) {
    paused = pause;
    notifyAll();
  }

  public synchronized boolean paused() {
    return paused;
  }

  private static String firstLine(SQLException e) {
    return String.valueOf(e.getMessage()).split("\n")[0].trim();
  }

  private static void quietly(SnapshotSource s) {
    if (s == null) {
      return;
    }
    try {
      s.close();
    } catch (SQLException | RuntimeException e) {
      LOG.debug("closing a snapshot connection: {}", e.getMessage());
    }
  }

  @Override
  public void close() {
    synchronized (this) {
      closed = true; // readers waiting for room and callers of ready() see it at once
      notifyAll();
    }
    if (thread != null) {
      thread.interrupt();
    }
    if (readers != null) {
      readers.shutdownNow();
    }
    synchronized (opened) {
      for (SnapshotSource s : opened) {
        quietly(s);
      }
      opened.clear();
    }
  }
}
