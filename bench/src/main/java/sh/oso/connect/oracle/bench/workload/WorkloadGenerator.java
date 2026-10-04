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
package sh.oso.connect.oracle.bench.workload;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Seeded, deterministic Oracle workload: inserts, updates (including key changes), deletes, LOB
 * writes, savepoint and full rollbacks, multi-table transactions, occasional large transactions,
 * truncates and column DDL, over N sessions. Each session owns the rows it inserted, so sessions
 * never block each other and the per-session statement stream depends only on the seed.
 *
 * <p>Every committed transaction carries one {@link Ledger} row written inside the transaction; the
 * correctness oracle compares the ledger with what the connector delivered.
 */
public final class WorkloadGenerator {

  private final WorkloadSpec spec;
  private final String url;
  private final String user;
  private final String password;

  public WorkloadGenerator(WorkloadSpec spec, String url, String user, String password) {
    spec.validate();
    this.spec = spec;
    this.url = url;
    this.user = user;
    this.password = password;
  }

  /** Drops and recreates the data tables and the ledger with ALL COLUMNS supplemental logging. */
  public void reset() throws SQLException {
    try (Connection c = open();
        Statement s = c.createStatement()) {
      for (int i = 1; i <= spec.tables; i++) {
        dropIfExists(s, spec.tableName(i));
        s.execute(
            "CREATE TABLE "
                + spec.tableName(i)
                + " (id NUMBER PRIMARY KEY, grp NUMBER NOT NULL, name VARCHAR2(100),"
                + " amount NUMBER(14,2), flag CHAR(1), note CLOB, updated_at TIMESTAMP(6))");
        s.execute("ALTER TABLE " + spec.tableName(i) + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      dropIfExists(s, spec.ledgerTable);
      s.execute(Ledger.ddl(spec.ledgerTable));
      c.commit();
    }
  }

  private static void dropIfExists(Statement s, String table) throws SQLException {
    try {
      s.execute("DROP TABLE " + table + " PURGE");
    } catch (SQLException e) {
      if (e.getErrorCode() != 942) {
        throw e;
      }
    }
  }

  /** Runs every session to completion and returns the counters. */
  public WorkloadResult run() throws SQLException, InterruptedException {
    WorkloadResult result = new WorkloadResult();
    long start = System.nanoTime();
    long deadline =
        spec.durationSeconds > 0
            ? start + TimeUnit.SECONDS.toNanos(spec.durationSeconds)
            : Long.MAX_VALUE;
    ExecutorService pool = Executors.newFixedThreadPool(spec.sessions);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < spec.sessions; i++) {
        final int session = i;
        futures.add(
            pool.submit(
                () -> {
                  new Session(session, deadline, result).run();
                  return null;
                }));
      }
      for (Future<?> f : futures) {
        try {
          f.get();
        } catch (java.util.concurrent.ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof SQLException) {
            throw (SQLException) cause;
          }
          throw new IllegalStateException(cause);
        }
      }
    } finally {
      pool.shutdownNow();
    }
    result.durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    return result;
  }

  private Connection open() throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", user);
    p.setProperty("password", password);
    Connection c = DriverManager.getConnection(url, p);
    c.setAutoCommit(false);
    try (Statement s = c.createStatement()) {
      s.execute("ALTER SESSION SET ddl_lock_timeout = 60");
    }
    return c;
  }

  private enum Op {
    INSERT,
    UPDATE,
    DELETE,
    KEY_CHANGE,
    LOB
  }

  /** One session's deterministic stream. */
  private final class Session {
    private final int id;
    private final long deadline;
    private final WorkloadResult result;
    private final SplittableRandom rnd;
    private final List<List<Long>> live = new ArrayList<>();
    private long nextId;
    private long seq;
    private int extraColumns;

    Session(int id, long deadline, WorkloadResult result) {
      this.id = id;
      this.deadline = deadline;
      this.result = result;
      this.rnd = new SplittableRandom(spec.seed + id);
      this.nextId = (long) (id + 1) * 1_000_000_000L;
      for (int t = 0; t < spec.tables; t++) {
        live.add(new ArrayList<>());
      }
    }

    void run() throws SQLException {
      try (Connection c = open()) {
        while (spec.transactionsPerSession > 0
            ? seq < spec.transactionsPerSession
            : System.nanoTime() < deadline) {
          seq++;
          transaction(c);
          if (id == 0) {
            maybeTruncate(c);
            maybeDdl(c);
          }
        }
      }
    }

    private void transaction(Connection c) throws SQLException {
      boolean large = spec.largeTransactionEvery > 0 && seq % spec.largeTransactionEvery == 0;
      int rows = large ? spec.largeTransactionRows : 1 + rnd.nextInt(spec.maxRowsPerTransaction);
      boolean fullRollback = rnd.nextDouble() < spec.fullRollbackProbability;
      boolean savepoint = rows >= 2 && rnd.nextDouble() < spec.savepointRollbackProbability;
      int savepointAt = savepoint ? Math.max(1, rows / 2) : -1;
      List<List<Long>> txStart = snapshot();
      List<List<Long>> spStart = null;
      int[] counts = new int[Op.values().length];
      int[] spCounts = null;
      int[] perTable = new int[spec.tables];
      int[] spPerTable = null;

      for (int i = 0; i < rows; i++) {
        if (i == savepointAt) {
          try (Statement s = c.createStatement()) {
            s.execute("SAVEPOINT wl_sp");
          }
          spStart = snapshot();
          spCounts = counts.clone();
          spPerTable = perTable.clone();
        }
        int table = rnd.nextInt(spec.tables);
        Op op = pickOp(table);
        int affected = apply(c, table, op);
        counts[op.ordinal()] += affected;
        perTable[table] += affected;
      }
      if (savepoint) {
        try (Statement s = c.createStatement()) {
          s.execute("ROLLBACK TO SAVEPOINT wl_sp");
        }
        restore(spStart);
        counts = spCounts;
        perTable = spPerTable;
        result.savepointRollbacks.increment();
      }
      String xid = Ledger.currentXid(c);
      if (fullRollback || xid == null) {
        c.rollback();
        restore(txStart);
        result.rolledBack.increment();
        return;
      }
      int inserts = counts[Op.INSERT.ordinal()] + counts[Op.LOB.ordinal()];
      int updates = counts[Op.UPDATE.ordinal()] + counts[Op.KEY_CHANGE.ordinal()];
      int deletes = counts[Op.DELETE.ordinal()];
      Ledger.insert(
          c,
          spec.ledgerTable,
          xid,
          id,
          seq,
          inserts + updates + deletes,
          inserts,
          updates,
          deletes,
          effects(perTable));
      c.commit();
      result.committed.increment();
      result.inserts.add(inserts);
      result.updates.add(counts[Op.UPDATE.ordinal()]);
      result.keyChanges.add(counts[Op.KEY_CHANGE.ordinal()]);
      result.lobWrites.add(counts[Op.LOB.ordinal()]);
      result.deletes.add(deletes);
    }

    private Op pickOp(int table) {
      if (live.get(table).isEmpty()) {
        return rnd.nextDouble() * (spec.insertWeight + spec.lobWeight) < spec.insertWeight
            ? Op.INSERT
            : Op.LOB;
      }
      double total =
          spec.insertWeight
              + spec.updateWeight
              + spec.deleteWeight
              + spec.keyChangeWeight
              + spec.lobWeight;
      double x = rnd.nextDouble() * total;
      if ((x -= spec.insertWeight) < 0) {
        return Op.INSERT;
      }
      if ((x -= spec.updateWeight) < 0) {
        return Op.UPDATE;
      }
      if ((x -= spec.deleteWeight) < 0) {
        return Op.DELETE;
      }
      if ((x -= spec.keyChangeWeight) < 0) {
        return Op.KEY_CHANGE;
      }
      return Op.LOB;
    }

    private int apply(Connection c, int table, Op op) throws SQLException {
      String t = spec.tableName(table + 1);
      List<Long> ids = live.get(table);
      switch (op) {
        case INSERT:
        case LOB:
          {
            long newId = nextId++;
            try (PreparedStatement ps =
                c.prepareStatement(
                    "INSERT INTO "
                        + t
                        + " (id, grp, name, amount, flag, note, updated_at) VALUES (?, ?, ?, ?,"
                        + " ?, ?, SYSTIMESTAMP)")) {
              ps.setLong(1, newId);
              ps.setInt(2, rnd.nextInt(100));
              ps.setString(3, "row " + newId + " " + token());
              ps.setBigDecimal(
                  4, java.math.BigDecimal.valueOf(rnd.nextLong(-10_000_000, 10_000_000), 2));
              ps.setString(5, rnd.nextBoolean() ? "Y" : "N");
              if (op == Op.LOB) {
                Clob clob = c.createClob();
                clob.setString(1, text(1 + rnd.nextInt(spec.lobMaxChars)));
                ps.setClob(6, clob);
              } else {
                ps.setNull(6, java.sql.Types.CLOB);
              }
              int n = ps.executeUpdate();
              ids.add(newId);
              return n;
            }
          }
        case UPDATE:
          {
            long target = ids.get(rnd.nextInt(ids.size()));
            try (PreparedStatement ps =
                c.prepareStatement(
                    "UPDATE "
                        + t
                        + " SET amount = ?, name = ?, flag = ?, updated_at = SYSTIMESTAMP WHERE id"
                        + " = ?")) {
              ps.setBigDecimal(
                  1, java.math.BigDecimal.valueOf(rnd.nextLong(-10_000_000, 10_000_000), 2));
              ps.setString(2, "upd " + token());
              ps.setString(3, rnd.nextBoolean() ? "Y" : "N");
              ps.setLong(4, target);
              return ps.executeUpdate();
            }
          }
        case DELETE:
          {
            int idx = rnd.nextInt(ids.size());
            long target = ids.remove(idx);
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + t + " WHERE id = ?")) {
              ps.setLong(1, target);
              return ps.executeUpdate();
            }
          }
        case KEY_CHANGE:
          {
            int idx = rnd.nextInt(ids.size());
            long old = ids.get(idx);
            long fresh = nextId++;
            try (PreparedStatement ps =
                c.prepareStatement(
                    "UPDATE " + t + " SET id = ?, updated_at = SYSTIMESTAMP WHERE id = ?")) {
              ps.setLong(1, fresh);
              ps.setLong(2, old);
              int n = ps.executeUpdate();
              if (n > 0) {
                ids.set(idx, fresh);
              } else {
                ids.remove(idx);
              }
              return n;
            }
          }
        default:
          throw new IllegalStateException(op.name());
      }
    }

    private void maybeTruncate(Connection c) throws SQLException {
      if (spec.truncateProbability <= 0 || rnd.nextDouble() >= spec.truncateProbability) {
        return;
      }
      int table = rnd.nextInt(spec.tables);
      try (Statement s = c.createStatement()) {
        s.execute("TRUNCATE TABLE " + spec.tableName(table + 1));
      }
      live.get(table).clear();
      result.truncates.increment();
    }

    private void maybeDdl(Connection c) throws SQLException {
      if (spec.ddlProbability <= 0 || rnd.nextDouble() >= spec.ddlProbability) {
        return;
      }
      int table = rnd.nextInt(spec.tables);
      String t = spec.tableName(table + 1);
      try (Statement s = c.createStatement()) {
        if (extraColumns > 0 && rnd.nextBoolean()) {
          s.execute("ALTER TABLE " + t + " DROP COLUMN extra_" + extraColumns);
          extraColumns--;
        } else {
          extraColumns++;
          s.execute("ALTER TABLE " + t + " ADD (extra_" + extraColumns + " NUMBER)");
        }
      } catch (SQLException e) {
        if (e.getErrorCode() == 904 || e.getErrorCode() == 1430) {
          return; // column already absent or present on this table; harmless
        }
        throw e;
      }
      result.ddls.increment();
    }

    private List<List<Long>> snapshot() {
      List<List<Long>> copy = new ArrayList<>(live.size());
      for (List<Long> l : live) {
        copy.add(new ArrayList<>(l));
      }
      return copy;
    }

    private void restore(List<List<Long>> snapshot) {
      for (int i = 0; i < live.size(); i++) {
        live.set(i, snapshot.get(i));
      }
    }

    private String token() {
      return Long.toString(rnd.nextLong(0, Long.MAX_VALUE), 36);
    }

    private String text(int length) {
      StringBuilder sb = new StringBuilder(length);
      while (sb.length() < length) {
        sb.append(token()).append(' ');
      }
      sb.setLength(length);
      return sb.toString();
    }

    private String effects(int[] perTable) {
      StringBuilder sb = new StringBuilder("{");
      for (int i = 0; i < perTable.length; i++) {
        if (perTable[i] > 0) {
          if (sb.length() > 1) {
            sb.append(',');
          }
          sb.append('"').append(spec.tableName(i + 1)).append("\":").append(perTable[i]);
        }
      }
      return sb.append('}').toString();
    }
  }
}
