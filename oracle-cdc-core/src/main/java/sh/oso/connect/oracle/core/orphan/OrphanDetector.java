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
package sh.oso.connect.oracle.core.orphan;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer.OpenTransaction;
import sh.oso.connect.oracle.core.errors.OrphanTransactionException;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * CORE-TX-7 with the ADR-0006 safeguards. Every {@code interval}, buffered transactions older than
 * the interval are looked up in GV$TRANSACTION. One is released only after three negatives: absent
 * in two consecutive checks; the engine has mined past the SCN of the first absence without seeing
 * its COMMIT or ROLLBACK (the entry is still open, so no end row existed up to that SCN); and the
 * owning session, when the START row named one, is gone. Released keys go into a bounded ledger
 * carried in the position, and a later COMMIT for one of them is a stop.
 */
public class OrphanDetector {

  public enum Action {
    RELEASE,
    FAIL
  }

  /** A release decision with what the ops event reports. */
  public record Release(OpenTransaction tx, long absentAtScn, String reason) {}

  public static final int LEDGER_MAX = 256;

  private final TransactionProbe probe;
  private final Duration interval;
  private final Action action;
  private final Map<TxKey, Long> firstAbsence = new HashMap<>();
  private final LinkedHashSet<String> released = new LinkedHashSet<>();
  private Instant lastCheck;

  public OrphanDetector(
      TransactionProbe probe, Duration interval, Action action, Collection<String> released) {
    this.probe = probe;
    this.interval = interval;
    this.action = action;
    this.released.addAll(released);
  }

  /** A detector that never checks: for tests and fakes. */
  public static OrphanDetector disabled() {
    return new OrphanDetector(null, Duration.ofDays(36500), Action.RELEASE, List.of()) {
      @Override
      public List<Release> check(Instant now, long minedToScn, List<OpenTransaction> open) {
        return List.of();
      }
    };
  }

  public boolean due(Instant now) {
    return lastCheck == null || Duration.between(lastCheck, now).compareTo(interval) >= 0;
  }

  /** The released ledger, oldest first, bounded to {@link #LEDGER_MAX} entries. */
  public List<String> released() {
    return new ArrayList<>(released);
  }

  /** Adds a key the engine discarded by policy (CORE-TX-6) to the bounded ledger. */
  public void releasedByPolicy(TxKey key) {
    released.add(key.toString());
    while (released.size() > LEDGER_MAX) {
      released.remove(released.iterator().next());
    }
  }

  public boolean wasReleased(TxKey key) {
    return released.contains(key.toString());
  }

  /**
   * Runs one check when due; returns the transactions to release. With the fail action an orphan
   * stops the task instead.
   */
  public List<Release> check(Instant now, long minedToScn, List<OpenTransaction> open)
      throws SQLException {
    if (!due(now)) {
      return List.of();
    }
    lastCheck = now;
    List<OpenTransaction> candidates = new ArrayList<>();
    for (OpenTransaction t : open) {
      if (t.firstSeenAt() != null
          && Duration.between(t.firstSeenAt(), now).compareTo(interval) >= 0) {
        candidates.add(t);
      }
    }
    firstAbsence.keySet().retainAll(open.stream().map(OpenTransaction::key).toList());
    if (candidates.isEmpty()) {
      return List.of();
    }
    Set<TxKey> active = probe.activeTransactions();
    long scn = probe.currentScn();
    List<Release> releases = new ArrayList<>();
    for (OpenTransaction t : candidates) {
      if (active.contains(t.key())) {
        firstAbsence.remove(t.key());
        continue;
      }
      Long absentAt = firstAbsence.putIfAbsent(t.key(), scn);
      if (absentAt == null) {
        continue; // first negative; confirm next time
      }
      if (minedToScn < absentAt) {
        continue; // the end row may still be ahead of the cursor
      }
      if (t.sessionNo() > 0 && probe.sessionExists(t.sessionNo(), t.serialNo())) {
        continue; // the owning session is alive: GV$TRANSACTION may be lagging
      }
      String reason =
          "absent from GV$TRANSACTION at SCN "
              + absentAt
              + " and again at SCN "
              + scn
              + "; mined to SCN "
              + minedToScn
              + " without its COMMIT or ROLLBACK"
              + (t.sessionNo() > 0
                  ? "; session " + t.sessionNo() + "," + t.serialNo() + " gone"
                  : "; START row not mined, session unknown");
      if (action == Action.FAIL) {
        throw new OrphanTransactionException(
            "Transaction "
                + t.key()
                + " (user "
                + t.username()
                + ", "
                + t.events()
                + " events from SCN "
                + t.firstScn()
                + ") is orphaned: "
                + reason
                + ".",
            "The transaction is not open in the database and its end was never mined. Set"
                + " cdc.transaction.orphan.action=release to discard it with an ops event, or"
                + " reset the offsets past it.");
      }
      releases.add(new Release(t, absentAt, reason));
      firstAbsence.remove(t.key());
      released.add(t.key().toString());
      while (released.size() > LEDGER_MAX) {
        released.remove(released.iterator().next());
      }
    }
    return releases;
  }
}
