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
package sh.oso.connect.oracle.core.topology;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.errors.TopologyException;

/**
 * ADR-0023 and ADR-0025: the database shapes this release mines. The task checks the topology
 * before it reads an offset, the doctor reports the same check as DOC-13, and the engine stops on
 * redo from a thread the start did not qualify. The redo byte address cursor (ADR-0014) and the
 * commit order are per thread, so a second enabled redo thread (RAC) would be skipped or repeated
 * by the one cursor this release keeps; it is refused until the per-thread position exists
 * (CORE-POS-4).
 */
public final class TopologyGuard {

  private TopologyGuard() {}

  /** Stops a start, or a reconnect, on a shape this release does not capture. */
  public static void requireQualified(Topology topology, CaptureMode mode) {
    String refusal = roleRefusal(topology.database(), mode);
    if (refusal != null) {
      throw new TopologyException(
          refusal,
          mode == CaptureMode.ONLINE
              ? "Point the connector at the primary, or use cdc.capture.mode=archive_only on an"
                  + " Active Data Guard standby open read-only (ADR-0025)."
              : "Open the standby read-only (Active Data Guard), or point the connector at the"
                  + " primary (ADR-0025).");
    }
    requireSingleThread(topology);
  }

  /**
   * ADR-0025: why a database in this role and open mode cannot be mined in {@code mode}, or null
   * when it can. Online mode adds online logs and reads the dictionary as of now, so it needs the
   * primary open read-write. Archive-only mode also accepts a physical standby open read-only: its
   * redo is the primary's, block for block, with the same SCNs. A mounted database has no
   * dictionary to read, and a logical or snapshot standby has redo and SCNs of its own.
   */
  public static String roleRefusal(DatabaseInfo db, CaptureMode mode) {
    String role = db.databaseRole() == null ? "" : db.databaseRole().trim();
    String open = db.openMode() == null ? "" : db.openMode().trim();
    boolean primary = "PRIMARY".equals(role) && "READ WRITE".equals(open);
    if (mode == CaptureMode.ONLINE) {
      return primary
          ? null
          : "cdc.capture.mode=online needs a PRIMARY database open READ WRITE; this one is "
              + role
              + " "
              + open
              + ".";
    }
    if (primary) {
      return null;
    }
    if ("PHYSICAL STANDBY".equals(role) && open.startsWith("READ ONLY")) {
      return null;
    }
    if (open.startsWith("MOUNTED")) {
      return "The database is "
          + role
          + " "
          + open
          + ": a mounted database has no dictionary to read, so LogMiner cannot decode its redo."
          + " Archive-only capture needs a physical standby open READ ONLY.";
    }
    return "The database is "
        + role
        + " "
        + open
        + ". Archive-only capture reads a primary or a physical standby; a logical or snapshot"
        + " standby writes redo and SCNs of its own.";
  }

  /** ADR-0025: a physical standby open READ ONLY without redo apply; the safe end stays put. */
  public static boolean applyStopped(DatabaseInfo db) {
    return "PHYSICAL STANDBY".equals(db.databaseRole() == null ? "" : db.databaseRole().trim())
        && "READ ONLY".equals(db.openMode() == null ? "" : db.openMode().trim());
  }

  private static void requireSingleThread(Topology topology) {
    Set<Integer> enabled = qualifiedThreads(topology);
    if (enabled.size() > 1) {
      throw new TopologyException(
          "The database has "
              + enabled.size()
              + " enabled redo threads ("
              + describe(topology.threads())
              + "). This release captures a single redo thread: with one cursor the connector"
              + " would skip or repeat whole threads.",
          "Run the connector against a single-instance database. RAC capture needs the per-thread"
              + " position (ADR-0023).");
    }
  }

  /** The redo threads a qualified start mines: every enabled one. */
  public static Set<Integer> qualifiedThreads(Topology topology) {
    return topology.threads().stream()
        .filter(ThreadInfo::enabled)
        .map(ThreadInfo::thread)
        .collect(Collectors.toUnmodifiableSet());
  }

  /** {@code thread 1 OPEN, thread 2 disabled}: one entry per V$THREAD row. */
  public static String describe(List<ThreadInfo> threads) {
    return threads.stream()
        .map(t -> "thread " + t.thread() + " " + (t.enabled() ? t.status() : "disabled"))
        .collect(Collectors.joining(", "));
  }
}
