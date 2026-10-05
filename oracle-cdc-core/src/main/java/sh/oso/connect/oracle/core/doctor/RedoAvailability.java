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
package sh.oso.connect.oracle.core.doctor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.errors.OracleCdcGapException;
import sh.oso.connect.oracle.core.errors.OracleCdcPurgedException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.topology.CatalogSource;

/**
 * Whether the redo from an SCN onward is still all there, with the same log selection and
 * continuity checks the engine runs before every step ({@link LogInventory}), and where the first
 * continuous stretch after a gap begins. The admin commands use it to refuse an offset whose redo
 * is purged and to find the position past a gap for a recovery resnapshot (PRD-02 SNAP-7).
 */
public final class RedoAvailability {

  /** Missing redo: the error code the engine would stop with and its message. */
  public record Gap(String code, String message) {}

  private final CatalogSource catalog;
  private final CaptureMode mode;
  private final int destId;
  private final LogInventory inventory;

  public RedoAvailability(CatalogSource catalog, CaptureMode mode, int destId) {
    this.catalog = catalog;
    this.mode = mode;
    this.destId = destId;
    this.inventory = new LogInventory(catalog, mode, destId);
  }

  /** Null when every log from {@code scn} to the end the engine could mine now is present. */
  public Gap check(long scn) throws SQLException {
    long end =
        mode == CaptureMode.ONLINE ? catalog.currentScn() : inventory.archiveOnlySafeEnd(scn);
    try {
      inventory.forRange(scn, Math.max(scn, end));
      return null;
    } catch (OracleCdcPurgedException | OracleCdcGapException e) {
      return new Gap(e.code().code(), e.getMessage());
    }
  }

  /**
   * The lowest SCN at or above {@code scn} from which every log is present, or -1 when no such
   * point exists. Candidates are the first SCNs of the logs present, starting after the newest log
   * the catalog marks deleted.
   */
  public long firstAvailableFrom(long scn) throws SQLException {
    long current = catalog.currentScn();
    List<RedoLog> logs = new ArrayList<>(catalog.archivedLogs(scn, current, destId));
    if (mode == CaptureMode.ONLINE) {
      logs.addAll(catalog.onlineLogs());
    }
    long floor = scn;
    for (RedoLog l : logs) {
      if (l.purgedInCatalog() && l.nextScn() > floor) {
        floor = l.nextScn();
      }
    }
    TreeSet<Long> candidates = new TreeSet<>();
    if (floor == scn) {
      candidates.add(scn);
    }
    for (RedoLog l : logs) {
      if (!l.purgedInCatalog() && l.firstScn() >= floor) {
        candidates.add(l.firstScn());
      }
    }
    for (long c : candidates) {
      if (check(c) == null) {
        return c;
      }
    }
    return -1;
  }
}
