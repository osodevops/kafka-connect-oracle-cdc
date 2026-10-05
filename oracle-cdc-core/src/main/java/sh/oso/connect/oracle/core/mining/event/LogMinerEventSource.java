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
package sh.oso.connect.oracle.core.mining.event;

import java.sql.SQLException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.logs.LogSet;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.LogMinerSource;
import sh.oso.connect.oracle.core.mining.MiningFilter;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;

/**
 * The real {@link EventSource}: inventory for the range, session log set, START_LOGMNR, filtered
 * query, row adapter. One step at a time; scheduling, retries and the DDL step cut sit above it.
 */
public final class LogMinerEventSource implements EventSource {

  private final LogInventory inventory;
  private final LogMinerSource session;
  private volatile MiningFilter filter;
  private volatile LogMinerRowAdapter adapter;
  private final DictionaryMode mode;

  public LogMinerEventSource(
      LogInventory inventory,
      LogMinerSource session,
      ResolvedObjects objects,
      MiningFilter filter,
      DictionaryMode mode) {
    this.inventory = inventory;
    this.session = session;
    this.filter = filter;
    this.adapter = new LogMinerRowAdapter(objects.byObject());
    this.mode = mode;
  }

  @Override
  public EventCursor open(sh.oso.connect.oracle.core.mining.step.StepCursor from, long endScn)
      throws SQLException {
    LogSet logs = inventory.forRange(from.scn(), endScn);
    session.setLogs(logs.logs());
    session.start(startScn(from, logs), endScn, mode);
    adapter.reset();
    return adapter.adapt(session.query(filter, from, endScn));
  }

  /**
   * ADR-0014: LogMiner's STARTSCN must not exclude redo bound late with an earlier SCN, so with a
   * redo byte address it is the start of the log holding the cursor rather than the cursor's SCN.
   */
  private static long startScn(
      sh.oso.connect.oracle.core.mining.step.StepCursor from, LogSet logs) {
    long startScn = from.scn();
    for (sh.oso.connect.oracle.core.logs.RedoLog l : logs.logs()) {
      if (l.firstScn() <= from.scn() && l.firstScn() < startScn) {
        startScn = l.firstScn();
      }
    }
    return from.hasRba() ? startScn : from.scn();
  }

  @Override
  public EventSource redoDictionary() {
    return new EventSource() {
      @Override
      public EventCursor open(sh.oso.connect.oracle.core.mining.step.StepCursor from, long endScn)
          throws SQLException {
        long start = startScn(from, inventory.forRange(from.scn(), endScn));
        sh.oso.connect.oracle.core.logs.DictionaryBuild build =
            inventory
                .dictionaryBuildBefore(start)
                .orElseThrow(
                    () ->
                        new sh.oso.connect.oracle.core.errors.DictionaryUnavailableException(
                            "No dictionary build in the archived logs ends before SCN "
                                + start
                                + ", so redo written before a DDL cannot be decoded.",
                            DICTIONARY_ACTION));
        LogSet logs;
        try {
          logs = inventory.forRange(build.firstScn(), endScn);
        } catch (sh.oso.connect.oracle.core.errors.OracleCdcPurgedException e) {
          throw new sh.oso.connect.oracle.core.errors.DictionaryUnavailableException(
              "The dictionary build starting at SCN "
                  + build.firstScn()
                  + " cannot be used: "
                  + e.getMessage(),
              DICTIONARY_ACTION,
              e);
        }
        session.setLogs(logs.logs());
        session.start(start, endScn, DictionaryMode.REDO_LOGS_WITH_DDL_TRACKING);
        adapter.reset();
        return adapter.adapt(session.query(filter, from, endScn));
      }

      @Override
      public void close() {}
    };
  }

  /** What the operator does when no dictionary build covers a lag case (CDC-6001). */
  public static final String DICTIONARY_ACTION =
      "Grant EXECUTE ON DBMS_LOGMNR_D to the connector user so it builds dictionaries"
          + " (cdc.dictionary.build.interval.ms), and keep the archived logs from the last build."
          + " To go on now, move the offset past the DDL; the table's rows in between are not"
          + " delivered.";

  /** Swaps the pushed-down ids after a DDL step cut (ADR-0001). */
  public void update(ResolvedObjects objects, MiningFilter newFilter) {
    this.filter = newFilter;
    this.adapter = new LogMinerRowAdapter(objects.byObject());
  }

  @Override
  public void recycle() throws SQLException {
    session.reset();
  }

  @Override
  public void close() throws SQLException {
    session.close();
  }
}
