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
package sh.oso.connect.oracle.core.mining.step;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import sh.oso.connect.oracle.core.errors.ErrorCode;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.mining.event.EventCursor;
import sh.oso.connect.oracle.core.mining.event.EventSource;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RedoRecordId;

/**
 * Runs one planned step as a staged unit (ADR-0005): events are collected, never applied, until the
 * cursor is exhausted. A step-retry error or a timeout discards everything; a DDL that changes the
 * pushed-down ids cuts the step after that DDL; MISSING_SCN stops the task.
 */
public final class StepRunner {

  private final OraErrorClassifier classifier;
  private final DdlStepCut cut;

  public StepRunner(OraErrorClassifier classifier, DdlStepCut cut) {
    this.classifier = classifier;
    this.cut = cut;
  }

  public StepOutcome run(EventSource source, StepCursor from, long endScn) {
    List<MiningEvent> staged = new ArrayList<>();
    int rows = 0;
    Map<Integer, RedoRecordId> last = new TreeMap<>(from.marks()); // per thread (ADR-0026)
    try (EventCursor c = source.open(from, endScn)) {
      while (c.next()) {
        rows++;
        MiningEvent e = c.event();
        if (e.id().zeroRsId()) {
          // no redo byte address: the query returns these rows from the last applied SCN on, so
          // one may come back in a later step; only rows that are safe to apply again are expected
          if (!(e instanceof MiningEvent.TxStart
              || e instanceof MiningEvent.Commit
              || e instanceof MiningEvent.Rollback
              || e instanceof MiningEvent.Other)) {
            throw new OracleCdcCorruptionException(
                "LogMiner returned "
                    + e.getClass().getSimpleName()
                    + " at SCN "
                    + e.id().scn()
                    + " with an all-zero RS_ID; a data row without a redo byte address cannot be"
                    + " placed in redo order, so it could be applied twice or not at all.",
                "Report it in a GitHub issue with the SCN and the Oracle release update. Restarting"
                    + " the task mines the step again from its last position.");
          }
          staged.add(e); // never the cursor: it names no place in the log
          continue;
        }
        if (from.alreadyApplied(e.thread(), e.id())) {
          continue;
        }
        RedoRecordId mark =
            last.containsKey(e.thread()) ? last.get(e.thread()) : from.markFor(e.thread());
        if (mark == null || e.id().compareTo(mark) > 0) {
          // the cursor advances by redo byte address, not by SCN (ADR-0014), and only within the
          // row's own thread: another thread's addresses are not comparable (ADR-0026)
          last.put(e.thread(), e.id());
        }
        if (e instanceof MiningEvent.MissingScn m) {
          throw new OracleCdcCorruptionException(
              "LogMiner reported MISSING_SCN at "
                  + m.id()
                  + (m.info() == null ? "" : ": " + m.info()),
              "Redo is missing or corrupt in this range; restore the archived logs and restart, or"
                  + " resnapshot the affected tables.");
        }
        staged.add(e);
        if (e instanceof MiningEvent.Ddl d && cut.requiresCut(d)) {
          Map<Integer, RedoRecordId> atCut = new TreeMap<>(last);
          atCut.put(d.thread(), d.id());
          return new StepOutcome(
              StepOutcome.Kind.CUT, staged, from.after(d.scn(), atCut), rows, null);
        }
      }
      return new StepOutcome(
          StepOutcome.Kind.COMPLETE, staged, from.after(endScn, last), rows, null);
    } catch (OracleCdcException e) {
      throw e;
    } catch (SQLException e) {
      if (e instanceof SQLTimeoutException || OraErrorClassifier.oraCode(e) == 1013) {
        return new StepOutcome(StepOutcome.Kind.TIMEOUT, List.of(), from, rows, e);
      }
      ErrorCode code = classifier.classify(e);
      if (code == ErrorCode.MINING_STEP_RETRY) {
        return new StepOutcome(StepOutcome.Kind.RETRY, List.of(), from, rows, e);
      }
      throw classifier.toException(e, "mining " + from.scn() + ".." + endScn);
    }
  }
}
