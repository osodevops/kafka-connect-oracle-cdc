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
import sh.oso.connect.oracle.core.errors.ErrorCode;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.mining.event.EventCursor;
import sh.oso.connect.oracle.core.mining.event.EventSource;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;

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
    try (EventCursor c = source.open(from.scn(), endScn)) {
      while (c.next()) {
        rows++;
        MiningEvent e = c.event();
        if (from.alreadyApplied(e.id())) {
          continue;
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
          return new StepOutcome(
              StepOutcome.Kind.CUT, staged, new StepCursor(d.scn(), d.id()), rows, null);
        }
      }
      return new StepOutcome(StepOutcome.Kind.COMPLETE, staged, StepCursor.at(endScn), rows, null);
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
