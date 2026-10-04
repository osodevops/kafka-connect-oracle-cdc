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

/**
 * Runs a rule set and collects findings; a rule that throws becomes a BLOCKING finding of its own.
 */
public final class Doctor {

  private final List<Rule> rules;

  public Doctor(List<Rule> rules) {
    this.rules = rules;
  }

  public Report run(DoctorContext ctx) {
    List<Finding> findings = new ArrayList<>();
    for (Rule r : rules) {
      try {
        findings.addAll(r.evaluate(ctx));
      } catch (SQLException | RuntimeException e) {
        findings.add(
            Finding.blocking(r.id(), "Rule " + r.id() + " could not run: " + e.getMessage(), null));
      }
    }
    return new Report(findings);
  }
}
