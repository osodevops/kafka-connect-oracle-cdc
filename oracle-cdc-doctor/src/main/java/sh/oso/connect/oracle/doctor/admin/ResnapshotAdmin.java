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
package sh.oso.connect.oracle.doctor.admin;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.doctor.CapturedTable;
import sh.oso.connect.oracle.core.doctor.RedoAvailability;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.doctor.kafka.KafkaPort;

/**
 * PRD-05 {@code resnapshot} (PRD-02 SNAP-7). Always writes a snapshot signal for the named tables
 * to the signals topic. When the stored offset points into redo that is no longer all there, it
 * also moves the offset to the first SCN after the gap, which skips the gap's changes: for the
 * named tables the snapshot restores them, and for every other captured table the command refuses
 * unless the operator says the gap is acceptable for them too. The signal goes out before the
 * offset moves, and the move is recorded on the ops topic before and after.
 */
public final class ResnapshotAdmin {

  private ResnapshotAdmin() {}

  /** The arguments of {@code resnapshot}. */
  public record Request(
      List<String> tables, String reason, boolean skipGapForUnlisted, boolean resume) {}

  public static int run(AdminSession s, AdminRecords records, PrintWriter out, Request req)
      throws Exception {
    if (req.reason() == null || req.reason().isBlank()) {
      throw AdminException.usage("--reason is required and cannot be blank.");
    }
    if (req.tables().isEmpty()) {
      throw AdminException.usage("--tables needs at least one table.");
    }
    KafkaPort kafka = s.kafka("resnapshot writes a signal and");
    List<CapturedTable> captured =
        s.db()
            .catalog()
            .capturedTables(
                patterns(s.config().tablesInclude(), s.config().tablesCaseSensitive()),
                patterns(s.config().tablesExclude(), s.config().tablesCaseSensitive()),
                s.config().core().pdbs());
    List<String> names = new ArrayList<>();
    for (String wanted : req.tables()) {
      String match =
          captured.stream()
              .map(CapturedTable::fqn)
              .filter(f -> f.equalsIgnoreCase(wanted.trim()))
              .findFirst()
              .orElseThrow(
                  () ->
                      AdminException.usage(
                          wanted
                              + " is not one of the "
                              + captured.size()
                              + " tables the connector captures; name tables as PDB.OWNER.TABLE"
                              + " in a CDB and OWNER.TABLE otherwise."));
      if (!names.contains(match)) {
        names.add(match);
      }
    }
    Position current = s.storedPosition();
    Plan plan = plan(s, req, names, captured, current);
    if (plan == null) {
      String id = records.snapshot(kafka, names);
      out.println(
          "Signal "
              + id
              + " written to "
              + records.signalsTopic()
              + ": snapshot of "
              + String.join(", ", names)
              + ". The redo from the stored position is present, so the offset is unchanged;"
              + " the connector acts on the signal when it runs and acknowledges it on "
              + records.opsTopic()
              + ".");
      return 0;
    }
    s.ensureStopped(out);
    // the task can no longer commit: plan again from the offset as it now stands
    current = s.storedPosition();
    plan = plan(s, req, names, captured, current);
    if (plan == null) {
      String id = records.snapshot(kafka, names);
      out.println(
          "The offset moved while the connector stopped and its redo is now present, so the"
              + " offset is unchanged. Signal "
              + id
              + " asks for a snapshot of "
              + String.join(", ", names)
              + ".");
      OffsetsAdmin.finish(s, out, req.resume());
      return 0;
    }
    RedoAvailability.Gap gap = plan.gap();
    long past = plan.past();
    List<String> unlisted = plan.unlisted();
    Map<String, String> details = new LinkedHashMap<>();
    details.put("connector", s.connector());
    details.put("command", "resnapshot");
    details.put("reason", req.reason());
    details.put("operator", s.env().operator());
    details.put("direction", "forward");
    details.put("previous_resume_scn", Long.toString(current.resumeScn()));
    details.put("new_resume_scn", Long.toString(past));
    details.put("gap", gap.code() + " " + gap.message());
    details.put("tables", String.join(",", names));
    if (!unlisted.isEmpty()) {
      details.put("unlisted_tables", String.join(",", unlisted));
    }
    // the signal first: if the offset change then fails, a snapshot only repeats rows
    String id = records.snapshot(kafka, names);
    details.put("signal_id", id);
    OffsetsAdmin.apply(s, records, kafka, details, past, current.withResumeScn(past));
    out.println(
        "The redo from SCN "
            + current.resumeScn()
            + " is gone ("
            + gap.code()
            + "). Signal "
            + id
            + " asks for a snapshot of "
            + String.join(", ", names)
            + ", and the offset now resumes at SCN "
            + past
            + ", the first SCN with every log present. Both are recorded on "
            + records.opsTopic()
            + ".");
    OffsetsAdmin.finish(s, out, req.resume());
    return 0;
  }

  /** The gap and the position past it; null when the redo from the stored position is present. */
  record Plan(RedoAvailability.Gap gap, long past, List<String> unlisted) {}

  static Plan plan(
      AdminSession s,
      Request req,
      List<String> names,
      List<CapturedTable> captured,
      Position current)
      throws java.sql.SQLException {
    if (current == null) {
      return null;
    }
    s.identity(current);
    RedoAvailability redo = s.redo();
    RedoAvailability.Gap gap = redo.check(current.resumeScn());
    if (gap == null) {
      return null;
    }
    long past = redo.firstAvailableFrom(current.resumeScn());
    if (past < 0) {
      throw new AdminException(
          "Refused: the redo from SCN "
              + current.resumeScn()
              + " is not all present ("
              + gap.code()
              + ") and no later SCN has every log from it to now. "
              + gap.message());
    }
    List<String> unlisted = new ArrayList<>();
    for (CapturedTable t : captured) {
      if (!names.contains(t.fqn())) {
        unlisted.add(t.fqn());
      }
    }
    if (!unlisted.isEmpty() && !req.skipGapForUnlisted()) {
      throw new AdminException(
          "Refused: the stored position (SCN "
              + current.resumeScn()
              + ") points into redo that is gone ("
              + gap.code()
              + "), and moving it to SCN "
              + past
              + " skips the gap's changes for every captured table. These captured tables are not"
              + " in --tables, so their changes in the gap would be lost: "
              + String.join(", ", unlisted)
              + ". Add them to --tables, or pass --skip-gap-for-unlisted-tables if they had no"
              + " changes in the gap.");
    }
    return new Plan(gap, past, unlisted);
  }

  private static List<Pattern> patterns(List<String> regexes, boolean caseSensitive) {
    List<Pattern> out = new ArrayList<>();
    for (String r : regexes) {
      if (!r.isBlank()) {
        out.add(
            caseSensitive
                ? Pattern.compile(r.trim())
                : Pattern.compile(r.trim(), Pattern.CASE_INSENSITIVE));
      }
    }
    return out;
  }
}
