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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.doctor.RedoAvailability;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.core.snapshot.SnapshotProgress;
import sh.oso.connect.oracle.ops.OpsEvent;

/**
 * PRD-05 {@code offsets show} and {@code offsets set}. Setting goes through Kafka Connect's KIP-875
 * endpoint with the connector stopped, refuses an SCN whose redo is no longer all there, refuses to
 * move forward (which skips committed changes) without {@code --allow-skip}, and records the change
 * on the connector's ops topic before and after the PATCH, so no change goes unrecorded.
 */
public final class OffsetsAdmin {

  private static final ObjectMapper JSON =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private OffsetsAdmin() {}

  /** The decoded position as ordered fields, for text and JSON output. */
  static Map<String, Object> describe(Position p) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("resume_scn", p.resumeScn());
    m.put(
        "resume_redo_address",
        p.resumeRsId() == null ? null : p.resumeRsId() + " ssn " + p.resumeSsn());
    m.put("last_commit_scn", p.hasCommit() ? p.lastCommitScn() : null);
    m.put("last_commit_transaction", p.hasCommit() ? p.lastCommitKey().toString() : null);
    m.put("last_commit_thread", p.hasCommit() ? p.lastCommitThread() : null);
    m.put(
        "last_commit_redo_address",
        p.lastCommitRsId() == null ? null : p.lastCommitRsId() + " ssn " + p.lastCommitSsn());
    m.put("event_index", p.eventIndex());
    m.put("journal_generation", p.journalGeneration());
    m.put("schema_epoch", p.schemaEpoch());
    m.put("dbid", p.identity().dbid());
    m.put("resetlogs_scn", p.identity().resetlogsScn());
    m.put("released_transactions", p.released());
    m.put("snapshot", snapshot(p));
    if (!p.extras().isEmpty()) {
      m.put("other_fields", p.extras());
    }
    return m;
  }

  private static String snapshot(Position p) {
    SnapshotProgress s = SnapshotProgress.of(p.snapshot());
    if (s == null) {
      return "none started";
    }
    if (s.complete()) {
      return "complete";
    }
    Map<String, Object> tables = asMap(s.toMap().get("tables"));
    if (tables.isEmpty()) {
      return "in progress, no table started";
    }
    List<String> parts = new ArrayList<>();
    for (Map.Entry<String, Object> e : tables.entrySet()) {
      Map<String, Object> t = asMap(e.getValue());
      parts.add(
          e.getKey()
              + (Boolean.TRUE.equals(t.get("done"))
                  ? " done"
                  : " next chunk from " + t.getOrDefault("frontier", "the start")));
    }
    return "in progress: " + String.join("; ", parts);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object o) {
    return o instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
  }

  /** {@code offsets show}: prints the stored offset decoded; with {@code checkRedo} its redo. */
  public static int show(AdminSession s, PrintWriter out, boolean json, boolean checkRedo)
      throws IOException, SQLException {
    Map<String, Object> raw = s.storedOffset();
    Position p = raw == null || raw.isEmpty() ? null : PositionCodec.read(raw);
    String redo = null;
    if (p != null && checkRedo) {
      s.identity(p);
      RedoAvailability.Gap gap = s.redo().check(p.resumeScn());
      redo = gap == null ? "present" : "missing: " + gap.message();
    }
    if (json) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("connector", s.connector());
      m.put("partition", s.partition());
      m.put("offset", raw);
      m.put("position", p == null ? null : describe(p));
      if (redo != null) {
        m.put("redo_from_resume", redo);
      }
      out.println(JSON.writeValueAsString(m));
      return 0;
    }
    out.println("Connector " + s.connector() + ", partition " + s.partition());
    if (p == null) {
      out.println(
          "No stored offset: the connector starts from the current SCN, or its snapshot, when it"
              + " next runs.");
      return 0;
    }
    for (Map.Entry<String, Object> e : describe(p).entrySet()) {
      Object v = e.getValue();
      out.println(
          "  "
              + e.getKey()
              + ": "
              + (v == null ? "none" : v instanceof List<?> l && l.isEmpty() ? "none" : v));
    }
    if (redo != null) {
      out.println("  redo_from_resume: " + redo);
    }
    return 0;
  }

  /** The arguments of {@code offsets set}. */
  public record SetRequest(
      long scn, String reason, boolean allowSkip, List<String> forgetReleased, boolean resume) {}

  /** What {@code offsets set} would do from a stored position. */
  record SetPlan(Position next, String direction, Long previous) {}

  /** Checks the request against the stored position; refuses what it must not do. */
  static SetPlan plan(AdminSession s, SetRequest req, Position current) throws SQLException {
    DatabaseIdentity identity = s.identity(current);
    long now = s.db().catalog().currentScn();
    if (req.scn() > now) {
      throw new AdminException(
          "SCN " + req.scn() + " is ahead of the database's current SCN " + now + ".");
    }
    RedoAvailability.Gap gap = s.redo().check(req.scn());
    if (gap != null) {
      throw new AdminException(
          "Refused: the redo from SCN "
              + req.scn()
              + " is not all present ("
              + gap.code()
              + "), so the connector would stop there. "
              + gap.message()
              + " To recover tables whose redo is gone, use oracle-cdc-admin resnapshot.");
    }
    String direction;
    if (current == null) {
      direction = "initial";
    } else if (req.scn() > current.resumeScn()) {
      direction = "forward";
    } else if (req.scn() < current.resumeScn()) {
      direction = "backward";
    } else {
      direction = "same";
    }
    if ("forward".equals(direction) && !req.allowSkip()) {
      throw new AdminException(
          "Refused: moving the position forward from SCN "
              + current.resumeScn()
              + " to "
              + req.scn()
              + " skips every change committed between them. Pass --allow-skip to do it anyway"
              + " (the ops topic records it), or use oracle-cdc-admin resnapshot for the tables"
              + " concerned.");
    }
    List<String> released =
        current == null ? new ArrayList<>() : new ArrayList<>(current.released());
    for (String k : req.forgetReleased()) {
      if (!released.remove(k)) {
        throw AdminException.usage(
            "--forget-released "
                + k
                + " is not in the offset's released transactions "
                + (current == null ? "[]" : current.released())
                + ".");
      }
    }
    Position next =
        current == null
            ? Position.initial(req.scn(), identity)
            : current.withResumeScn(req.scn()).withReleased(released);
    return new SetPlan(next, direction, current == null ? null : current.resumeScn());
  }

  /**
   * {@code offsets set}: checks the request, stops the connector, checks again against the offset
   * as it stands once the task can no longer commit, then records, patches and records again.
   */
  public static int set(AdminSession s, AdminRecords records, PrintWriter out, SetRequest req)
      throws Exception {
    if (req.reason() == null || req.reason().isBlank()) {
      throw AdminException.usage("--reason is required and cannot be blank.");
    }
    if (req.scn() <= 0) {
      throw AdminException.usage("--scn must be a positive SCN.");
    }
    var kafka = s.kafka("offsets set records the change on the ops topic and");
    plan(s, req, s.storedPosition());
    s.ensureStopped(out);
    SetPlan plan;
    try {
      plan = plan(s, req, s.storedPosition());
    } catch (AdminException e) {
      throw new AdminException(
          e.exitCode(),
          e.getMessage() + " (The offset changed while the connector stopped; it stays stopped.)",
          e);
    }
    Map<String, String> details = new LinkedHashMap<>();
    details.put("connector", s.connector());
    details.put("command", "offsets-set");
    details.put("reason", req.reason());
    details.put("operator", s.env().operator());
    details.put("direction", plan.direction());
    details.put(
        "previous_resume_scn", plan.previous() == null ? "none" : Long.toString(plan.previous()));
    details.put("new_resume_scn", Long.toString(req.scn()));
    if (!req.forgetReleased().isEmpty()) {
      details.put("forgotten_released", String.join(",", req.forgetReleased()));
    }
    apply(s, records, kafka, details, req.scn(), plan.next());
    out.println(
        "Offset of "
            + s.connector()
            + " set to resume at SCN "
            + req.scn()
            + " ("
            + plan.direction()
            + "); the change is recorded on "
            + records.opsTopic()
            + ".");
    finish(s, out, req.resume());
    return 0;
  }

  /**
   * Records the change, patches the offset, and records the outcome. The first event goes out
   * before the PATCH, so a change is never left unrecorded; a failed PATCH is recorded as failed.
   */
  static void apply(
      AdminSession s,
      AdminRecords records,
      sh.oso.connect.oracle.doctor.kafka.KafkaPort kafka,
      Map<String, String> details,
      long resumeScn,
      Position next)
      throws Exception {
    long ts = s.env().clock().millis();
    Map<String, String> applying = new LinkedHashMap<>(details);
    applying.put("outcome", "applying");
    records.ops(kafka, new OpsEvent(OpsEvent.Type.OFFSETS_SET, ts, resumeScn, applying));
    try {
      s.api().patchOffset(s.connector(), s.partition(), PositionCodec.write(next));
    } catch (IOException e) {
      Map<String, String> failed = new LinkedHashMap<>(details);
      failed.put("outcome", "failed");
      failed.put("error", e.getMessage());
      records.ops(
          kafka,
          new OpsEvent(OpsEvent.Type.OFFSETS_SET, s.env().clock().millis(), resumeScn, failed));
      throw new AdminException(
          AdminException.REFUSED, "The offset was not changed: " + e.getMessage(), e);
    }
    Map<String, String> applied = new LinkedHashMap<>(details);
    applied.put("outcome", "applied");
    records.ops(
        kafka,
        new OpsEvent(OpsEvent.Type.OFFSETS_SET, s.env().clock().millis(), resumeScn, applied));
  }

  static void finish(AdminSession s, PrintWriter out, boolean resume) throws IOException {
    if (resume) {
      s.api().resume(s.connector());
      out.println("Connector " + s.connector() + " resumed.");
    } else {
      out.println(
          "Connector "
              + s.connector()
              + " is stopped; resume it with PUT /connectors/"
              + s.connector()
              + "/resume.");
    }
  }
}
