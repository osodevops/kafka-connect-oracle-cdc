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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * PRD-05 {@code redo-profile}: archive generation per hour and thread from V$ARCHIVED_LOG, and a
 * LogMiner sample with no table filter broken down by owner, table and operation, with
 * truncate-and-reload patterns flagged and the share of mined rows the connector's filter would
 * discard. Pure arithmetic over what the catalog and the sampler return.
 */
public final class RedoProfile {

  /** A table with at least this many inserts in the sample can be a reload. */
  public static final long RELOAD_MIN_ROWS = 1000;

  private RedoProfile() {}

  /** One aggregate row of the LogMiner sample. */
  public record SampleRow(
      String container, String owner, String table, String operation, long rows, long truncates) {}

  /** Logs switched out and bytes archived in one UTC hour on one thread. */
  public record HourRate(Instant hour, int thread, int logs, long bytes) {}

  /** One table's rows in the sample. {@code pattern} is null or a plain-English flag. */
  public record TableProfile(
      String name,
      boolean captured,
      long inserts,
      long updates,
      long deletes,
      long ddl,
      long truncates,
      long other,
      String pattern) {
    public long total() {
      return inserts + updates + deletes + ddl + other;
    }
  }

  /** The sample: its range, the logs mined and the rows found. */
  public record Sample(
      long startScn,
      long endScn,
      int logs,
      long bytes,
      long totalRows,
      long controlRows,
      long unattributedRows,
      long capturedRows,
      List<TableProfile> tables) {

    /** Rows that name a table. */
    public long tableRows() {
      return tables.stream().mapToLong(TableProfile::total).sum();
    }

    /** Share of the rows naming a table that belong to tables the connector does not capture. */
    public double discardShare() {
      long rows = tableRows();
      return rows == 0 ? 0 : (rows - capturedRows) / (double) rows;
    }
  }

  /** Archive generation by hour and thread over {@code [from, to)}. */
  public static List<HourRate> hourly(List<ArchiveStat> history, Instant from, Instant to) {
    Map<Instant, Map<Integer, long[]>> m = new TreeMap<>();
    for (ArchiveStat a : history) {
      if (a.nextTime() == null || a.nextTime().isBefore(from) || !a.nextTime().isBefore(to)) {
        continue;
      }
      long[] v =
          m.computeIfAbsent(a.nextTime().truncatedTo(ChronoUnit.HOURS), h -> new TreeMap<>())
              .computeIfAbsent(a.thread(), t -> new long[2]);
      v[0]++;
      v[1] += a.bytes();
    }
    List<HourRate> out = new ArrayList<>();
    for (Map.Entry<Instant, Map<Integer, long[]>> h : m.entrySet()) {
      for (Map.Entry<Integer, long[]> t : h.getValue().entrySet()) {
        out.add(new HourRate(h.getKey(), t.getKey(), (int) t.getValue()[0], t.getValue()[1]));
      }
    }
    return out;
  }

  /**
   * The newest {@code count} archived logs still present: the SCN range a sample mines. Returns
   * {@code [first SCN, last next SCN - 1]}, or null when no log is present.
   */
  public static long[] sampleRange(List<ArchiveStat> history, int count) {
    List<ArchiveStat> present =
        history.stream()
            .filter(a -> !a.deleted() && a.nextTime() != null)
            // newest by SCN: NEXT_TIME has whole seconds, and logs switched within one second tie
            .sorted(
                Comparator.comparingLong(ArchiveStat::nextScn)
                    .thenComparingInt(ArchiveStat::thread)
                    .reversed())
            .limit(Math.max(1, count))
            .toList();
    if (present.isEmpty()) {
      return null;
    }
    long first = present.stream().mapToLong(ArchiveStat::firstScn).min().orElseThrow();
    long next = present.stream().mapToLong(ArchiveStat::nextScn).max().orElseThrow();
    return new long[] {first, next - 1};
  }

  /**
   * Builds the sample view. Table names are {@code CONTAINER.OWNER.TABLE} in a CDB and {@code
   * OWNER.TABLE} otherwise, matched against the connector's include and exclude patterns as the
   * connector matches them (whole name, case-insensitive).
   */
  public static Sample sample(
      List<SampleRow> rows,
      boolean cdb,
      List<String> include,
      List<String> exclude,
      long startScn,
      long endScn,
      int logs,
      long bytes) {
    List<Pattern> in =
        include.stream().map(p -> Pattern.compile(p, Pattern.CASE_INSENSITIVE)).toList();
    List<Pattern> ex =
        exclude.stream().map(p -> Pattern.compile(p, Pattern.CASE_INSENSITIVE)).toList();
    Map<String, long[]> byTable = new LinkedHashMap<>();
    long total = 0;
    long control = 0;
    long unattributed = 0;
    for (SampleRow r : rows) {
      total += r.rows();
      String op = r.operation() == null ? "" : r.operation().toUpperCase(Locale.ROOT);
      if (op.equals("START") || op.equals("COMMIT") || op.equals("ROLLBACK")) {
        control += r.rows();
        continue;
      }
      if (r.owner() == null || r.table() == null) {
        unattributed += r.rows();
        continue;
      }
      String name =
          (cdb && r.container() != null ? r.container() + "." : "") + r.owner() + "." + r.table();
      long[] v = byTable.computeIfAbsent(name, n -> new long[6]);
      if (op.contains("INSERT")) {
        v[0] += r.rows();
      } else if (op.equals("UPDATE")) {
        v[1] += r.rows();
      } else if (op.equals("DELETE")) {
        v[2] += r.rows();
      } else if (op.equals("DDL")) {
        v[3] += r.rows();
        v[4] += r.truncates();
      } else {
        v[5] += r.rows();
      }
    }
    List<TableProfile> tables = new ArrayList<>();
    long captured = 0;
    for (Map.Entry<String, long[]> e : byTable.entrySet()) {
      String name = e.getKey();
      boolean cap =
          (in.isEmpty() || in.stream().anyMatch(p -> p.matcher(name).matches()))
              && ex.stream().noneMatch(p -> p.matcher(name).matches());
      long[] v = e.getValue();
      TableProfile t = new TableProfile(name, cap, v[0], v[1], v[2], v[3], v[4], v[5], pattern(v));
      if (cap) {
        captured += t.total();
      }
      tables.add(t);
    }
    tables.sort(Comparator.comparingLong(TableProfile::total).reversed());
    return new Sample(
        startScn, endScn, logs, bytes, total, control, unattributed, captured, tables);
  }

  private static String pattern(long[] v) {
    long inserts = v[0];
    long deletes = v[2];
    long truncates = v[4];
    if (truncates > 0 && inserts >= RELOAD_MIN_ROWS) {
      return "truncate and reload";
    }
    if (deletes >= RELOAD_MIN_ROWS
        && inserts >= RELOAD_MIN_ROWS
        && Math.min(deletes, inserts) >= 0.8 * Math.max(deletes, inserts)) {
      return "delete and reload";
    }
    return null;
  }

  /** The Markdown report. */
  public static String toMarkdown(
      Instant from, Instant to, List<HourRate> hours, Sample sample, int top) {
    StringBuilder sb = new StringBuilder("# oracle-cdc-doctor redo profile\n\n");
    sb.append("## Archive generation per hour\n\n")
        .append("Logs switched out from ")
        .append(from.truncatedTo(ChronoUnit.SECONDS))
        .append(" to ")
        .append(to.truncatedTo(ChronoUnit.SECONDS))
        .append(" (UTC), from V$ARCHIVED_LOG.\n\n");
    if (hours.isEmpty()) {
      sb.append("No log was archived in the window.\n");
    } else {
      sb.append("| Hour (UTC) | Thread | Logs | Archived |\n|---|---|---|---|\n");
      for (HourRate h : hours) {
        sb.append("| ")
            .append(h.hour())
            .append(" | ")
            .append(h.thread())
            .append(" | ")
            .append(h.logs())
            .append(" | ")
            .append(Sizing.bytes(h.bytes()))
            .append(" |\n");
      }
    }
    sb.append("\n## Redo by table (LogMiner sample)\n\n");
    if (sample == null) {
      return sb.append("No archived log is present to sample.\n").toString();
    }
    sb.append("Mined SCN ")
        .append(sample.startScn())
        .append(" to ")
        .append(sample.endScn())
        .append(" in ")
        .append(sample.logs())
        .append(sample.logs() == 1 ? " log" : " logs")
        .append(" (")
        .append(Sizing.bytes(sample.bytes()))
        .append(") with no table filter: ")
        .append(sample.totalRows())
        .append(" rows, of which ")
        .append(sample.controlRows())
        .append(" are transaction control rows and ")
        .append(sample.unattributedRows())
        .append(" name no table.\n\n");
    long rows = sample.tableRows();
    if (rows > 0) {
      sb.append("Of ")
          .append(rows)
          .append(" rows naming a table, ")
          .append(sample.capturedRows())
          .append(
              " belong to captured tables. The connector's mining query filters out the"
                  + " other ")
          .append(percent(sample.discardShare()))
          .append(", but LogMiner still reads their redo.\n\n");
      sb.append(
          "| Table | Captured | Rows | Share | Inserts | Updates | Deletes | DDL | Other | Pattern"
              + " |\n|---|---|---|---|---|---|---|---|---|---|\n");
      int n = 0;
      for (TableProfile t : sample.tables()) {
        if (n++ >= top) {
          break;
        }
        sb.append("| ")
            .append(t.name())
            .append(" | ")
            .append(t.captured() ? "yes" : "no")
            .append(" | ")
            .append(t.total())
            .append(" | ")
            .append(percent(t.total() / (double) rows))
            .append(" | ")
            .append(t.inserts())
            .append(" | ")
            .append(t.updates())
            .append(" | ")
            .append(t.deletes())
            .append(" | ")
            .append(t.ddl())
            .append(" | ")
            .append(t.other())
            .append(" | ")
            .append(t.pattern() == null ? "" : t.pattern())
            .append(" |\n");
      }
      TableProfile first = sample.tables().get(0);
      sb.append('\n');
      if (!first.captured()) {
        sb.append("The top redo source, ")
            .append(first.name())
            .append(", is not captured")
            .append(first.pattern() == null ? "" : " and looks like a " + first.pattern() + " job")
            .append(": it accounts for ")
            .append(percent(first.total() / (double) rows))
            .append(" of the rows LogMiner reads for the connector.\n");
      }
      for (TableProfile t : sample.tables()) {
        if (t.pattern() != null && t != first) {
          sb.append(t.name()).append(" looks like a ").append(t.pattern()).append(" job.\n");
        }
      }
    }
    return sb.toString();
  }

  static String percent(double share) {
    return String.format(Locale.ROOT, "%.1f per cent", share * 100);
  }
}
