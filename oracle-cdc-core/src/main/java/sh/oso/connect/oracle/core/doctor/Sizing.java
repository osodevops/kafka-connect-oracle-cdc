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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Redo sizing figures from V$ARCHIVED_LOG history (PRD-05 {@code sizing}, DOC-9, DOC-10): log
 * switches per hour per thread, archive generation per day, the online log size that keeps the peak
 * hour at {@link #TARGET_SWITCHES_PER_HOUR} switches, and the archive retention and space a stated
 * maximum downtime needs. Pure arithmetic; hours and days are UTC.
 */
public final class Sizing {

  /** DOC-9 warns above this many switches per hour on a thread. */
  public static final int MAX_SWITCHES_PER_HOUR = 6;

  /** The recommended size aims at one switch every 15 minutes in the peak hour. */
  public static final int TARGET_SWITCHES_PER_HOUR = 4;

  /** Recommended sizes are rounded up to this granularity. */
  public static final long SIZE_STEP = 64L * 1024 * 1024;

  private Sizing() {}

  /** One redo thread over the observed window. */
  public record ThreadFigures(
      int thread,
      int logs,
      double switchesPerHour,
      int peakSwitchesPerHour,
      Instant peakHour,
      long peakHourBytes,
      long bytesPerDay,
      long currentLogBytes,
      long recommendedLogBytes) {}

  /** The whole report. */
  public record Result(
      Instant from,
      Instant to,
      Duration observed,
      List<ThreadFigures> threads,
      long peakHourBytes,
      long busiestDayBytes,
      long bytesPerDay,
      Duration requiredRetention,
      long archiveSpaceBytes) {}

  /** How far back archived redo reaches at the destination (DOC-10). */
  public record Reach(boolean purgeSeen, Instant availableFrom, Duration reach) {}

  /**
   * Figures for the logs switched out in {@code [from, to)}. {@code requiredRetention} is the
   * maximum downtime plus the journal threshold (the longest an open transaction pins the resume
   * position before it is journaled), rounded up to whole hours.
   */
  public static Result compute(
      List<ArchiveStat> history,
      List<OnlineLogGroup> groups,
      Instant from,
      Instant to,
      Duration maxDowntime,
      Duration journalThreshold) {
    List<ArchiveStat> window = new ArrayList<>();
    Instant earliest = to;
    for (ArchiveStat a : history) {
      if (a.nextTime() != null && !a.nextTime().isBefore(from) && a.nextTime().isBefore(to)) {
        window.add(a);
        if (a.nextTime().isBefore(earliest)) {
          earliest = a.nextTime();
        }
      }
    }
    Instant start = window.isEmpty() ? from : max(from, earliest.truncatedTo(ChronoUnit.HOURS));
    Duration observed = Duration.between(start, to);
    if (observed.compareTo(Duration.ofHours(1)) < 0) {
      observed = Duration.ofHours(1);
    }
    double hours = observed.toSeconds() / 3600.0;
    double days = observed.toSeconds() / 86400.0;

    Map<Integer, List<ArchiveStat>> byThread = new TreeMap<>();
    Map<Instant, Long> bytesByHour = new TreeMap<>();
    Map<Instant, Long> bytesByDay = new TreeMap<>();
    long total = 0;
    for (ArchiveStat a : window) {
      byThread.computeIfAbsent(a.thread(), t -> new ArrayList<>()).add(a);
      bytesByHour.merge(a.nextTime().truncatedTo(ChronoUnit.HOURS), a.bytes(), Long::sum);
      bytesByDay.merge(a.nextTime().truncatedTo(ChronoUnit.DAYS), a.bytes(), Long::sum);
      total += a.bytes();
    }
    TreeSet<Integer> threads = new TreeSet<>(byThread.keySet());
    for (OnlineLogGroup g : groups) {
      threads.add(g.thread());
    }
    List<ThreadFigures> figures = new ArrayList<>();
    for (int t : threads) {
      List<ArchiveStat> logs = byThread.getOrDefault(t, List.of());
      Map<Instant, Integer> countByHour = new TreeMap<>();
      Map<Instant, Long> sizeByHour = new TreeMap<>();
      long threadBytes = 0;
      for (ArchiveStat a : logs) {
        Instant h = a.nextTime().truncatedTo(ChronoUnit.HOURS);
        countByHour.merge(h, 1, Integer::sum);
        sizeByHour.merge(h, a.bytes(), Long::sum);
        threadBytes += a.bytes();
      }
      Instant peakHour = null;
      int peak = 0;
      for (Map.Entry<Instant, Integer> e : countByHour.entrySet()) {
        if (e.getValue() > peak) {
          peak = e.getValue();
          peakHour = e.getKey();
        }
      }
      long peakBytes = sizeByHour.values().stream().mapToLong(Long::longValue).max().orElse(0);
      long current =
          groups.stream()
              .filter(g -> g.thread() == t)
              .mapToLong(OnlineLogGroup::bytes)
              .max()
              .orElse(0);
      long recommended = Math.max(current, roundUp(peakBytes / TARGET_SWITCHES_PER_HOUR));
      figures.add(
          new ThreadFigures(
              t,
              logs.size(),
              logs.size() / hours,
              peak,
              peakHour,
              peakBytes,
              Math.round(threadBytes / days),
              current,
              recommended));
    }
    long peakHourBytes = bytesByHour.values().stream().mapToLong(Long::longValue).max().orElse(0);
    long busiestDay = bytesByDay.values().stream().mapToLong(Long::longValue).max().orElse(0);
    Duration retention = roundUpHours(maxDowntime.plus(journalThreshold));
    long retentionHours = retention.toHours();
    long space =
        retentionHours <= 24
            ? peakHourBytes * retentionHours
            : Math.round(busiestDay * (retentionHours / 24.0));
    return new Result(
        from,
        to,
        observed,
        figures,
        peakHourBytes,
        busiestDay,
        Math.round(total / days),
        retention,
        space);
  }

  /**
   * How far back redo is available: from the newest of each enabled thread's oldest log still
   * present, since mining needs every thread. {@code purgeSeen} is false when the catalog lists no
   * deleted log, so retention has not been exercised yet; null when no log is present at all.
   */
  public static Reach reach(List<ArchiveStat> all, Collection<Integer> threads, Instant now) {
    boolean purged = all.stream().anyMatch(ArchiveStat::deleted);
    Instant availableFrom = null;
    for (int t : threads) {
      Instant oldest =
          all.stream()
              .filter(a -> a.thread() == t && !a.deleted() && a.firstTime() != null)
              .map(ArchiveStat::firstTime)
              .min(Instant::compareTo)
              .orElse(null);
      if (oldest == null) {
        continue;
      }
      availableFrom = availableFrom == null ? oldest : max(availableFrom, oldest);
    }
    if (availableFrom == null) {
      return null;
    }
    return new Reach(purged, availableFrom, Duration.between(availableFrom, now));
  }

  static long roundUp(long bytes) {
    if (bytes <= 0) {
      return 0;
    }
    return ((bytes + SIZE_STEP - 1) / SIZE_STEP) * SIZE_STEP;
  }

  static Duration roundUpHours(Duration d) {
    long hours = d.toHours();
    return d.equals(Duration.ofHours(hours)) ? d : Duration.ofHours(hours + 1);
  }

  private static Instant max(Instant a, Instant b) {
    return a.isAfter(b) ? a : b;
  }

  /** Bytes as a short human figure: {@code 512 MiB}, {@code 1.5 GiB}. */
  public static String bytes(long b) {
    if (b < 1024 * 1024) {
      return (b / 1024) + " KiB";
    }
    if (b < 1024L * 1024 * 1024) {
      return (b / (1024 * 1024)) + " MiB";
    }
    long gib = 1024L * 1024 * 1024;
    return (b % gib == 0
            ? Long.toString(b / gib)
            : String.format(java.util.Locale.ROOT, "%.1f", b / (double) gib))
        + " GiB";
  }

  /** A duration as hours and minutes: {@code 26 h}, {@code 1 h 30 min}, {@code 45 min}. */
  public static String duration(Duration d) {
    long h = d.toHours();
    long m = d.toMinutesPart();
    if (h == 0) {
      return m + " min";
    }
    return m == 0 ? h + " h" : h + " h " + m + " min";
  }
}
