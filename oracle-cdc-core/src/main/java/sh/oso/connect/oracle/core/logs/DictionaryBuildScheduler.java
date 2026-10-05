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
package sh.oso.connect.oracle.core.logs;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;

/**
 * PRD-03 section 3 step 5: runs {@code DBMS_LOGMNR_D.BUILD} into the redo on its own thread and
 * connection, first at {@code cdc.dictionary.build.time} in the database's time and then every
 * {@code cdc.dictionary.build.interval.ms}, so a lag case finds a build before its rows. A missing
 * privilege switches it off; any other failure is reported and the schedule goes on.
 */
public final class DictionaryBuildScheduler implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DictionaryBuildScheduler.class);

  /** Runs one build, on a connection of its own. */
  public interface Builder {
    void build() throws SQLException;
  }

  /** What happened to a build, for the ops topic. */
  public interface Events {
    void built(Duration took);

    void failed(String message);

    void disabled(String message);
  }

  private final Builder builder;
  private final Events events;
  private final Duration interval;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> schedule;
  private volatile boolean disabled;

  public DictionaryBuildScheduler(Builder builder, Events events, Duration interval) {
    this.builder = builder;
    this.events = events;
    this.interval = interval;
  }

  /** The wait from {@code databaseNow} until the next {@code at} in the database's time. */
  public static Duration delayUntil(LocalDateTime databaseNow, LocalTime at) {
    LocalDateTime next = databaseNow.toLocalDate().atTime(at);
    if (!next.isAfter(databaseNow)) {
      next = next.plusDays(1);
    }
    return Duration.between(databaseNow, next);
  }

  /** Schedules the builds; with {@code buildNow} one runs straight away as well. */
  public synchronized void start(Duration initialDelay, boolean buildNow) {
    executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "oracle-cdc-dictionary-build");
              t.setDaemon(true);
              return t;
            });
    if (buildNow) {
      executor.execute(this::runOnce);
    }
    schedule =
        executor.scheduleAtFixedRate(
            this::runOnce, initialDelay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** One build; a privilege error switches the schedule off. */
  void runOnce() {
    if (disabled) {
      return;
    }
    long t0 = System.nanoTime();
    try {
      builder.build();
      Duration took = Duration.ofNanos(System.nanoTime() - t0);
      LOG.info("Dictionary build written to the redo in {} ms", took.toMillis());
      events.built(took);
    } catch (SQLException e) {
      if (privilege(e)) {
        disabled = true;
        if (schedule != null) {
          schedule.cancel(false);
        }
        String message =
            "Dictionary builds are off: the connector user cannot run DBMS_LOGMNR_D.BUILD ("
                + firstLine(e)
                + "). Grant EXECUTE ON DBMS_LOGMNR_D so rows written before a DDL can be decoded.";
        LOG.warn(message);
        events.disabled(message);
      } else {
        String message = "Dictionary build failed: " + firstLine(e);
        LOG.warn(message, e);
        events.failed(message);
      }
    } catch (RuntimeException e) {
      LOG.warn("Dictionary build failed", e);
      events.failed("Dictionary build failed: " + e.getMessage());
    }
  }

  public boolean disabled() {
    return disabled;
  }

  /** PLS-00201 (DBMS_LOGMNR_D not visible) or ORA-01031 (insufficient privileges). */
  static boolean privilege(SQLException e) {
    int code = OraErrorClassifier.oraCode(e);
    String m = String.valueOf(e.getMessage());
    return code == 1031 || (code == 6550 && m.contains("PLS-00201"));
  }

  private static String firstLine(SQLException e) {
    return String.valueOf(e.getMessage()).split("\n")[0].trim();
  }

  @Override
  public synchronized void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
