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
package sh.oso.connect.oracle.bench.soak;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import sh.oso.connect.oracle.bench.check.CheckReport;

/**
 * The soak's output directory: {@code soak.log} (also echoed to the console), {@code
 * check-NNN.json} evidence, {@code metrics.csv} and the summary. Log lines carry verdicts, counts
 * and SCNs only; foreign text (exception messages) is scrubbed of secrets, and row values never
 * reach the log: they stay in the evidence files, as with {@code bench check}.
 */
public final class SoakOutput {

  public static final String LOG = "soak.log";
  public static final String METRICS = "metrics.csv";
  public static final String SUMMARY_JSON = "summary.json";
  public static final String SUMMARY_MD = "summary.md";

  private final Path dir;
  private final PrintStream console;
  private final Redactor redactor;
  private final Soak.Clock clock;

  public SoakOutput(Path dir, PrintStream console, Redactor redactor, Soak.Clock clock) {
    this.dir = dir;
    this.console = console;
    this.redactor = redactor;
    this.clock = clock;
    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Path dir() {
    return dir;
  }

  public Path metricsCsv() {
    return dir.resolve(METRICS);
  }

  /** Appends a timestamped line to {@code soak.log} and the console. */
  public synchronized void log(String message) {
    String line = Instant.ofEpochMilli(clock.nowMillis()) + " " + message;
    console.println(line);
    console.flush();
    append(dir.resolve(LOG), line + "\n");
  }

  /** Writes the evidence of check {@code index}; returns the file name. */
  public String writeCheck(int index, CheckReport report) {
    String name = String.format("check-%03d.json", index);
    try {
      report.write(dir.resolve(name));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return name;
  }

  public void writeSummary(SoakSummary s) {
    write(dir.resolve(SUMMARY_JSON), s.toJson());
    write(dir.resolve(SUMMARY_MD), s.toMarkdown());
  }

  public String scrub(String text) {
    return redactor.scrub(text);
  }

  /** An exception as one line: its type and the first line of its message, scrubbed. */
  public String describe(Throwable t) {
    Throwable root = t;
    while (root.getCause() != null
        && root.getCause() != root
        && (root.getMessage() == null || root instanceof java.util.concurrent.ExecutionException)) {
      root = root.getCause();
    }
    String msg = root.getMessage();
    if (msg != null) {
      int nl = msg.indexOf('\n');
      msg = nl < 0 ? msg : msg.substring(0, nl);
      if (msg.length() > 300) {
        msg = msg.substring(0, 300) + "...";
      }
    }
    return redactor.scrub(root.getClass().getSimpleName() + (msg == null ? "" : ": " + msg));
  }

  static void append(Path file, String text) {
    try {
      Files.writeString(
          file,
          text,
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND,
          StandardOpenOption.WRITE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void write(Path file, String text) {
    try {
      Files.writeString(file, text, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
