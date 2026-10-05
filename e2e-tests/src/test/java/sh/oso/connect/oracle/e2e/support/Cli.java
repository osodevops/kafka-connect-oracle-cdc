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
package sh.oso.connect.oracle.e2e.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.doctor.admin.Environment;
import sh.oso.connect.oracle.doctor.cli.AdminMain;
import sh.oso.connect.oracle.doctor.cli.DoctorMain;

/**
 * Runs the operator tools as an operator would: {@code oracle-cdc-doctor} and {@code
 * oracle-cdc-admin} in this JVM through their entry points (same parsing, same exit codes), and the
 * Python migration tools under {@code uv run --project tools/migration}.
 */
public final class Cli {

  private static final Logger LOG = LoggerFactory.getLogger(Cli.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private Cli() {}

  /** Exit code, standard output and standard error of one command. */
  public record Result(int exit, String out, String err) {
    public String text() {
      return out + err;
    }

    @Override
    public String toString() {
      return "exit " + exit + "\n--- stdout\n" + out + "\n--- stderr\n" + err;
    }
  }

  /** {@code oracle-cdc-admin <args>} against the real Connect REST API, database and brokers. */
  public static Result admin(String... args) {
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    int exit =
        AdminMain.run(Environment.standard(), new PrintWriter(out), new PrintWriter(err), args);
    Result r = new Result(exit, out.toString(), err.toString());
    LOG.info("oracle-cdc-admin {}: {}", args.length > 0 ? args[0] : "", r);
    return r;
  }

  /** {@code oracle-cdc-doctor <args>}. */
  public static Result doctor(String... args) {
    return doctor(Environment.standard(), args);
  }

  /** {@code oracle-cdc-doctor <args>} with another environment (clock, sleeping, endpoints). */
  public static Result doctor(Environment env, String... args) {
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    int exit = DoctorMain.run(env, new PrintWriter(out), new PrintWriter(err), args);
    Result r = new Result(exit, out.toString(), err.toString());
    LOG.info("oracle-cdc-doctor {}: {}", args.length > 0 ? args[0] : "", r);
    return r;
  }

  /**
   * Writes a connector configuration file for the CLI on this host: the worker's copy names the
   * database by its network alias, which only the containers resolve, so the copy points at the
   * mapped port instead. Everything else is the connector's own configuration.
   */
  public static Path hostConfig(
      Path file, String name, Map<String, String> config, OracleTestDatabase db)
      throws IOException {
    Map<String, String> c = new LinkedHashMap<>(config);
    c.put("cdc.database.host", db.container().getHost());
    c.put("cdc.database.port", Integer.toString(db.container().getMappedPort(1521)));
    Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("name", name);
    envelope.put("config", c);
    Files.writeString(file, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(envelope));
    return file;
  }

  /** The repository root (the parent of {@code e2e-tests}). */
  public static Path repoRoot() {
    return Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
  }

  /** Whether {@code uv} runs on this host. */
  public static boolean uvAvailable() {
    try {
      Process p = new ProcessBuilder("uv", "--version").redirectErrorStream(true).start();
      p.getInputStream().readAllBytes();
      return p.waitFor(30, TimeUnit.SECONDS) && p.exitValue() == 0;
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * {@code uv run --frozen --project tools/migration python tools/migration/<script> <args>} from
   * the repository root, with {@code env} added to the environment (passwords travel this way,
   * never on the command line). The lock file is used as it stands and never rewritten.
   */
  public static Result migration(
      String script, Map<String, String> env, Duration timeout, String... args)
      throws IOException, InterruptedException {
    Path root = repoRoot();
    List<String> command = new ArrayList<>();
    command.add("uv");
    command.add("run");
    command.add("--frozen");
    command.add("--project");
    command.add(root.resolve("tools/migration").toString());
    command.add("python");
    command.add(root.resolve("tools/migration").resolve(script).toString());
    command.addAll(List.of(args));
    ProcessBuilder pb = new ProcessBuilder(command).directory(root.toFile());
    pb.environment().putAll(env);
    Process p = pb.start();
    // drain both streams on their own threads so a full pipe never blocks the tool
    Drain out = new Drain(p.getInputStream());
    Drain err = new Drain(p.getErrorStream());
    out.start();
    err.start();
    if (!p.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
      p.destroyForcibly();
      p.waitFor(10, TimeUnit.SECONDS);
      out.join(5000);
      err.join(5000);
      throw new AssertionError(
          script + " did not finish within " + timeout + "\n" + out.text() + "\n" + err.text());
    }
    out.join(10_000);
    err.join(10_000);
    Result r = new Result(p.exitValue(), out.text(), err.text());
    LOG.info("{}: {}", script, r);
    return r;
  }

  private static final class Drain extends Thread {
    private final InputStream in;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

    Drain(InputStream in) {
      this.in = in;
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        in.transferTo(buf);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    String text() {
      synchronized (buf) {
        return buf.toString(StandardCharsets.UTF_8);
      }
    }
  }
}
