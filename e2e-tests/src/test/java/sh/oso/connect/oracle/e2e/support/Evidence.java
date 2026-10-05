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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.bench.check.CheckReport;

/**
 * The evidence file of one T2 scenario (testing strategy section 3, item 5): {@code
 * target/<suite>[-<case>]-evidence.json} with the scenario, parameters and seed, the versions under
 * test, the fault schedule as it was applied, counts, the correctness-oracle reports, the verdict
 * and a SHA-256 of the document. The nightly workflow uploads every {@code target/*-evidence.json};
 * the release gate needs them.
 *
 * <p>Write it from a {@code finally} block so a failed run leaves evidence too: call {@link
 * #failed(Throwable)} from the catch, then {@link #write()}.
 */
public final class Evidence {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private final String suite;
  private final String caseName;
  private final String scenario;
  private final String startedAt = Instant.now().toString();
  private final Map<String, Object> parameters = new LinkedHashMap<>();
  private final List<Map<String, Object>> faults = new ArrayList<>();
  private final Map<String, Object> counts = new LinkedHashMap<>();
  private final Map<String, Object> checks = new LinkedHashMap<>();
  private final List<String> notes = new ArrayList<>();
  private final List<String> failures = new ArrayList<>();

  private Evidence(Class<?> suite, String caseName, String scenario) {
    this.suite = suite.getSimpleName();
    this.caseName = caseName;
    this.scenario = scenario;
  }

  /** One evidence file for a suite with a single scenario. */
  public static Evidence of(Class<?> suite, String scenario) {
    return new Evidence(suite, null, scenario);
  }

  /** One evidence file per case when a suite holds several scenarios. */
  public static Evidence of(Class<?> suite, String caseName, String scenario) {
    return new Evidence(suite, caseName, scenario);
  }

  public Evidence param(String name, Object value) {
    parameters.put(name, value);
    return this;
  }

  public Evidence count(String name, Object value) {
    counts.put(name, value);
    return this;
  }

  public Evidence note(String text) {
    notes.add(text);
    return this;
  }

  /** Records a fault as it is applied, with the wall-clock time in UTC. */
  public synchronized Evidence fault(String kind, Object... details) {
    Map<String, Object> f = new LinkedHashMap<>();
    f.put("at", Instant.now().toString());
    f.put("kind", kind);
    for (int i = 0; i + 1 < details.length; i += 2) {
      f.put(String.valueOf(details[i]), details[i + 1]);
    }
    faults.add(f);
    return this;
  }

  /** Embeds a correctness-oracle report; anything but PASS marks the evidence failed. */
  public Evidence check(String name, CheckReport report) {
    try {
      checks.put(name, MAPPER.readTree(report.toJson()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (report.verdict() != CheckReport.Verdict.PASS) {
      failures.add(name + ": oracle verdict " + report.verdict() + " " + report.failures());
    }
    return this;
  }

  public Evidence failed(Throwable t) {
    String m = t.getMessage() == null ? "" : t.getMessage();
    failures.add(
        t.getClass().getSimpleName() + ": " + (m.length() > 2000 ? m.substring(0, 2000) : m));
    return this;
  }

  public String verdict() {
    return failures.isEmpty() ? "PASS" : "FAIL";
  }

  public Path path() {
    return Path.of("target", suite + (caseName == null ? "" : "-" + caseName) + "-evidence.json");
  }

  /** Writes the file and returns its path; never throws, so it cannot mask the test outcome. */
  public Path write() {
    Path out = path();
    try {
      ObjectNode doc = MAPPER.createObjectNode();
      doc.put("report", "oracle-cdc-nightly");
      doc.put("schemaVersion", 1);
      doc.put("suite", suite);
      if (caseName != null) {
        doc.put("case", caseName);
      }
      doc.put("scenario", scenario);
      doc.put("startedAt", startedAt);
      doc.put("finishedAt", Instant.now().toString());
      ObjectNode versions = doc.putObject("versions");
      versions.put("project", System.getProperty("project.version", "unknown"));
      versions.put("oracleImageTag", System.getProperty("oracle.image.tag", "unknown"));
      versions.put("kafkaImage", ConnectCluster.KAFKA_IMAGE);
      versions.put("java", System.getProperty("java.version"));
      doc.set("parameters", MAPPER.valueToTree(parameters));
      doc.set("faults", MAPPER.valueToTree(faults));
      doc.set("counts", MAPPER.valueToTree(counts));
      doc.set("checks", MAPPER.valueToTree(checks));
      doc.set("notes", MAPPER.valueToTree(notes));
      doc.put("verdict", verdict());
      doc.set("failures", MAPPER.valueToTree(failures));
      String body = MAPPER.writeValueAsString(doc);
      doc.put("sha256", sha256(body));
      Files.createDirectories(out.getParent());
      Files.writeString(out, MAPPER.writeValueAsString(doc), StandardCharsets.UTF_8);
      System.out.println("evidence: " + out.toAbsolutePath() + " verdict=" + verdict());
    } catch (IOException | RuntimeException e) {
      System.err.println("evidence: could not write " + out + ": " + e);
    }
    return out;
  }

  private static String sha256(String s) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
