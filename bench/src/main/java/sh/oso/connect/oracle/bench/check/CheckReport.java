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
package sh.oso.connect.oracle.bench.check;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The evidence of one check run: verdict, counts and the first examples of every difference. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "report",
  "schemaVersion",
  "generatedAt",
  "verdict",
  "checkScn",
  "recordsConsumed",
  "tombstones",
  "state",
  "transactions",
  "invariants",
  "failures",
  "inconclusiveReason",
  "sha256"
})
public final class CheckReport {

  public static final int EXIT_PASS = 0;
  public static final int EXIT_FAIL = 1;
  public static final int EXIT_INCONCLUSIVE = 3;
  public static final String REPORT = "oracle-cdc-correctness";
  public static final int SCHEMA_VERSION = 1;

  public enum Verdict {
    PASS,
    FAIL,
    INCONCLUSIVE
  }

  private final String generatedAt = Instant.now().toString();
  private Verdict verdict = Verdict.PASS;
  private long checkScn;
  private long recordsConsumed;
  private long tombstones;
  private final Map<String, Object> state = new LinkedHashMap<>();
  private final Map<String, Object> transactions = new LinkedHashMap<>();
  private final Map<String, Object> invariants = new LinkedHashMap<>();
  private final List<String> failures = new ArrayList<>();
  private String inconclusiveReason;
  private String sha256;

  @JsonProperty
  public String report() {
    return REPORT;
  }

  @JsonProperty
  public int schemaVersion() {
    return SCHEMA_VERSION;
  }

  @JsonProperty
  public String generatedAt() {
    return generatedAt;
  }

  @JsonProperty
  public Verdict verdict() {
    return verdict;
  }

  @JsonProperty
  public long checkScn() {
    return checkScn;
  }

  public void checkScn(long scn) {
    this.checkScn = scn;
  }

  @JsonProperty
  public long recordsConsumed() {
    return recordsConsumed;
  }

  public void recordsConsumed(long n) {
    this.recordsConsumed = n;
  }

  @JsonProperty
  public long tombstones() {
    return tombstones;
  }

  public void tombstones(long n) {
    this.tombstones = n;
  }

  @JsonProperty
  public Map<String, Object> state() {
    return state;
  }

  @JsonProperty
  public Map<String, Object> transactions() {
    return transactions;
  }

  @JsonProperty
  public Map<String, Object> invariants() {
    return invariants;
  }

  @JsonProperty
  public List<String> failures() {
    return failures;
  }

  @JsonProperty
  public String inconclusiveReason() {
    return inconclusiveReason;
  }

  @JsonProperty
  public String sha256() {
    return sha256;
  }

  public void fail(String reason) {
    verdict = Verdict.FAIL;
    failures.add(reason);
  }

  public void inconclusive(String reason) {
    if (verdict != Verdict.FAIL) {
      verdict = Verdict.INCONCLUSIVE;
    }
    inconclusiveReason = reason;
  }

  public int exitCode() {
    switch (verdict) {
      case PASS:
        return EXIT_PASS;
      case INCONCLUSIVE:
        return EXIT_INCONCLUSIVE;
      default:
        return EXIT_FAIL;
    }
  }

  /** Serialises with the SHA-256 of the document without the hash field. */
  public String toJson() {
    try {
      ObjectMapper m = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
      sha256 = null;
      String body = m.writeValueAsString(this);
      sha256 = sha256(body);
      return m.writeValueAsString(this);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void write(Path file) throws IOException {
    Files.writeString(file, toJson(), StandardCharsets.UTF_8);
  }

  static String sha256(String s) {
    try {
      byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte b : d) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
