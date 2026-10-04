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
package sh.oso.connect.oracle.bench.workload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Parameters of a deterministic workload (testing strategy section 3, item 1). Every field has a
 * default so a spec file only names what it changes. The same seed and spec always produce the same
 * statement sequence per session.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WorkloadSpec {

  /** Seed for every session's random stream; session {@code i} uses {@code seed + i}. */
  public long seed = 1;

  /** Concurrent sessions, each in its own connection and touching only its own rows. */
  public int sessions = 1;

  /** Data tables {@code <tablePrefix>1..tables}. */
  public int tables = 3;

  /** Transactions per session; 0 means run until {@link #durationSeconds} elapses. */
  public int transactionsPerSession = 100;

  /** Wall-clock bound used when {@link #transactionsPerSession} is 0. */
  public int durationSeconds = 0;

  /** Rows touched by an ordinary transaction are 1 to this many. */
  public int maxRowsPerTransaction = 10;

  /** Every Nth transaction of a session is large (0 disables). */
  public int largeTransactionEvery = 0;

  /** Rows in a large transaction. */
  public int largeTransactionRows = 10_000;

  /** Relative weights of the row operations once a session owns live rows. */
  public double insertWeight = 5;

  public double updateWeight = 3;
  public double deleteWeight = 1;
  public double keyChangeWeight = 0.5;
  public double lobWeight = 0.5;

  /** Probability that a transaction rolls back to a savepoint halfway through. */
  public double savepointRollbackProbability = 0.1;

  /** Probability that a transaction rolls back entirely (nothing reaches the ledger). */
  public double fullRollbackProbability = 0.05;

  /** Probability, per committed transaction of session 0, of a TRUNCATE of a random table. */
  public double truncateProbability = 0;

  /** Probability, per committed transaction of session 0, of an ADD or DROP COLUMN. */
  public double ddlProbability = 0;

  /** Longest CLOB written by a LOB operation; above about 4,000 characters it goes out of line. */
  public int lobMaxChars = 8000;

  public String tablePrefix = "WL_T";

  public String ledgerTable = "WL_LEDGER";

  public static WorkloadSpec defaults() {
    return new WorkloadSpec();
  }

  public static WorkloadSpec read(Path file) throws IOException {
    return new ObjectMapper().readValue(Files.readString(file), WorkloadSpec.class);
  }

  public static WorkloadSpec parse(String json) throws IOException {
    return new ObjectMapper().readValue(json, WorkloadSpec.class);
  }

  public String tableName(int index) {
    return tablePrefix + index;
  }

  void validate() {
    if (sessions < 1 || tables < 1) {
      throw new IllegalArgumentException("sessions and tables must be at least 1");
    }
    if (transactionsPerSession <= 0 && durationSeconds <= 0) {
      throw new IllegalArgumentException(
          "set transactionsPerSession or durationSeconds to a positive value");
    }
    if (maxRowsPerTransaction < 1) {
      throw new IllegalArgumentException("maxRowsPerTransaction must be at least 1");
    }
  }
}
