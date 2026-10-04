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
package sh.oso.connect.oracle.core.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcPurgedException;
import sh.oso.connect.oracle.core.errors.TransientDatabaseException;
import sh.oso.connect.oracle.core.testkit.FixedClock;

class WithRetryTest {

  private static ConnectionFactory factory(FixedClock clock, Duration budget) {
    OracleConnectionSpec spec =
        OracleConnectionSpec.from(
            new CoreConfig(
                Map.of(
                    CoreConfig.DATABASE_HOST, "h",
                    CoreConfig.DATABASE_SERVICE, "S",
                    CoreConfig.DATABASE_USER, "u",
                    CoreConfig.DATABASE_PASSWORD, "p")));
    RetryPolicy retry =
        new RetryPolicy(Duration.ofMillis(10), Duration.ofMillis(40), budget, clock, clock::sleep);
    return new ConnectionFactory(spec, s -> null, retry, new OraErrorClassifier());
  }

  @Test
  void transientFailuresAreRetriedUntilSuccess() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    String result =
        factory(new FixedClock(0), Duration.ofSeconds(5))
            .withRetry(
                () -> {
                  if (calls.incrementAndGet() < 3) {
                    throw new RuntimeException(
                        new SQLException("ORA-03113: end-of-file", "08006", 3113));
                  }
                  return "ok";
                },
                "probe");
    assertThat(result).isEqualTo("ok");
    assertThat(calls.get()).isEqualTo(3);
  }

  @Test
  void stopConditionsAndUnknownErrorsAreNotRetried() {
    AtomicInteger calls = new AtomicInteger();
    assertThatThrownBy(
            () ->
                factory(new FixedClock(0), Duration.ofSeconds(5))
                    .withRetry(
                        () -> {
                          calls.incrementAndGet();
                          throw new RuntimeException(
                              new SQLException("ORA-01284: file cannot be opened", "99999", 1284));
                        },
                        "add log"))
        .isInstanceOf(OracleCdcPurgedException.class);
    assertThat(calls.get()).isEqualTo(1);
    assertThatThrownBy(
            () ->
                factory(new FixedClock(0), Duration.ofSeconds(5))
                    .withRetry(
                        () -> {
                          throw new IllegalStateException("bug");
                        },
                        "x"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void budgetExhaustionSurfacesTheTransientError() {
    FixedClock clock = new FixedClock(0);
    assertThatThrownBy(
            () ->
                factory(clock, Duration.ofMillis(100))
                    .withRetry(
                        () -> {
                          throw new RuntimeException(
                              new SQLException("ORA-12541: no listener", "66000", 12541));
                        },
                        "connect"))
        .isInstanceOf(TransientDatabaseException.class);
    assertThat(clock.getAsLong()).isLessThanOrEqualTo(100L);
  }
}
