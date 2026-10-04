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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.testkit.FixedClock;

class RetryPolicyTest {

  @Test
  void delaysGrowAndAreCapped() {
    RetryPolicy p =
        new RetryPolicy(
            Duration.ofSeconds(1),
            Duration.ofSeconds(30),
            Duration.ofMinutes(5),
            () -> 0L,
            d -> {});
    assertThat(p.delayFor(1).toMillis()).isBetween(500L, 1000L);
    assertThat(p.delayFor(3).toMillis()).isBetween(2000L, 4000L);
    assertThat(p.delayFor(40).toMillis()).isBetween(15000L, 30000L);
  }

  @Test
  void budgetBoundsTheRetries() throws InterruptedException {
    FixedClock clock = new FixedClock(0);
    List<Duration> slept = new ArrayList<>();
    RetryPolicy p =
        new RetryPolicy(
            Duration.ofSeconds(1),
            Duration.ofSeconds(8),
            Duration.ofSeconds(20),
            clock,
            d -> {
              slept.add(d);
              clock.sleep(d);
            });
    RetryPolicy.Attempts a = p.begin();
    int retries = 0;
    while (a.backoff()) {
      retries++;
    }
    assertThat(retries).isBetween(3, 12);
    assertThat(clock.getAsLong()).isLessThanOrEqualTo(20000L);
    assertThat(slept).isNotEmpty();
    assertThat(a.attempt()).isEqualTo(retries + 1);
  }
}
