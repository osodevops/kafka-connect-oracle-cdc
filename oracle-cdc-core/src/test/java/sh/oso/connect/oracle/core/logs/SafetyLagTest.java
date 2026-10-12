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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SafetyLagTest {

  @Test
  void theLagIsZeroWithOneEnabledThreadWhateverTheKeySays() {
    assertThat(SafetyLag.effective(-1, 1)).isZero();
    assertThat(SafetyLag.effective(5000, 1)).isZero();
    assertThat(SafetyLag.effective(-1, 0)).isZero();
    assertThat(SafetyLag.effective(-1, 2)).isEqualTo(Duration.ofMillis(3000));
    assertThat(SafetyLag.effective(0, 2)).isZero();
    assertThat(SafetyLag.effective(750, 3)).isEqualTo(Duration.ofMillis(750));
  }

  @Test
  void withoutALagTheSafeEndIsTheCurrentScnAndNothingIsKept() {
    AtomicLong clock = new AtomicLong();
    SafetyLag none = new SafetyLag(Duration.ZERO, clock::get);
    assertThat(none.safeEnd(100)).isEqualTo(100);
    clock.set(Long.MAX_VALUE); // the clock is not even read
    assertThat(none.safeEnd(250)).isEqualTo(250);
  }

  @Test
  void theSafeEndIsTheNewestReadAtLeastTheLagOld() {
    AtomicLong clock = new AtomicLong(10_000);
    SafetyLag lag = new SafetyLag(Duration.ofMillis(3000), clock::get);
    assertThat(lag.safeEnd(100)).as("no read is old enough yet").isEqualTo(SafetyLag.NONE);
    clock.set(11_000);
    assertThat(lag.safeEnd(200)).isEqualTo(SafetyLag.NONE);
    clock.set(12_500);
    assertThat(lag.safeEnd(300)).isEqualTo(SafetyLag.NONE);
    clock.set(13_000);
    assertThat(lag.safeEnd(400)).as("the read at 10 000 is exactly 3 s old").isEqualTo(100);
    clock.set(13_999);
    assertThat(lag.safeEnd(450)).as("the read at 11 000 is not quite 3 s old").isEqualTo(100);
    clock.set(15_600);
    assertThat(lag.safeEnd(500)).as("newest of those at least 3 s old: 12 500").isEqualTo(300);
    clock.set(15_601);
    assertThat(lag.safeEnd(510)).as("it stays the answer until a newer read is old").isEqualTo(300);
    clock.set(30_000);
    assertThat(lag.safeEnd(900)).as("the read at 15 601").isEqualTo(510);
  }
}
