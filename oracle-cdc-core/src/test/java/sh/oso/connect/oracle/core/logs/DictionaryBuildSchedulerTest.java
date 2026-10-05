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

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DictionaryBuildSchedulerTest {

  final List<String> events = new ArrayList<>();
  SQLException next;
  int builds;

  DictionaryBuildScheduler scheduler() {
    return new DictionaryBuildScheduler(
        () -> {
          builds++;
          if (next != null) {
            throw next;
          }
        },
        new DictionaryBuildScheduler.Events() {
          public void built(Duration took) {
            events.add("built");
          }

          public void failed(String message) {
            events.add("failed: " + message);
          }

          public void disabled(String message) {
            events.add("disabled: " + message);
          }
        },
        Duration.ofDays(1));
  }

  @Test
  void theFirstBuildWaitsForTheNextBuildTimeInTheDatabasesClock() {
    LocalTime two = LocalTime.of(2, 0);
    assertThat(DictionaryBuildScheduler.delayUntil(LocalDateTime.of(2026, 10, 5, 1, 30), two))
        .isEqualTo(Duration.ofMinutes(30));
    assertThat(DictionaryBuildScheduler.delayUntil(LocalDateTime.of(2026, 10, 5, 2, 0), two))
        .as("exactly at the time: the next day")
        .isEqualTo(Duration.ofDays(1));
    assertThat(DictionaryBuildScheduler.delayUntil(LocalDateTime.of(2026, 10, 5, 23, 0), two))
        .isEqualTo(Duration.ofHours(3));
  }

  @Test
  void aFailedBuildIsReportedAndAMissingPrivilegeSwitchesBuildsOff() {
    DictionaryBuildScheduler s = scheduler();
    s.runOnce();
    next = new SQLException("ORA-01653: unable to extend table", "72000", 1653);
    s.runOnce();
    assertThat(s.disabled()).isFalse();
    next =
        new SQLException(
            "ORA-06550: line 1, column 7:\nPLS-00201: identifier 'DBMS_LOGMNR_D' must be declared",
            "65000",
            6550);
    s.runOnce();
    assertThat(s.disabled()).isTrue();
    s.runOnce(); // off: not attempted
    assertThat(builds).isEqualTo(3);
    assertThat(events.get(0)).isEqualTo("built");
    assertThat(events.get(1)).startsWith("failed: Dictionary build failed: ORA-01653");
    assertThat(events.get(2))
        .startsWith("disabled: Dictionary builds are off")
        .contains("ORA-06550")
        .contains("EXECUTE ON DBMS_LOGMNR_D");
    assertThat(events).hasSize(3);
    assertThat(
            DictionaryBuildScheduler.privilege(
                new SQLException("ORA-01031: insufficient privileges", "42000", 1031)))
        .isTrue();
  }

  @Test
  void startRunsABuildAtOnceWhenAskedAndCloseStopsTheThread() throws Exception {
    DictionaryBuildScheduler s = scheduler();
    s.start(Duration.ofHours(1), true);
    long deadline = System.currentTimeMillis() + 5000;
    while (events.isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    s.close();
    assertThat(events).containsExactly("built");
  }
}
