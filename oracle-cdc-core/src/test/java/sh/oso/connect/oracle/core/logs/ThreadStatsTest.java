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

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;

class ThreadStatsTest {
  @Test
  void switchesAndOldestNeededSequencePerThread() throws Exception {
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 5, 1000, 100);
    Map<Integer, ThreadStats.Stat> stats = ThreadStats.compute(cat, 1, 1250, Instant.now());
    assertThat(stats).containsKey(1);
    assertThat(stats.get(1).switchesPerHour()).isEqualTo(5.0);
    assertThat(stats.get(1).oldestNeededSequence()).isEqualTo(12L);
    assertThat(stats.get(1).currentSequence()).isEqualTo(50L);
  }
}
