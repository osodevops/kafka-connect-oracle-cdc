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
package sh.oso.connect.oracle.core.doctor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

class RedoAvailabilityTest {

  /** Thread 1: sequences 10 to 19, SCN 1000 to 2000 in steps of 100, then the online log. */
  private static FakeCatalog catalog() {
    FakeCatalog c = new FakeCatalog().archivedRun(1, 10, 10, 1000, 100).onlineCurrent(1, 20, 2000);
    c.currentScn = 2500;
    return c;
  }

  @Test
  void presentRedoHasNoGap() throws Exception {
    RedoAvailability a = new RedoAvailability(catalog(), CaptureMode.ONLINE, 1);
    assertThat(a.check(1050)).isNull();
    assertThat(a.firstAvailableFrom(1050)).isEqualTo(1050);
  }

  @Test
  void aPurgedLogIsAGapAndThePositionPastItIsTheNextLog() throws Exception {
    FakeCatalog c = catalog().markDeleted(1, 10).markDeleted(1, 11).markDeleted(1, 13);
    RedoAvailability a = new RedoAvailability(c, CaptureMode.ONLINE, 1);
    RedoAvailability.Gap gap = a.check(1050);
    assertThat(gap.code()).isEqualTo("CDC-2002");
    assertThat(gap.message()).contains("sequence 10");
    assertThat(a.firstAvailableFrom(1050)).isEqualTo(1400);
    assertThat(a.check(1400)).isNull();
  }

  @Test
  void aMissingSequenceIsAGapToo() throws Exception {
    FakeCatalog c = catalog().removeArchived(1, 12);
    RedoAvailability a = new RedoAvailability(c, CaptureMode.ONLINE, 1);
    assertThat(a.check(1000).code()).isEqualTo("CDC-2001");
    assertThat(a.firstAvailableFrom(1000)).isEqualTo(1300);
  }

  @Test
  void archiveOnlyChecksUpToTheArchivedEnd() throws Exception {
    FakeCatalog c = catalog();
    RedoAvailability a = new RedoAvailability(c, CaptureMode.ARCHIVE_ONLY, 1);
    assertThat(a.check(1500)).isNull();
    c.markDeleted(1, 16);
    assertThat(a.check(1500).code()).isEqualTo("CDC-2002");
    assertThat(a.firstAvailableFrom(1500)).isEqualTo(1700);
  }

  @Test
  void noPointPastTheGapWhenAnotherThreadIsMissingEverything() throws Exception {
    FakeCatalog c = catalog().markDeleted(1, 10);
    c.threads.add(new ThreadInfo(2, true, "OPEN", 3));
    RedoAvailability a = new RedoAvailability(c, CaptureMode.ONLINE, 1);
    assertThat(a.firstAvailableFrom(1000)).isEqualTo(-1);
  }
}
