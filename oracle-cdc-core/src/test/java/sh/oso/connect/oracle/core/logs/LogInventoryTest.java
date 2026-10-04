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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.errors.OracleCdcGapException;
import sh.oso.connect.oracle.core.errors.OracleCdcPurgedException;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

class LogInventoryTest {

  @Test
  void contiguousArchivedRangeIsChosenAndOnlineTailAddedInOnlineMode() throws Exception {
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 3, 1000, 100).onlineCurrent(1, 13, 1300);
    LogSet set = new LogInventory(cat, CaptureMode.ONLINE, 1).forRange(1050, 1350);
    assertThat(set.logs()).extracting(RedoLog::sequence).containsExactly(10L, 11L, 12L, 13L);
    assertThat(set.logs().get(3).archived()).isFalse();
    LogSet archiveOnly = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).forRange(1050, 1250);
    assertThat(archiveOnly.logs()).extracting(RedoLog::sequence).containsExactly(10L, 11L, 12L);
  }

  @Test
  void archiveOnlyModeNeverAddsOnlineLogsAndReportsSafeEnd() throws Exception {
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 3, 1000, 100).onlineCurrent(1, 13, 1300);
    LogInventory inv = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1);
    assertThat(inv.archiveOnlySafeEnd(1000)).isEqualTo(1299);
    assertThatThrownBy(() -> inv.forRange(1050, 1350))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("before the end SCN");
  }

  @Test
  void missingSequenceIsAGapNamingTheThreadAndSequence() {
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 4, 1000, 100).removeArchived(1, 12);
    assertThatThrownBy(
            () -> new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).forRange(1050, 1350))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("thread 1")
        .hasMessageContaining("sequence 12 is missing");
  }

  @Test
  void deletedLogIsPurgedNeverSkipped() {
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 4, 1000, 100).markDeleted(1, 11);
    assertThatThrownBy(
            () -> new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).forRange(1050, 1350))
        .isInstanceOf(OracleCdcPurgedException.class)
        .hasMessageContaining("sequence 11")
        .hasMessageContaining("never skips");
  }

  @Test
  void duplicateArchivedCopiesPreferTheReadableOne() throws Exception {
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 2, 1000, 100);
    cat.archived.add(
        new RedoLog(1, 11, 1100, 1200, "/arch/dup.arc", true, "D", true, 1, false, false));
    LogSet set = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).forRange(1000, 1150);
    assertThat(set.logs()).extracting(RedoLog::sequence).containsExactly(10L, 11L);
    assertThat(set.logs()).noneMatch(RedoLog::purgedInCatalog);
  }

  @Test
  void racThreadsAreCheckedIndependentlyWhateverTheirPublicOrPrivateStatus() throws Exception {
    FakeCatalog cat =
        new FakeCatalog().archivedRun(1, 10, 3, 1000, 100).archivedRun(2, 40, 3, 1000, 100);
    cat.threads.add(new ThreadInfo(2, true, "OPEN", 42));
    LogSet set = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).forRange(1050, 1250);
    assertThat(set.byThread().keySet()).containsExactly(1, 2);
    cat.removeArchived(2, 41);
    assertThatThrownBy(
            () -> new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).forRange(1050, 1250))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("thread 2");
    // a disabled thread with no redo in the range is not required
    cat.threads.replaceAll(t -> t.thread() == 2 ? new ThreadInfo(2, false, "CLOSED", 42) : t);
    cat.archived.removeIf(l -> l.thread() == 2);
    assertThat(
            new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1)
                .forRange(1050, 1250)
                .byThread()
                .keySet())
        .containsExactly(1);
  }

  @Test
  void startBeforeTheEarliestLogIsAGap() {
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 3, 1000, 100);
    assertThatThrownBy(() -> new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).forRange(900, 1250))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("after the start SCN");
  }
}
