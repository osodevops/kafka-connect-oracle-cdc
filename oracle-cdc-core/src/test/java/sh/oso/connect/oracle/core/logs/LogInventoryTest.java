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
    // ADR-0025: on a physical standby the current SCN is the applied one; redo received but not
    // applied is not mined yet, because the dictionary does not have its DDL
    cat.currentScn = 1150;
    assertThat(inv.archiveOnlySafeEnd(1000)).isEqualTo(1150);
    cat.currentScn = 900;
    assertThat(inv.archiveOnlySafeEnd(1000)).as("never below the start").isEqualTo(1000);
    assertThatThrownBy(() -> inv.forRange(1050, 1350))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("before the end SCN");
  }

  @Test
  void duringALogSwitchTheSetEndsAtTheNewestListedLog() throws Exception {
    // the current SCN has passed sequence 12's end, and V$LOG does not list sequence 13 yet
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 3, 1000, 100);
    LogSet set = new LogInventory(cat, CaptureMode.ONLINE, 1).forRange(1050, 1350);
    assertThat(set.endScn()).isEqualTo(1299);
    assertThat(set.logs()).extracting(RedoLog::sequence).containsExactly(10L, 11L, 12L);
    // in archive-only mode the safe end never passes the archived end, so this stays a gap
    assertThatThrownBy(
            () -> new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).forRange(1050, 1350))
        .isInstanceOf(OracleCdcGapException.class);
  }

  @Test
  void aSkippedSequenceIsStillAGapWhenALaterLogIsListed() {
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 10, 3, 1000, 100).onlineCurrent(1, 14, 1400);
    assertThatThrownBy(() -> new LogInventory(cat, CaptureMode.ONLINE, 1).forRange(1050, 1450))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("sequence 13 is missing");
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

  @Test
  void theNewestDictionaryBuildCompleteBeforeTheStartIsChosen() throws Exception {
    // logs of 100 SCNs from 1000: sequence 3 holds a whole build, sequences 6 and 7 another
    FakeCatalog cat =
        new FakeCatalog()
            .archivedRun(1, 1, 10, 1000, 100)
            .dictionaryBuild(1, 3, 3)
            .dictionaryBuild(1, 6, 7);
    LogInventory inv = new LogInventory(cat, CaptureMode.ONLINE, 1);
    assertThat(inv.dictionaryBuildBefore(1299)).isEmpty();
    assertThat(inv.dictionaryBuildBefore(1300)).contains(new DictionaryBuild(1, 1200, 1300));
    assertThat(inv.dictionaryBuildBefore(1650))
        .as("the second build is not complete until log 7 ends")
        .contains(new DictionaryBuild(1, 1200, 1300));
    assertThat(inv.dictionaryBuildBefore(1700)).contains(new DictionaryBuild(1, 1500, 1700));
    cat.markDeleted(1, 7);
    assertThat(inv.dictionaryBuildBefore(5000))
        .as("a build in a deleted log cannot be read")
        .contains(new DictionaryBuild(1, 1200, 1300));
    cat.markDeleted(1, 3);
    assertThat(inv.dictionaryBuildBefore(5000)).isEmpty();
  }
}
