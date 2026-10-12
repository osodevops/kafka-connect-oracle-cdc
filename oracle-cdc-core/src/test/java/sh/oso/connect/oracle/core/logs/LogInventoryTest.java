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

  /** Two threads; thread 2 crashed during sequence 2, which began at 1050. */
  private static FakeCatalog crashed() {
    FakeCatalog cat = new FakeCatalog();
    cat.threads.clear();
    cat.archivedRun(1, 1, 3, 1000, 100).archivedLog(2, 1, 1000, 1050);
    return cat.thread(1, true, "OPEN", 4).thread(2, true, "CLOSED", 2);
  }

  @org.junit.jupiter.api.Tag("rac-instance-crash")
  @Test
  void aClosedThreadHoldsTheArchiveOnlySafeEndUntilItsLastLogIsArchived() throws Exception {
    FakeCatalog cat = crashed();
    LogInventory inv = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1);
    assertThat(inv.archiveOnlySafeEnd(1000)).as("its last log is not archived").isEqualTo(1049);
    cat.archivedLog(2, 2, 1050, 1081);
    assertThat(inv.archiveOnlySafeEnd(1000))
        .as("closed at 1081: nothing more to archive")
        .isEqualTo(1299);
    LogSet tail = inv.forRange(1049, 1299);
    assertThat(tail.byThread().get(2))
        .as("its redo up to the close is still mined")
        .extracting(RedoLog::sequence)
        .containsExactly(1L, 2L);
    assertThat(inv.forRange(1100, 1299).byThread().keySet())
        .as("past the close it is not required")
        .containsExactly(1);
    cat.removeArchived(2, 1);
    assertThatThrownBy(() -> inv.forRange(1000, 1299))
        .as("before the close its sequences must still be contiguous")
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("thread 2");
  }

  @org.junit.jupiter.api.Tag("rac-instance-crash")
  @Test
  void inOnlineModeAClosedThreadsArchivedLastLogDoesNotCutTheStep() throws Exception {
    FakeCatalog cat = crashed().archivedLog(2, 2, 1050, 1081).onlineCurrent(1, 4, 1300);
    cat.onlineLog(2, 2, 1050, 1081, "INACTIVE");
    LogSet set = new LogInventory(cat, CaptureMode.ONLINE, 1).forRange(1049, 1350);
    assertThat(set.endScn()).isEqualTo(1350);
    // CLOSED but its last log only in V$LOG: a log switch under way, as before
    cat.removeArchived(2, 2);
    assertThat(new LogInventory(cat, CaptureMode.ONLINE, 1).forRange(1049, 1350).endScn())
        .isEqualTo(1080);
  }

  @org.junit.jupiter.api.Tag("rac-thread-disabled")
  @Test
  void aDisabledThreadIsMinedToItsLastLogAndNeverWaitedFor() throws Exception {
    FakeCatalog cat = crashed().archivedLog(2, 2, 1050, 1120);
    cat.disableThread(2);
    LogInventory inv = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1);
    assertThat(inv.archiveOnlySafeEnd(1000)).isEqualTo(1299);
    assertThat(inv.forRange(1049, 1299).byThread().get(2))
        .extracting(RedoLog::sequence)
        .containsExactly(1L, 2L);
    assertThat(inv.forRange(1200, 1299).byThread().keySet()).containsExactly(1);
  }

  @org.junit.jupiter.api.Tag("rac-thread-disabled")
  @Test
  void aThreadOpenAgainIsRequiredFromItsFirstNewLog() throws Exception {
    FakeCatalog cat = crashed().archivedLog(2, 2, 1050, 1081);
    cat.archivedLog(1, 4, 1300, 1400).archivedLog(2, 3, 1350, 1400);
    cat.thread(1, true, "OPEN", 5).thread(2, true, "OPEN", 4);
    LogInventory inv = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1);
    assertThat(inv.forRange(1299, 1399).byThread().get(2))
        .as("sequence 2 ended at 1081: nothing of thread 2 lies before sequence 3")
        .extracting(RedoLog::sequence)
        .containsExactly(3L);
    assertThat(inv.archiveOnlySafeEnd(1000)).isEqualTo(1399);
    // the previous sequence is gone from the catalog: it cannot be shown to end before the range
    cat.removeArchived(2, 2);
    assertThatThrownBy(() -> inv.forRange(1299, 1399))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("sequence 3 starting at SCN 1350, after the start SCN");
  }

  @org.junit.jupiter.api.Tag("rac-thread-disabled")
  @Test
  void aThreadEnabledForTheFirstTimeIsRequiredFromItsFirstLog() throws Exception {
    FakeCatalog cat = new FakeCatalog();
    cat.threads.clear();
    cat.archivedRun(1, 1, 5, 1000, 100).archivedLog(2, 1, 1350, 1450);
    cat.thread(1, true, "OPEN", 6).thread(2, true, "OPEN", 2);
    LogInventory inv = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1);
    assertThat(inv.forRange(1299, 1449).byThread().get(2))
        .extracting(RedoLog::sequence)
        .containsExactly(1L);
    // online, before its first log is archived: its current log starts after the range
    cat.removeArchived(2, 1).onlineCurrent(2, 1, 1350).onlineCurrent(1, 6, 1500);
    cat.thread(2, true, "OPEN", 1);
    LogInventory online = new LogInventory(cat, CaptureMode.ONLINE, 1);
    assertThat(online.forRange(1200, 1340).byThread().keySet()).containsExactly(1);
    assertThat(online.forRange(1200, 1400).byThread().get(2))
        .extracting(RedoLog::sequence)
        .containsExactly(1L);
  }

  @Test
  void aLogStartingAfterTheStartWhosePreviousLogRunsIntoTheRangeIsStillAGap() {
    // sequence 3 of thread 2 was archived before sequence 2, which ran to 1350 and is only online
    FakeCatalog cat = new FakeCatalog();
    cat.threads.clear();
    cat.archivedRun(1, 1, 5, 1000, 100).archivedLog(2, 3, 1350, 1450);
    cat.onlineLog(2, 2, 1100, 1350, "ACTIVE").onlineCurrent(2, 4, 1450).onlineCurrent(1, 6, 1500);
    cat.thread(1, true, "OPEN", 6).thread(2, true, "OPEN", 4);
    assertThatThrownBy(() -> new LogInventory(cat, CaptureMode.ONLINE, 1).forRange(1299, 1449))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("sequence 3 starting at SCN 1350, after the start SCN");
  }

  @Test
  void withOneThreadAClosedStatusAndASequenceOneChangeNothing() throws Exception {
    // a single-thread database keeps the rules before ADR-0026, and asks the catalog for no more
    FakeCatalog cat = new FakeCatalog().archivedRun(1, 1, 3, 1000, 100);
    cat.thread(1, true, "CLOSED", 3);
    LogInventory inv = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1);
    assertThat(inv.archiveOnlySafeEnd(1000)).isEqualTo(1299);
    assertThatThrownBy(() -> inv.forRange(1050, 1350))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("before the end SCN");
    FakeCatalog later = new FakeCatalog().archivedRun(1, 1, 3, 1100, 100);
    assertThatThrownBy(
            () -> new LogInventory(later, CaptureMode.ARCHIVE_ONLY, 1).forRange(1050, 1250))
        .isInstanceOf(OracleCdcGapException.class)
        .hasMessageContaining("after the start SCN");
    assertThat(cat.sequenceLookups + later.sequenceLookups).isZero();
  }

  @Test
  void allThreadsClosedKeepsTheRulesBeforeAdr0026() throws Exception {
    FakeCatalog cat = crashed().archivedLog(2, 2, 1050, 1081);
    cat.thread(1, true, "CLOSED", 3);
    assertThat(new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1).archiveOnlySafeEnd(1000))
        .as("no thread is left to bound it: the lowest archived end, as before")
        .isEqualTo(1080);
  }

  @Test
  void threadStateChangesAreReportedOnceEach() throws Exception {
    FakeCatalog cat = crashed();
    cat.thread(2, true, "OPEN", 2);
    LogInventory inv = new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, 1);
    inv.archiveOnlySafeEnd(1000);
    assertThat(inv.drainThreadChanges()).as("the first read records the states").isEmpty();
    cat.thread(2, true, "CLOSED", 2).archivedLog(2, 2, 1050, 1081);
    inv.archiveOnlySafeEnd(1000);
    inv.forRange(1049, 1299);
    assertThat(inv.drainThreadChanges())
        .containsExactly(new ThreadStateChange(2, "open", "closed", 2, 1081L));
    cat.disableThread(2);
    inv.forRange(1049, 1299);
    assertThat(inv.drainThreadChanges())
        .containsExactly(new ThreadStateChange(2, "closed", "disabled", 2, null));
    assertThat(inv.drainThreadChanges()).isEmpty();
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
