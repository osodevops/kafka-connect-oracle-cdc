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
package sh.oso.connect.oracle.e2e.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static sh.oso.connect.oracle.e2e.regression.MinesTheClosedThreadsTailAfterAnInstanceCrashTest.START;
import static sh.oso.connect.oracle.e2e.regression.MinesTheClosedThreadsTailAfterAnInstanceCrashTest.commit;
import static sh.oso.connect.oracle.e2e.regression.MinesTheClosedThreadsTailAfterAnInstanceCrashTest.names;

import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;

/**
 * ADR-0026, CORE-LOG-3: a RAC thread disabled mid-run (its instance removed from the cluster) has
 * its remaining logs mined and is then no longer waited for; enabled again, or enabled for the
 * first time, it is required from its first new log. Its new log starts at an SCN after the end of
 * its previous one, because the thread wrote nothing in between. The log sequences are contiguous,
 * so no redo is missing, but the inventory saw a log starting after the step's start and stopped
 * the task with a gap (CDC-2001) that was not there. Found by design review of the per-thread
 * position (9 October 2026), before RAC is qualified.
 */
@Tag("rac-thread-disabled")
class HandlesAThreadDisabledMidRunTest {

  @Test
  void aThreadDisabledMidRunIsDrainedThenNotWaitedForAndRequiredAgainOnceEnabled()
      throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    FakeCatalog cat = new FakeCatalog();
    cat.threads.clear();
    cat.archivedRun(1, 1, 3, 1000, 100).archivedLog(2, 1, 1000, 1080);
    cat.thread(1, true, "OPEN", 4).thread(2, true, "OPEN", 2);
    s.catalog = cat;
    s.mode = CoreConfig.CaptureMode.ARCHIVE_ONLY;
    s.safeEnds = () -> MinesTheClosedThreadsTailAfterAnInstanceCrashTest.archiveOnlySafeEnd(s);
    s.boundToLogs = true;

    FakeLogMiner f = s.fake.perThreadRba();
    commit(f.startAt(1001).onThread(1), f.tx(1, 1, 1), 1, "a1");
    commit(f.startAt(1050).onThread(2), f.tx(2, 1, 1), 101, "w1");
    commit(f.startAt(1100).onThread(2), f.tx(2, 2, 1), 102, "v1"); // in thread 2's last log
    commit(f.startAt(1150).onThread(1), f.tx(1, 2, 1), 2, "b1");
    commit(f.startAt(1250).onThread(1), f.tx(1, 3, 1), 3, "c1");

    CaptureEngine e = s.engine(3, START).withExpectedThreads(Set.of(1, 2));
    s.runUntilIdle(e);
    assertThat(names(s)).as("an open thread bounds the safe end").containsExactly("a1", "w1");

    // instance 2 stops, its last log is archived and its thread disabled
    cat.archivedLog(2, 2, 1080, 1120);
    cat.disableThread(2);
    s.runUntilIdle(e);
    assertThat(names(s))
        .as("the disabled thread's last log is mined, then it is not waited for")
        .containsExactly("a1", "w1", "v1", "b1", "c1");
    assertThat(e.cursor().scn()).isEqualTo(1299);

    // the thread is enabled again and its instance opens at 1350, in its next sequence
    commit(f.startAt(1360).onThread(2), f.tx(2, 3, 1), 103, "x1");
    commit(f.startAt(1370).onThread(1), f.tx(1, 4, 1), 4, "d1");
    cat.thread(2, true, "OPEN", 3);
    s.runUntilIdle(e);
    assertThat(names(s))
        .as("enabled again, thread 2 is waited for until its first new log is archived")
        .containsExactly("a1", "w1", "v1", "b1", "c1");
    cat.archivedLog(1, 4, 1300, 1400).archivedLog(2, 3, 1350, 1400);
    cat.thread(1, true, "OPEN", 5).thread(2, true, "OPEN", 4);
    s.runUntilIdle(e);
    assertThat(names(s))
        .as("required from its first new log, which starts after the step's start")
        .containsExactly("a1", "w1", "v1", "b1", "c1", "x1", "d1");
    assertThat(s.threadStates)
        .as("each state change is reported once (CORE-LOG-3)")
        .containsExactly("2 disabled (was open)", "2 open (was disabled)");
  }

  @Test
  void aThreadEnabledForTheFirstTimeMidRunIsRequiredFromItsFirstLog() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    FakeCatalog cat = new FakeCatalog();
    cat.threads.clear();
    cat.archivedRun(1, 1, 3, 1000, 100);
    cat.thread(1, true, "OPEN", 4).thread(2, false, "CLOSED", 0); // created, never enabled
    s.catalog = cat;
    s.mode = CoreConfig.CaptureMode.ARCHIVE_ONLY;
    s.safeEnds = () -> MinesTheClosedThreadsTailAfterAnInstanceCrashTest.archiveOnlySafeEnd(s);
    s.boundToLogs = true;

    FakeLogMiner f = s.fake.perThreadRba();
    commit(f.startAt(1001).onThread(1), f.tx(1, 1, 1), 1, "a1");
    CaptureEngine e = s.engine(3, START).withExpectedThreads(Set.of(1, 2));
    s.runUntilIdle(e);
    assertThat(names(s)).containsExactly("a1");
    assertThat(e.cursor().scn()).isEqualTo(1299);

    // thread 2 is enabled and its instance opens at 1350, writing sequence 1
    commit(f.startAt(1360).onThread(2), f.tx(2, 1, 1), 101, "x1");
    commit(f.startAt(1370).onThread(1), f.tx(1, 2, 1), 2, "b1");
    cat.thread(2, true, "OPEN", 1);
    cat.archivedLog(1, 4, 1300, 1400).archivedLog(1, 5, 1400, 1500);
    s.runUntilIdle(e);
    assertThat(names(s))
        .as("the new thread is waited for until its first log is archived")
        .containsExactly("a1");
    cat.archivedLog(2, 1, 1350, 1450);
    cat.thread(2, true, "OPEN", 2);
    s.runUntilIdle(e);
    assertThat(names(s)).containsExactly("a1", "x1", "b1");
    assertThat(e.cursor().scn()).isEqualTo(1449);
    assertThat(s.threadStates).containsExactly("2 open (was disabled)");
  }
}
