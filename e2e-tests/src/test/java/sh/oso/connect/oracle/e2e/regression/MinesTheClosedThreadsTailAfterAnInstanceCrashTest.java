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

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;

/**
 * ADR-0026, CORE-LOG-3: when a RAC instance crashes, V$THREAD shows its thread CLOSED and still
 * enabled. The redo it wrote before the crash (its tail) is in its last log, which a surviving
 * instance archives during recovery; after that the thread writes nothing until the instance opens
 * again. Treating the closed thread like an open one, the inventory waited for a log after its last
 * one forever: in archive-only mode the safe end stayed at the end of its last archived log, and in
 * online mode every step was cut there as a log switch under way. Capture of the surviving instance
 * stopped without an error. Found by design review of the per-thread position (9 October 2026),
 * before RAC is qualified.
 *
 * <p>The thread's tail must still be mined, and nothing may be mined past the closed thread's last
 * log while it is not archived. When the instance opens again its thread is required from its next
 * log, with no reset and no duplicates.
 */
@Tag("rac-instance-crash")
class MinesTheClosedThreadsTailAfterAnInstanceCrashTest {

  static final Position START = Position.initial(1000, new DatabaseIdentity(1, 1));

  @Test
  void inArchiveOnlyModeTheTailIsMinedOnceArchivedAndTheOtherThreadCarriesOn() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    FakeCatalog cat = new FakeCatalog();
    cat.threads.clear();
    // thread 1 switches every 100 SCNs; thread 2's sequence 2 began at 1050 and is current when
    // instance 2 crashes at about 1080
    cat.archivedRun(1, 1, 3, 1000, 100).archivedLog(2, 1, 1000, 1050);
    cat.thread(1, true, "OPEN", 4).thread(2, true, "CLOSED", 2);
    s.catalog = cat;
    s.mode = CoreConfig.CaptureMode.ARCHIVE_ONLY;
    s.safeEnds = () -> archiveOnlySafeEnd(s);
    s.boundToLogs = true;

    FakeLogMiner f = s.fake.perThreadRba();
    commit(f.startAt(1001).onThread(1), f.tx(1, 1, 1), 1, "a1");
    commit(f.startAt(1060).onThread(2), f.tx(2, 1, 1), 101, "w1"); // the tail of thread 2
    commit(f.startAt(1150).onThread(1), f.tx(1, 2, 1), 2, "b1");
    commit(f.startAt(1250).onThread(1), f.tx(1, 3, 1), 3, "c1");

    CaptureEngine e = s.engine(3, START).withExpectedThreads(Set.of(1, 2));
    s.runUntilIdle(e);
    assertThat(names(s))
        .as("while the closed thread's last log is not archived, nothing past it is mined")
        .containsExactly("a1");
    assertThat(e.cursor().scn()).isLessThan(1050);

    // a surviving instance archives the closed thread's last log during instance recovery
    cat.archivedLog(2, 2, 1050, 1081);
    s.runUntilIdle(e);
    assertThat(names(s))
        .as("the closed thread's tail, then the open thread's later commits")
        .containsExactly("a1", "w1", "b1", "c1");

    // instance 2 opens again at 1350: its thread is required from its next log, sequence 3
    commit(f.startAt(1360).onThread(2), f.tx(2, 2, 1), 102, "x1");
    commit(f.startAt(1370).onThread(1), f.tx(1, 4, 1), 4, "d1");
    cat.archivedLog(1, 4, 1300, 1400).archivedLog(2, 3, 1350, 1400);
    cat.thread(1, true, "OPEN", 5).thread(2, true, "OPEN", 4);
    s.runUntilIdle(e);
    assertThat(names(s))
        .as("after the instance opened again: no reset, no duplicates")
        .containsExactly("a1", "w1", "b1", "c1", "x1", "d1");
    assertThat(s.threadStates).containsExactly("2 open (was closed)");
  }

  @Test
  void inOnlineModeTheClosedThreadsArchivedLastLogDoesNotHoldTheStepEnd() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    FakeCatalog cat = new FakeCatalog();
    cat.threads.clear();
    cat.archivedRun(1, 1, 2, 1000, 100).onlineCurrent(1, 3, 1200);
    // the closed thread's last log is archived; its group in V$LOG is no longer CURRENT
    cat.archivedLog(2, 1, 1000, 1050).archivedLog(2, 2, 1050, 1081);
    cat.onlineLog(2, 2, 1050, 1081, "INACTIVE");
    cat.thread(1, true, "OPEN", 3).thread(2, true, "CLOSED", 2);
    s.catalog = cat;
    s.boundToLogs = true;

    FakeLogMiner f = s.fake.perThreadRba();
    commit(f.startAt(1001).onThread(1), f.tx(1, 1, 1), 1, "a1");
    commit(f.startAt(1060).onThread(2), f.tx(2, 1, 1), 101, "w1");
    commit(f.startAt(1150).onThread(1), f.tx(1, 2, 1), 2, "b1");
    commit(f.startAt(1250).onThread(1), f.tx(1, 3, 1), 3, "c1");
    s.safeEnd = 1300; // the current SCN

    CaptureEngine e = s.engine(3, START).withExpectedThreads(Set.of(1, 2));
    s.runUntilIdle(e);
    assertThat(names(s)).containsExactly("a1", "w1", "b1", "c1");
    assertThat(e.cursor().scn()).isEqualTo(1300);
  }

  /** As the task computes it: from the start's resume SCN (JdbcEngineFactory). */
  static long archiveOnlySafeEnd(ScriptedEngine s) {
    try {
      return s.inventory.archiveOnlySafeEnd(START.resumeScn());
    } catch (java.sql.SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  static void commit(FakeLogMiner f, TxKey tx, int id, String name) {
    f.start(tx, "APP")
        .insert(tx, ScriptedEngine.ORDERS, ScriptedEngine.insert(id, name))
        .commit(tx);
  }

  static List<String> names(ScriptedEngine s) {
    return s.committed.stream()
        .flatMap(t -> t.events().stream())
        .map(c -> String.valueOf(c.after().get("NAME")))
        .toList();
  }
}
