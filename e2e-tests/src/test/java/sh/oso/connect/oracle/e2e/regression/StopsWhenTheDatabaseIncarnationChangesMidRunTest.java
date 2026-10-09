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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLRecoverableException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.errors.TopologyException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;

/**
 * A failover or a point-in-time recovery opens the database with RESETLOGS: a new incarnation whose
 * log sequences start again at 1. The offset's identity (DBID and RESETLOGS SCN) was checked only
 * at task start (found by code review, 9 October 2026), so a task that lost its connection to a
 * primary and reconnected to the new one went on mining with a cursor of the old incarnation, and
 * the redo byte address filter skipped the new incarnation's rows. After every reconnect the
 * identity is read again; a new incarnation stops the task before anything of it is applied.
 */
@Tag("incarnation-change")
class StopsWhenTheDatabaseIncarnationChangesMidRunTest {

  private static final DatabaseIdentity ORIGINAL = new DatabaseIdentity(1, 1);

  @Test
  void aReconnectToANewIncarnationStopsTheTaskBeforeItsRedoIsApplied() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    AtomicReference<DatabaseIdentity> connected = new AtomicReference<>(ORIGINAL);
    s.reconnector = reconnectTo(s, connected, new DatabaseIdentity(1, 5_000));
    TxKey a = s.fake.tx(1, 1, 1);
    TxKey b = s.fake.tx(2, 2, 2);
    s.fake
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "before the failover"))
        .commit(a);
    s.safeEnd = s.fake.nextScn();
    CaptureEngine e = s.engine(3).withIdentityCheck(connected::get);
    s.runUntilIdle(e);
    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(a);

    // the primary fails over: the connection drops and the standby opens with RESETLOGS
    s.fake.failNextOpen(new SQLRecoverableException("ORA-03113: end-of-file", "08006", 3113));
    s.fake
        .start(b, "APP")
        .insert(b, ScriptedEngine.ORDERS, ScriptedEngine.insert(2, "after the failover"))
        .commit(b);
    s.safeEnd = s.fake.nextScn();

    assertThatThrownBy(() -> s.runUntilIdle(e))
        .isInstanceOf(TopologyException.class)
        .hasMessageContaining("CDC-5001")
        .hasMessageContaining("RESETLOGS");
    assertThat(s.committed)
        .as("nothing mined from the new incarnation is delivered")
        .extracting(CommittedTransaction::key)
        .containsExactly(a);
  }

  @Test
  void aReconnectToTheSameIncarnationContinuesFromTheCursor() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    AtomicReference<DatabaseIdentity> connected = new AtomicReference<>(ORIGINAL);
    // a switchover or a dropped connection: same DBID, same RESETLOGS SCN
    s.reconnector = reconnectTo(s, connected, ORIGINAL);
    TxKey a = s.fake.tx(1, 1, 1);
    TxKey b = s.fake.tx(2, 2, 2);
    s.fake
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "a"))
        .commit(a);
    s.safeEnd = s.fake.nextScn();
    CaptureEngine e = s.engine(3).withIdentityCheck(connected::get);
    s.runUntilIdle(e);

    s.fake.failNextOpen(new SQLRecoverableException("ORA-03113: end-of-file", "08006", 3113));
    s.fake
        .start(b, "APP")
        .insert(b, ScriptedEngine.ORDERS, ScriptedEngine.insert(2, "b"))
        .commit(b);
    s.safeEnd = s.fake.nextScn();
    s.runUntilIdle(e);

    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(a, b);
    assertThat(e.metrics().reconnects.get()).isEqualTo(1);
  }

  /** Reconnects to the same scripted redo, now reporting {@code after} as its identity. */
  private static CaptureEngine.Reconnector reconnectTo(
      ScriptedEngine s, AtomicReference<DatabaseIdentity> connected, DatabaseIdentity after) {
    return cause -> {
      connected.set(after);
      return new CaptureEngine.Sources(
          s.fake,
          new LogInventory(
              new FakeCatalog().archivedRun(1, 1, 40, 1000, 100), CoreConfig.CaptureMode.ONLINE, 1),
          () -> s.safeEnd);
    };
  }
}
