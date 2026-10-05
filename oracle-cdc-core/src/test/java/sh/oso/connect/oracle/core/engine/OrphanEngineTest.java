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
package sh.oso.connect.oracle.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.OrphanReleaseViolationException;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.orphan.OrphanDetector;
import sh.oso.connect.oracle.core.orphan.TransactionProbe;

/** CORE-TX-7 in the engine loop: a confirmed orphan is discarded; its late COMMIT is a stop. */
class OrphanEngineTest {

  static final class FakeProbe implements TransactionProbe {
    final Set<TxKey> active = new HashSet<>();
    long scn = 1000;

    @Override
    public Set<TxKey> activeTransactions() {
      return Set.copyOf(active);
    }

    @Override
    public boolean sessionExists(long sessionNo, long serialNo) {
      return false;
    }

    @Override
    public long currentScn() {
      return scn;
    }
  }

  @Test
  void anOrphanIsReleasedWithoutEmittingAndItsLateCommitStopsTheTask() throws Exception {
    CaptureEngineTest.Harness h = new CaptureEngineTest.Harness();
    FakeProbe probe = new FakeProbe();
    OrphanDetector detector =
        new OrphanDetector(probe, Duration.ofMinutes(5), OrphanDetector.Action.RELEASE, List.of());
    TxKey a = h.fake.tx(1, 1, 1);
    TxKey b = h.fake.tx(1, 1, 2);
    h.fake
        .start(a, "APP")
        .insert(a, CaptureEngineTest.T, "a1")
        .start(b, "APP")
        .insert(b, CaptureEngineTest.T, "b1")
        .commit(b);
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL).withOrphanDetector(detector);
    h.runUntilIdle(e);
    assertThat(h.sink.sqls()).containsExactly("b1");
    assertThat(h.buffer.openTransactions()).isEqualTo(1);
    // a is young: the first due check finds it absent (first negative)
    h.clock.advance(Duration.ofMinutes(6));
    probe.scn = h.fake.nextScn() - 1; // the engine has mined past this SCN already
    h.runUntilIdle(e);
    assertThat(h.buffer.openTransactions()).as("one absence is not enough").isEqualTo(1);
    // second check: still absent, mined through, START row's session is gone: released
    h.clock.advance(Duration.ofMinutes(6));
    h.runUntilIdle(e);
    assertThat(h.buffer.openTransactions()).isZero();
    assertThat(e.metrics().orphansReleased.get()).isEqualTo(1);
    assertThat(detector.released()).containsExactly(a.toString());
    assertThat(h.sink.sqls()).as("nothing of a was emitted").containsExactly("b1");
    // the resume candidate no longer pins on a
    assertThat(h.buffer.oldestFirstCaptured()).isEmpty();
    // a COMMIT for the released transaction is a typed stop, never a partial emission
    h.fake.insert(a, CaptureEngineTest.T, "a2").commit(a);
    h.safeEnd = h.fake.nextScn();
    assertThatThrownBy(() -> h.runUntilIdle(e))
        .isInstanceOf(OrphanReleaseViolationException.class)
        .hasMessageContaining("CDC-7001");
    assertThat(h.sink.sqls()).containsExactly("b1");
  }

  @Test
  void anActiveTransactionIsNeverReleased() throws Exception {
    CaptureEngineTest.Harness h = new CaptureEngineTest.Harness();
    FakeProbe probe = new FakeProbe();
    OrphanDetector detector =
        new OrphanDetector(probe, Duration.ofMinutes(5), OrphanDetector.Action.RELEASE, List.of());
    TxKey a = h.fake.tx(1, 1, 1);
    probe.active.add(a);
    h.fake.start(a, "APP").insert(a, CaptureEngineTest.T, "a1");
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL).withOrphanDetector(detector);
    for (int i = 0; i < 4; i++) {
      h.clock.advance(Duration.ofMinutes(6));
      h.runUntilIdle(e);
    }
    assertThat(h.buffer.openTransactions()).isEqualTo(1);
    assertThat(e.metrics().orphansReleased.get()).isZero();
    h.fake.commit(a);
    h.safeEnd = h.fake.nextScn();
    h.runUntilIdle(e);
    assertThat(h.sink.sqls()).containsExactly("a1");
  }
}
