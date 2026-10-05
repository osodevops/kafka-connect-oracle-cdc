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
package sh.oso.connect.oracle.core.orphan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer.OpenTransaction;
import sh.oso.connect.oracle.core.errors.OrphanTransactionException;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/** ADR-0006: three negatives before a release, a bounded ledger, and the fail action. */
class OrphanDetectorTest {

  static final Instant T0 = Instant.parse("2026-10-05T08:00:00Z");

  static final class FakeProbe implements TransactionProbe {
    final Set<TxKey> active = new HashSet<>();
    final Set<String> sessions = new HashSet<>();
    long scn = 1000;
    int calls;

    @Override
    public Set<TxKey> activeTransactions() {
      calls++;
      return Set.copyOf(active);
    }

    @Override
    public boolean sessionExists(long sessionNo, long serialNo) {
      return sessions.contains(sessionNo + "," + serialNo);
    }

    @Override
    public long currentScn() {
      return scn;
    }
  }

  static TxKey tx(int n) {
    return new TxKey(3, new Xid(n, 1, 1));
  }

  static OpenTransaction open(TxKey key, Instant firstSeen, long session, long serial) {
    return new OpenTransaction(
        key, "APP", "c", 500, 600, firstSeen, session, serial, 4, 100, 0, false);
  }

  @Test
  void releasesOnlyAfterTwoAbsencesMinedThroughAndWithTheSessionGone() throws Exception {
    FakeProbe probe = new FakeProbe();
    OrphanDetector d =
        new OrphanDetector(probe, Duration.ofMinutes(5), OrphanDetector.Action.RELEASE, List.of());
    TxKey a = tx(1);
    List<OpenTransaction> open = List.of(open(a, T0, 11, 7));
    // too young: nothing is even looked up (this first call still starts the interval clock)
    assertThat(d.check(T0.plusSeconds(1), 900, open)).isEmpty();
    assertThat(probe.calls).isZero();
    // old enough and still active: no negative recorded
    probe.active.add(a);
    assertThat(d.check(T0.plusSeconds(302), 900, open)).isEmpty();
    assertThat(probe.calls).isEqualTo(1);
    // first absence at SCN 1000: wait for confirmation
    probe.active.clear();
    probe.scn = 1000;
    assertThat(d.check(T0.plusSeconds(603), 900, open)).isEmpty();
    // second absence but the cursor has not reached SCN 1000: the end row may be ahead
    probe.scn = 1100;
    assertThat(d.check(T0.plusSeconds(904), 950, open)).isEmpty();
    // mined through, but the owning session is alive: GV$TRANSACTION may lag
    probe.sessions.add("11,7");
    assertThat(d.check(T0.plusSeconds(1205), 1200, open)).isEmpty();
    // session gone: release
    probe.sessions.clear();
    List<OrphanDetector.Release> rel = d.check(T0.plusSeconds(1506), 1200, open);
    assertThat(rel).hasSize(1);
    assertThat(rel.get(0).tx().key()).isEqualTo(a);
    assertThat(rel.get(0).absentAtScn()).isEqualTo(1000);
    assertThat(rel.get(0).reason()).contains("session 11,7 gone").contains("mined to SCN 1200");
    assertThat(d.released()).containsExactly(a.toString());
    assertThat(d.wasReleased(a)).isTrue();
    assertThat(d.wasReleased(tx(2))).isFalse();
  }

  @Test
  void anActiveTransactionResetsItsFirstAbsenceAndChecksRunOnlyWhenDue() throws Exception {
    FakeProbe probe = new FakeProbe();
    OrphanDetector d =
        new OrphanDetector(probe, Duration.ofMinutes(1), OrphanDetector.Action.RELEASE, List.of());
    TxKey a = tx(1);
    List<OpenTransaction> open = List.of(open(a, T0, 0, 0));
    assertThat(d.check(T0.plusSeconds(61), 5000, open)).as("first absence").isEmpty();
    assertThat(d.check(T0.plusSeconds(90), 5000, open)).as("not due").isEmpty();
    assertThat(probe.calls).isEqualTo(1);
    probe.active.add(a); // it reappears (GV$TRANSACTION lag)
    assertThat(d.check(T0.plusSeconds(122), 5000, open)).isEmpty();
    probe.active.clear();
    assertThat(d.check(T0.plusSeconds(183), 5000, open))
        .as("counts as a first absence again")
        .isEmpty();
    // START not mined (session 0): the session negative is waived
    assertThat(d.check(T0.plusSeconds(244), 5000, open)).hasSize(1);
    assertThat(d.check(T0.plusSeconds(305), 5000, List.of()))
        .as("released entries are gone")
        .isEmpty();
  }

  @Test
  void failActionStopsInsteadOfReleasing() throws Exception {
    FakeProbe probe = new FakeProbe();
    OrphanDetector d =
        new OrphanDetector(probe, Duration.ofMinutes(1), OrphanDetector.Action.FAIL, List.of());
    List<OpenTransaction> open = List.of(open(tx(1), T0, 0, 0));
    d.check(T0.plusSeconds(61), 5000, open);
    assertThatThrownBy(() -> d.check(T0.plusSeconds(122), 5000, open))
        .isInstanceOf(OrphanTransactionException.class)
        .hasMessageContaining("orphaned")
        .satisfies(
            e -> assertThat(((OrphanTransactionException) e).code().code()).isEqualTo("CDC-7002"));
    assertThat(d.released()).isEmpty();
  }

  @Test
  void theLedgerStartsFromThePositionAndStaysBounded() throws Exception {
    FakeProbe probe = new FakeProbe();
    List<String> carried = List.of("3:9.9.9");
    OrphanDetector d =
        new OrphanDetector(probe, Duration.ofMinutes(1), OrphanDetector.Action.RELEASE, carried);
    assertThat(d.wasReleased(new TxKey(3, new Xid(9, 9, 9)))).isTrue();
    Instant now = T0;
    for (int i = 1; i <= OrphanDetector.LEDGER_MAX + 10; i++) {
      List<OpenTransaction> open = List.of(open(tx(i), T0, 0, 0));
      now = now.plusSeconds(61);
      d.check(now, 5000, open);
      now = now.plusSeconds(61);
      assertThat(d.check(now, 5000, open)).hasSize(1);
    }
    assertThat(d.released()).hasSize(OrphanDetector.LEDGER_MAX);
    assertThat(d.wasReleased(new TxKey(3, new Xid(9, 9, 9)))).as("oldest dropped").isFalse();
    assertThat(d.wasReleased(tx(OrphanDetector.LEDGER_MAX + 10))).isTrue();
    assertThat(OrphanDetector.disabled().check(now, 1, List.of(open(tx(1), T0, 0, 0)))).isEmpty();
  }
}
