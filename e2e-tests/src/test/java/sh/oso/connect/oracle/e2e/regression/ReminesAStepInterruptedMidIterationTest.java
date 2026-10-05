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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.errors.MiningStepRetryException;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * Invariant: a mining step whose result set fails part way with ORA-00310 (the online log was
 * overwritten while it was read) is discarded and mined again from the same cursor, so nothing of
 * the interrupted range is skipped or applied twice; a failure that persists is a typed stop
 * (CDC-1002), never an end of the batch. Debezium treated the error as the end of the result set
 * and moved its start past the unread rows.
 *
 * <p>Proven here with the real engine over a scripted miner (T0); {@code
 * ReminesAStepInterruptedMidIterationEngineIT} injects the error into a real mining session.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/2504">dbz#2504</a>
 */
@Tag("dbz-2504")
class ReminesAStepInterruptedMidIterationTest {

  static final SQLException ORA_00310 =
      new SQLException(
          "ORA-00310: archived log contains sequence 55897; sequence 55898 required", "72000", 310);

  final ScriptedEngine s = new ScriptedEngine();
  TxKey a;
  TxKey b;
  TxKey c;

  /** A spans both steps; B commits inside the first; C inside the second. */
  long script() {
    a = s.fake.tx(1, 1, 1);
    b = s.fake.tx(1, 2, 2);
    c = s.fake.tx(1, 3, 3);
    s.fake.start(a, "APP");
    for (int i = 1; i <= 10; i++) {
      s.fake.insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(i, "a" + i));
    }
    s.fake.start(b, "APP");
    for (int i = 101; i <= 103; i++) {
      s.fake.insert(b, ScriptedEngine.ORDERS, ScriptedEngine.insert(i, "b" + i));
    }
    s.fake.commit(b);
    long firstStepEnd = s.fake.nextScn();
    for (int i = 11; i <= 20; i++) {
      s.fake.insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(i, "a" + i));
    }
    s.fake.commit(a);
    s.fake.start(c, "APP");
    for (int i = 201; i <= 205; i++) {
      s.fake.insert(c, ScriptedEngine.ORDERS, ScriptedEngine.insert(i, "c" + i));
    }
    s.fake.commit(c);
    return firstStepEnd;
  }

  @Test
  void anInterruptedStepIsMinedAgainAndEveryChangeIsDeliveredOnce() throws Exception {
    long firstStepEnd = script();
    // events 0 to 15 are the first step; the second has returned A's 11th to 15th rows (16 to 20)
    // when its result set fails
    s.fake.faultAt(21, ORA_00310, false);
    CaptureEngine e = s.engine(20);
    s.safeEnd = firstStepEnd;
    s.runUntilIdle(e);
    assertThat(s.committed).extracting(t -> t.key()).containsExactly(b);
    s.safeEnd = s.fake.nextScn() + 1;
    s.runUntilIdle(e);
    assertThat(e.metrics().stepRetries.get()).as("the interrupted step").isEqualTo(1);
    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(b, a, c);
    assertThat(ids(s.committed.get(1))).isEqualTo(range(1, 20));
    assertThat(ids(s.committed.get(2))).isEqualTo(range(201, 205));
    assertThat(s.resume.scn()).as("nothing left open").isEqualTo(s.minedTo);
  }

  @Test
  void aStepThatKeepsFailingStopsWithATypedErrorAndDeliversNothingFromIt() throws Exception {
    long firstStepEnd = script();
    s.fake.faultAt(21, ORA_00310, true);
    CaptureEngine e = s.engine(3);
    s.safeEnd = firstStepEnd;
    s.runUntilIdle(e);
    long cursor = e.cursor().scn();
    s.safeEnd = s.fake.nextScn() + 1;
    for (int i = 0; i < 3; i++) {
      assertThat(e.runOnce()).isEqualTo(CaptureEngine.Progress.STEP_RETRIED);
    }
    assertThatThrownBy(e::runOnce)
        .isInstanceOf(MiningStepRetryException.class)
        .hasMessageContaining("CDC-1002");
    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(b);
    assertThat(e.cursor().scn()).as("the cursor stays before the failing range").isEqualTo(cursor);
  }

  private static List<BigDecimal> ids(CommittedTransaction t) {
    List<BigDecimal> out = new ArrayList<>();
    for (RowChange ch : t.events()) {
      out.add((BigDecimal) ch.after().get("ID"));
    }
    return out;
  }

  private static List<BigDecimal> range(int from, int to) {
    List<BigDecimal> out = new ArrayList<>();
    for (int i = from; i <= to; i++) {
      out.add(new BigDecimal(i));
    }
    return out;
  }
}
