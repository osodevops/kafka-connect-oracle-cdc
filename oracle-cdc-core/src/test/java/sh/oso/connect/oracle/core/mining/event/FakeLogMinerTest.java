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
package sh.oso.connect.oracle.core.mining.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;

class FakeLogMinerTest {

  @Test
  void replaysTheScriptInsideTheRangeAndFiresFaultsOnce() throws Exception {
    FakeLogMiner fake = new FakeLogMiner().startAt(100);
    TxKey t1 = fake.tx(1, 1, 1);
    fake.start(t1, "APP")
        .insert(t1, FakeLogMiner.DEFAULT_TABLE, "insert 1")
        .update(t1, FakeLogMiner.DEFAULT_TABLE, "update 1")
        .commit(t1);
    assertThat(fake.events()).hasSize(4);
    assertThat(fake.nextScn()).isEqualTo(104);
    fake.faultAt(
        2, new SQLException("ORA-00310: archived log contains sequence 5", "72000", 310), false);

    List<MiningEvent> seen = new ArrayList<>();
    EventCursor c = fake.open(100, 200);
    assertThat(c.next()).isTrue();
    assertThat(c.next()).isTrue();
    assertThatThrownBy(c::next).isInstanceOf(SQLException.class).hasMessageContaining("ORA-00310");
    c = fake.open(100, 200);
    while (c.next()) {
      seen.add(c.event());
    }
    assertThat(seen).hasSize(4);
    assertThat(seen.get(3)).isInstanceOf(MiningEvent.Commit.class);
    assertThat(fake.opened).isEqualTo(2);

    List<MiningEvent> partial = new ArrayList<>();
    c = fake.open(101, 103);
    while (c.next()) {
      partial.add(c.event());
    }
    assertThat(partial).hasSize(2).allMatch(e -> e instanceof MiningEvent.Dml);
    fake.close();
    assertThat(fake.closed).isTrue();
  }
}
