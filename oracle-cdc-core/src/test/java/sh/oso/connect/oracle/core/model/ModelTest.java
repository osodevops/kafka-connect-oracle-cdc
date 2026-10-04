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
package sh.oso.connect.oracle.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ModelTest {

  @Test
  void xidParsesAndOrdersLikeLogMinerColumns() {
    Xid x = Xid.parse("7.23.1234");
    assertThat(x).isEqualTo(new Xid(7, 23, 1234));
    assertThat(x.toString()).isEqualTo("7.23.1234");
    assertThat(Xid.ZERO.isZero()).isTrue();
    assertThat(x.isZero()).isFalse();
    assertThat(new Xid(7, 23, 1233)).isLessThan(x);
    assertThat(new Xid(7, 22, 9999)).isLessThan(x);
    assertThat(new Xid(6, 99, 9999)).isLessThan(x);
    assertThatThrownBy(() -> Xid.parse("1.2")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void txKeySeparatesTheSameXidInTwoPdbs() {
    TxKey a = new TxKey(3, Xid.parse("1.1.1"));
    TxKey b = new TxKey(4, Xid.parse("1.1.1"));
    assertThat(a).isNotEqualTo(b).isLessThan(b);
    assertThat(a.toString()).isEqualTo("3:1.1.1");
  }

  @Test
  void redoRecordIdOrdersByScnThenRbaThenSsn() {
    RedoRecordId a = new RedoRecordId(100, " 0x000003.00000a1c.0010 ", 0);
    RedoRecordId b = new RedoRecordId(100, " 0x000003.00000a1c.0010 ", 1);
    RedoRecordId c = new RedoRecordId(100, " 0x000003.00000a1d.0010 ", 0);
    RedoRecordId d = new RedoRecordId(101, " 0x000003.00000001.0000 ", 0);
    assertThat(a).isLessThan(b);
    assertThat(b).isLessThan(c);
    assertThat(c).isLessThan(d);
    assertThat(a.toString()).isEqualTo("100/0x000003.00000a1c.0010/0");
  }

  @Test
  void tableIdFqnDependsOnPdb() {
    assertThat(new TableId("FREEPDB1", "APP", "ORDERS").fqn()).isEqualTo("FREEPDB1.APP.ORDERS");
    assertThat(new TableId(null, "APP", "ORDERS").fqn()).isEqualTo("APP.ORDERS");
    assertThat(new TableId("p", "s", "t").fqnUpper()).isEqualTo("P.S.T");
  }

  @Test
  void operationCodesRoundTrip() {
    for (Operation op : Operation.values()) {
      if (op != Operation.UNKNOWN) {
        assertThat(Operation.fromCode(op.code())).isEqualTo(op);
      }
    }
    assertThat(Operation.fromCode(99)).isEqualTo(Operation.UNKNOWN);
    assertThat(Operation.INSERT.isRowChange()).isTrue();
    assertThat(Operation.LOB_WRITE.isRowChange()).isTrue();
    assertThat(Operation.COMMIT.isRowChange()).isFalse();
    assertThat(Operation.COMMIT.isTransactionControl()).isTrue();
    assertThat(Operation.DDL.isTransactionControl()).isFalse();
  }
}
