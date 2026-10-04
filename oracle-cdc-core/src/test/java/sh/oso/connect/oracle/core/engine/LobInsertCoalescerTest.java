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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

class LobInsertCoalescerTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "WL_T3");
  static final TxKey K = new TxKey(3, new Xid(8, 17, 675));
  static final Set<String> LOBS = Set.of("NOTE");

  static Map<String, Object> row(Object id, Object note) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ID", id);
    m.put("NAME", "row");
    m.put("NOTE", note);
    return m;
  }

  static Map<String, Object> named(Object id, String name) {
    Map<String, Object> m = row(id, null);
    m.put("NAME", name);
    return m;
  }

  static RowChange insert(long scn, String rowId, Map<String, Object> after) {
    return new RowChange(
        T,
        Operation.INSERT,
        null,
        after,
        false,
        rowId,
        new RedoRecordId(scn, "0x", 0),
        K,
        Instant.EPOCH);
  }

  static RowChange update(
      long scn, String rowId, Map<String, Object> before, Map<String, Object> after) {
    return new RowChange(
        T,
        Operation.UPDATE,
        before,
        after,
        false,
        rowId,
        new RedoRecordId(scn, "0x", 0),
        K,
        Instant.EPOCH);
  }

  @Test
  void foldsTheLocatorUpdateIntoThePlaceholderInsert() {
    LobInsertCoalescer c = new LobInsertCoalescer();
    RowChange ins = insert(100, LobInsertCoalescer.PLACEHOLDER_ROWID, row(new BigDecimal("1"), ""));
    assertThat(c.accept(K, ins, LOBS)).isEmpty();
    assertThat(c.pendingCount()).isEqualTo(1);
    assertThat(c.oldestPending()).contains(ins.id());
    RowChange locator =
        update(102, "AAAR5N", row(new BigDecimal("1.0"), ""), row(new BigDecimal("1.0"), null));
    List<RowChange> out = c.accept(K, locator, LOBS);
    assertThat(out).hasSize(1);
    RowChange merged = out.get(0);
    assertThat(merged.op()).isEqualTo(Operation.INSERT);
    assertThat(merged.rowId()).isEqualTo("AAAR5N");
    assertThat(merged.after()).containsEntry("NOTE", null).containsEntry("NAME", "row");
    assertThat(merged.id().scn()).isEqualTo(100);
    assertThat(c.pendingCount()).isZero();
    assertThat(c.merged()).isEqualTo(1);
  }

  @Test
  void nonMatchingFollowersReleaseTheInsertUnchanged() {
    LobInsertCoalescer c = new LobInsertCoalescer();
    RowChange ins = insert(100, LobInsertCoalescer.PLACEHOLDER_ROWID, row(1, ""));
    c.accept(K, ins, LOBS);
    RowChange other = update(101, "AAAX", row(2, null), named(2, "changed"));
    assertThat(c.accept(K, other, LOBS)).containsExactly(ins, other);
    RowChange ins2 = insert(103, LobInsertCoalescer.PLACEHOLDER_ROWID, row(3, ""));
    RowChange ins3 = insert(104, LobInsertCoalescer.PLACEHOLDER_ROWID, row(4, ""));
    assertThat(c.accept(K, ins2, LOBS)).isEmpty();
    assertThat(c.accept(K, ins3, LOBS)).containsExactly(ins2);
    assertThat(c.flush(K)).contains(ins3);
    assertThat(c.flush(K)).isEmpty();
    RowChange plain = insert(105, "AAAY", row(5, null));
    assertThat(c.accept(K, plain, LOBS)).containsExactly(plain);
    c.accept(K, insert(106, LobInsertCoalescer.PLACEHOLDER_ROWID, row(6, "")), LOBS);
    c.discard(K);
    assertThat(c.pendingCount()).isZero();
    // an update that changes a non-LOB column is a real update, not a locator
    c.accept(K, insert(107, LobInsertCoalescer.PLACEHOLDER_ROWID, row(7, "")), LOBS);
    RowChange real = update(108, "AAAZ", row(7, ""), named(7, "renamed"));
    assertThat(c.accept(K, real, LOBS)).hasSize(2);
  }
}
