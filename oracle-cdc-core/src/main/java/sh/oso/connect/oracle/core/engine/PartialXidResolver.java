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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.mining.step.StepOutcome;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * LogMiner gives some rows written by a rollback, the ROLLBACK row and the undo rows of a rollback
 * to a savepoint, the transaction sequence {@link sh.oso.connect.oracle.core.model.Xid#PARTIAL_SQN}
 * instead of the real one (seen on GitHub runners, 7 October 2026). An undo slot holds one active
 * transaction at a time, so such a row belongs to the transaction open in its container, undo
 * segment and slot: one buffered before the step, or one whose rows came earlier in the step. A row
 * whose slot has no open transaction undoes nothing that was captured and is dropped. The pairing
 * is only ever made against transactions open at that moment, never kept as an identity.
 */
final class PartialXidResolver {

  private record Slot(int srcConId, long usn, long slot) {
    static Slot of(TxKey k) {
      return new Slot(k.srcConId(), k.xid().usn(), k.xid().slot());
    }
  }

  private long dropped;

  /** Rows dropped because no open transaction held their slot. */
  long dropped() {
    return dropped;
  }

  /** The step's rows in order, each partial XID replaced by the open transaction of its slot. */
  StepOutcome resolve(StepOutcome outcome, Collection<TxKey> open) {
    boolean any = false;
    for (MiningEvent e : outcome.events()) {
      TxKey tx = e.txOrNull();
      if (tx != null && tx.xid().partialSqn()) {
        any = true;
        break;
      }
    }
    if (!any) {
      return outcome;
    }
    Map<Slot, TxKey> active = new HashMap<>();
    for (TxKey k : open) {
      if (!k.xid().partialSqn()) {
        // a slot is reused only after its transaction ended: the higher sequence is the live one
        active.merge(Slot.of(k), k, (a, b) -> a.xid().sqn() >= b.xid().sqn() ? a : b);
      }
    }
    List<MiningEvent> out = new ArrayList<>(outcome.events().size());
    for (MiningEvent e : outcome.events()) {
      TxKey tx = e.txOrNull();
      if (tx == null || tx.xid().isZero()) {
        out.add(e);
        continue;
      }
      if (tx.xid().partialSqn()) {
        TxKey live = active.get(Slot.of(tx));
        if (live == null) {
          dropped++;
          continue;
        }
        e = e.withTx(live);
        tx = live;
      } else {
        active.put(Slot.of(tx), tx);
      }
      out.add(e);
      if (e instanceof MiningEvent.Commit || e instanceof MiningEvent.Rollback) {
        active.remove(Slot.of(tx), tx);
      }
    }
    return new StepOutcome(
        outcome.kind(), out, outcome.next(), outcome.rowsSeen(), outcome.cause());
  }
}
