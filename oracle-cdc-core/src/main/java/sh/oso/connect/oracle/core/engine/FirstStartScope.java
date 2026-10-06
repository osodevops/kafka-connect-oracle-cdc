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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.mining.step.StepOutcome;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.Position;

/**
 * ADR-0019: on a first start that mines from before its start SCN to read open transactions whole,
 * the rows below the start SCN of every other transaction are dropped before they are decoded or
 * buffered. Those transactions ended before the start: decoding them could only stop the task (a
 * type or dictionary the connector cannot read) for a change it would skip. Kept below the floor:
 * transactions {@code GV$TRANSACTION} listed, and transactions whose START row is at or after the
 * SCN read before that query, which covers every transaction open at the start. DDL below the floor
 * is before the start, and the dictionary read at start already reflects it.
 */
final class FirstStartScope {

  private final long floor;
  private final long openScn;
  private final Set<String> open;
  private final Set<TxKey> startedLater = new HashSet<>();

  private FirstStartScope(long floor, long openScn, Set<String> open) {
    this.floor = floor;
    this.openScn = openScn;
    this.open = open;
  }

  /** The scope of {@code start}, or null when it has no first-start floor. */
  static FirstStartScope of(Position start) {
    if (start.hasCommit() || start.startFloorScn() <= 0) {
      return null;
    }
    return new FirstStartScope(
        start.startFloorScn(), start.startOpenScn(), start.startOpenTransactions());
  }

  long floor() {
    return floor;
  }

  /** {@code outcome} without the events of transactions that ended before the start. */
  StepOutcome filter(StepOutcome outcome) {
    List<MiningEvent> kept = null;
    List<MiningEvent> events = outcome.events();
    for (int i = 0; i < events.size(); i++) {
      MiningEvent e = events.get(i);
      boolean keep = keep(e);
      if (!keep && kept == null) {
        kept = new ArrayList<>(events.subList(0, i));
      } else if (keep && kept != null) {
        kept.add(e);
      }
    }
    return kept == null
        ? outcome
        : new StepOutcome(
            outcome.kind(), kept, outcome.next(), outcome.rowsSeen(), outcome.cause());
  }

  private boolean keep(MiningEvent e) {
    if (e.scn() >= floor) {
      return true;
    }
    if (e instanceof MiningEvent.Ddl) {
      return false;
    }
    TxKey tx = txOf(e);
    if (tx == null) {
      return true; // log boundaries and missing-SCN markers are never dropped
    }
    if (e instanceof MiningEvent.TxStart && e.scn() >= openScn) {
      startedLater.add(tx);
    }
    return startedLater.contains(tx) || open.contains(tx.toString());
  }

  private static TxKey txOf(MiningEvent e) {
    if (e instanceof MiningEvent.TxStart s) {
      return s.tx();
    } else if (e instanceof MiningEvent.Dml d) {
      return d.tx();
    } else if (e instanceof MiningEvent.Commit c) {
      return c.tx();
    } else if (e instanceof MiningEvent.Rollback r) {
      return r.tx();
    } else if (e instanceof MiningEvent.Unsupported u) {
      return u.tx();
    } else if (e instanceof MiningEvent.Other o) {
      return o.tx();
    }
    return null;
  }
}
