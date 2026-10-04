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
package sh.oso.connect.oracle.bench.workload;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.atomic.LongAdder;

/** Counters of one generator run; what the ledger must agree with. */
public final class WorkloadResult {
  public final LongAdder committed = new LongAdder();
  public final LongAdder rolledBack = new LongAdder();
  public final LongAdder savepointRollbacks = new LongAdder();
  public final LongAdder inserts = new LongAdder();
  public final LongAdder updates = new LongAdder();
  public final LongAdder deletes = new LongAdder();
  public final LongAdder keyChanges = new LongAdder();
  public final LongAdder lobWrites = new LongAdder();
  public final LongAdder truncates = new LongAdder();
  public final LongAdder ddls = new LongAdder();
  public volatile long durationMs;

  public String toJson() {
    try {
      return new ObjectMapper().writeValueAsString(new Snapshot(this));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Plain values for JSON. */
  public record Snapshot(
      long committed,
      long rolledBack,
      long savepointRollbacks,
      long inserts,
      long updates,
      long deletes,
      long keyChanges,
      long lobWrites,
      long truncates,
      long ddls,
      long durationMs) {
    Snapshot(WorkloadResult r) {
      this(
          r.committed.sum(),
          r.rolledBack.sum(),
          r.savepointRollbacks.sum(),
          r.inserts.sum(),
          r.updates.sum(),
          r.deletes.sum(),
          r.keyChanges.sum(),
          r.lobWrites.sum(),
          r.truncates.sum(),
          r.ddls.sum(),
          r.durationMs);
    }
  }
}
