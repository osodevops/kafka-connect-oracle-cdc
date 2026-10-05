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
package sh.oso.connect.oracle.delivery;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.TransactionContext;

/**
 * SRC-EOS-2 and ADR-0007: with {@code transaction.boundary=connector}, a Kafka transaction ends
 * only after a record marked as a boundary (the last record of an Oracle transaction, or a record
 * outside one), once it has reached {@code maxRecords}, {@code maxBytes} or {@code maxMs}, or when
 * no more records are waiting; a forced boundary (a split, SRC-EOS-4) always ends it. The batch
 * handed to Connect stops at that record and the commit is requested for the batch, so the offset
 * committed with the transaction is the boundary record's (SRC-EOS-3); the records after it are
 * held for the next poll.
 */
public final class EosBoundaries {

  private final long maxRecords;
  private final long maxBytes;
  private final long maxMs;
  private List<RecordQueueSink.Queued> held = new ArrayList<>();
  private long records;
  private long bytes;
  private long openedAt = -1;
  private long commits;

  public EosBoundaries(long maxRecords, long maxBytes, long maxMs) {
    this.maxRecords = maxRecords;
    this.maxBytes = maxBytes;
    this.maxMs = maxMs;
  }

  /** Records held back from the last batch; the next poll returns them before draining more. */
  public boolean holding() {
    return !held.isEmpty();
  }

  /**
   * The records to hand to Connect now: the held ones and then {@code drained}, up to the first
   * record that ends a Kafka transaction, for which {@link TransactionContext#commitTransaction()}
   * is requested. {@code queueEmpty}: nothing else is waiting in the record queue.
   */
  public List<SourceRecord> next(
      List<RecordQueueSink.Queued> drained, TransactionContext ctx, long now, boolean queueEmpty) {
    List<RecordQueueSink.Queued> batch = held;
    batch.addAll(drained);
    held = new ArrayList<>();
    List<SourceRecord> out = new ArrayList<>(batch.size());
    for (int i = 0; i < batch.size(); i++) {
      RecordQueueSink.Queued q = batch.get(i);
      out.add(q.record());
      if (openedAt < 0) {
        openedAt = now;
      }
      records++;
      bytes += q.bytes();
      boolean last = i + 1 == batch.size();
      if (q.boundary()
          && (q.force()
              || records >= maxRecords
              || bytes >= maxBytes
              || now - openedAt >= maxMs
              || (last && queueEmpty))) {
        ctx.commitTransaction();
        commits++;
        records = 0;
        bytes = 0;
        openedAt = -1;
        held = new ArrayList<>(batch.subList(i + 1, batch.size()));
        return out;
      }
    }
    return out;
  }

  /** Kafka transaction commits requested so far. */
  public long commits() {
    return commits;
  }
}
