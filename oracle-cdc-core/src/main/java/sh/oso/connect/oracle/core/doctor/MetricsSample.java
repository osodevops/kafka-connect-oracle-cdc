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
package sh.oso.connect.oracle.core.doctor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * One reading of a task's metrics (the {@code TaskMetricsMXBean} attributes, by attribute name such
 * as {@code QueueDepth}) and its largest open transactions, for {@code explain-lag}. A metric the
 * source did not provide reads as -1.
 */
public record MetricsSample(Instant at, Map<String, Long> values, List<OpenTransaction> largest) {

  /** The attributes explain-lag reads. */
  public static final List<String> ATTRIBUTES =
      List.of(
          "Steps",
          "RowsMined",
          "EventsApplied",
          "StepTimeouts",
          "StepRetries",
          "TransactionsCommitted",
          "LagReplays",
          "LastStepMillis",
          "WindowLogs",
          "MinedToScn",
          "SafeEndScn",
          "ScnLag",
          "OpenTransactions",
          "BufferedEvents",
          "BufferHeapBytes",
          "BufferMemoryMaxBytes",
          "OldestOpenScn",
          "SpilledTransactions",
          "SpilledBytes",
          "JournaledTransactions",
          "QueueDepth",
          "MillisBehindSource");

  /** One entry of {@code LargestTransactions}. */
  public record OpenTransaction(
      String xid,
      String username,
      long ageMillis,
      long firstScn,
      long events,
      long heapBytes,
      long spilledBytes,
      boolean journaled) {
    public long bytes() {
      return heapBytes + spilledBytes;
    }
  }

  public MetricsSample {
    values = Map.copyOf(values);
    largest = List.copyOf(largest);
  }

  public long get(String attribute) {
    Long v = values.get(attribute);
    return v == null ? -1 : v;
  }
}
