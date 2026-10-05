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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Counters the engine exports; plain fields now, JMX MBeans with P1-23. */
public final class EngineMetrics {
  public final AtomicLong steps = new AtomicLong();
  public final AtomicLong stepRetries = new AtomicLong();
  public final AtomicLong stepTimeouts = new AtomicLong();
  public final AtomicLong stepCuts = new AtomicLong();
  public final AtomicLong rowsMined = new AtomicLong();
  public final AtomicLong eventsApplied = new AtomicLong();
  public final AtomicLong transactionsCommitted = new AtomicLong();
  public final AtomicLong transactionsSkipped = new AtomicLong();
  public final AtomicLong decodeFailures = new AtomicLong();
  public final AtomicLong idlePolls = new AtomicLong();
  public final AtomicLong sessionRecycles = new AtomicLong();
  public final AtomicLong lobInsertsMerged = new AtomicLong();
  public final AtomicLong lobRowsApplied = new AtomicLong();

  /** Published by the engine thread after every step and idle poll, for JMX readers. */
  public volatile sh.oso.connect.oracle.core.buffer.BufferMetricsSnapshot buffer;

  /** The 20 largest open transactions at the same moment (CORE-TX-8). */
  public volatile java.util.List<
          sh.oso.connect.oracle.core.buffer.TransactionBuffer.OpenTransaction>
      largest;

  public final AtomicLong reconnects = new AtomicLong();
  public final AtomicLong orphansReleased = new AtomicLong();
  public final AtomicLong transactionsDiscarded = new AtomicLong();
  public final AtomicLong lastStepMillis = new AtomicLong();
  public final AtomicLong minedToScn = new AtomicLong();
  public final AtomicLong safeEndScn = new AtomicLong();
  public final AtomicInteger windowLogs = new AtomicInteger();
}
