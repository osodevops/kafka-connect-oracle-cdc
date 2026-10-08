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
package sh.oso.connect.oracle.core.topology;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import sh.oso.connect.oracle.core.errors.TopologyException;

/**
 * ADR-0023: the database shapes this release mines. The task checks the topology before it reads an
 * offset, the doctor reports the same check as DOC-13, and the engine stops on redo from a thread
 * the start did not qualify. The redo byte address cursor (ADR-0014) and the commit order are per
 * thread, so a second enabled redo thread (RAC) would be skipped or repeated by the one cursor this
 * release keeps; it is refused until the per-thread position exists (CORE-POS-4).
 */
public final class TopologyGuard {

  private TopologyGuard() {}

  /** Stops a start on a shape this release does not capture. */
  public static void requireQualified(Topology topology) {
    Set<Integer> enabled = qualifiedThreads(topology);
    if (enabled.size() > 1) {
      throw new TopologyException(
          "The database has "
              + enabled.size()
              + " enabled redo threads ("
              + describe(topology.threads())
              + "). This release captures a single redo thread: with one cursor the connector"
              + " would skip or repeat whole threads.",
          "Run the connector against a single-instance database. RAC capture needs the per-thread"
              + " position (ADR-0023).");
    }
  }

  /** The redo threads a qualified start mines: every enabled one. */
  public static Set<Integer> qualifiedThreads(Topology topology) {
    return topology.threads().stream()
        .filter(ThreadInfo::enabled)
        .map(ThreadInfo::thread)
        .collect(Collectors.toUnmodifiableSet());
  }

  /** {@code thread 1 OPEN, thread 2 disabled}: one entry per V$THREAD row. */
  public static String describe(List<ThreadInfo> threads) {
    return threads.stream()
        .map(t -> "thread " + t.thread() + " " + (t.enabled() ? t.status() : "disabled"))
        .collect(Collectors.joining(", "));
  }
}
