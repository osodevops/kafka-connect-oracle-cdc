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
package sh.oso.connect.oracle.core.position;

import sh.oso.connect.oracle.core.buffer.CommittedTransaction;

/**
 * CORE-POS-3: after a restart, commits at or before the acknowledged commit are skipped entirely
 * except the acknowledged one itself, whose first {@code eventIndex} events are skipped. Before any
 * commit is acknowledged, commits below the first-start floor are skipped (ADR-0019).
 */
public final class SkipRule {

  private SkipRule() {}

  /** Number of leading events of {@code tx} to skip under {@code position}; may equal its size. */
  public static int eventsToSkip(Position position, CommittedTransaction tx) {
    if (position.perThread()) {
      return perThread(position, tx);
    }
    if (!position.hasCommit()) {
      return position.beforeFirstStart(tx.commitScn()) ? tx.size() : 0;
    }
    int c =
        CommitOrder.compare(
            tx.commitId(),
            tx.thread(),
            tx.key(),
            position.lastCommitId(),
            position.lastCommitThread(),
            position.lastCommitKey());
    if (c < 0) {
      return tx.size();
    }
    if (c == 0) {
      return Math.min(position.eventIndex(), tx.size());
    }
    return 0;
  }

  /**
   * ADR-0026: a commit is compared only with the last acknowledged commit of its own thread. The
   * connector emits whole transactions one after another and each record's position holds, per
   * thread, the last commit emitted before it, so a commit at or before its thread's mark was
   * emitted whole, unless it is the globally last acknowledged commit, which resumes at its event
   * index. A thread with no acknowledged commit has nothing to skip.
   */
  private static int perThread(Position position, CommittedTransaction tx) {
    ThreadMark mark = position.threads().get(tx.thread());
    if (mark == null || !mark.hasCommit()) {
      return 0;
    }
    int c =
        CommitOrder.compare(
            tx.commitId(), tx.thread(), tx.key(), mark.commit(), tx.thread(), mark.commitKey());
    if (c > 0) {
      return 0;
    }
    boolean globalLast =
        c == 0
            && tx.thread() == position.lastCommitThread()
            && tx.key().equals(position.lastCommitKey());
    return globalLast ? Math.min(position.eventIndex(), tx.size()) : tx.size();
  }
}
