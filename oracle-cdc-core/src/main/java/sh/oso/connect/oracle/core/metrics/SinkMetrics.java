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
package sh.oso.connect.oracle.core.metrics;

/** What the connector's record sink counts; implemented by the task's record queue. */
public interface SinkMetrics {
  long heartbeatsSent();

  long opsEvents();

  long journalChunks();

  long journalTombstones();

  long dlqRecords();

  int queued();

  /** Commit time of the last committed transaction queued, in epoch milliseconds, or -1. */
  long lastCommitTimestampMillis();

  /** When it was queued, minus its commit time, in milliseconds, or -1. */
  long millisBehindSource();

  SinkMetrics NONE =
      new SinkMetrics() {
        public long heartbeatsSent() {
          return 0;
        }

        public long opsEvents() {
          return 0;
        }

        public long journalChunks() {
          return 0;
        }

        public long journalTombstones() {
          return 0;
        }

        public long dlqRecords() {
          return 0;
        }

        public int queued() {
          return 0;
        }

        public long lastCommitTimestampMillis() {
          return -1;
        }

        public long millisBehindSource() {
          return -1;
        }
      };
}
