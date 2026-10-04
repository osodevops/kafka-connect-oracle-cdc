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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * What the framework has acknowledged (commitRecord). Offsets ride on each record, so correctness
 * does not depend on this class; it exposes the acknowledged position for metrics and for the
 * quiet-database heartbeat that arrives in Phase 1b (CORE-POS-3, CORE-POS-5).
 */
public final class AckTracker {

  private final AtomicLong acknowledged = new AtomicLong();
  private final AtomicReference<Map<String, ?>> lastOffset = new AtomicReference<>();

  public void acknowledged(SourceRecord record) {
    acknowledged.incrementAndGet();
    if (record != null && record.sourceOffset() != null) {
      lastOffset.set(record.sourceOffset());
    }
  }

  public long acknowledgedCount() {
    return acknowledged.get();
  }

  public Map<String, ?> lastAcknowledgedOffset() {
    return lastOffset.get();
  }
}
