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

import java.beans.ConstructorProperties;

/** One open transaction in the top 20 by bytes buffered (CORE-TX-8); an MXBean composite. */
public final class TransactionInfo {
  private final String xid;
  private final String username;
  private final String clientId;
  private final long ageMillis;
  private final long firstScn;
  private final int events;
  private final long heapBytes;
  private final long spilledBytes;
  private final boolean journaled;

  @ConstructorProperties({
    "xid",
    "username",
    "clientId",
    "ageMillis",
    "firstScn",
    "events",
    "heapBytes",
    "spilledBytes",
    "journaled"
  })
  public TransactionInfo(
      String xid,
      String username,
      String clientId,
      long ageMillis,
      long firstScn,
      int events,
      long heapBytes,
      long spilledBytes,
      boolean journaled) {
    this.xid = xid;
    this.username = username;
    this.clientId = clientId;
    this.ageMillis = ageMillis;
    this.firstScn = firstScn;
    this.events = events;
    this.heapBytes = heapBytes;
    this.spilledBytes = spilledBytes;
    this.journaled = journaled;
  }

  public String getXid() {
    return xid;
  }

  public String getUsername() {
    return username;
  }

  public String getClientId() {
    return clientId;
  }

  public long getAgeMillis() {
    return ageMillis;
  }

  public long getFirstScn() {
    return firstScn;
  }

  public int getEvents() {
    return events;
  }

  public long getHeapBytes() {
    return heapBytes;
  }

  public long getSpilledBytes() {
    return spilledBytes;
  }

  public boolean isJournaled() {
    return journaled;
  }
}
