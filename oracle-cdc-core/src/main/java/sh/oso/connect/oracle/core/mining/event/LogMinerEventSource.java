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
package sh.oso.connect.oracle.core.mining.event;

import java.sql.SQLException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.logs.LogSet;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.LogMinerSource;
import sh.oso.connect.oracle.core.mining.MiningFilter;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;

/**
 * The real {@link EventSource}: inventory for the range, session log set, START_LOGMNR, filtered
 * query, row adapter. One step at a time; scheduling, retries and the DDL step cut sit above it.
 */
public final class LogMinerEventSource implements EventSource {

  private final LogInventory inventory;
  private final LogMinerSource session;
  private volatile MiningFilter filter;
  private volatile LogMinerRowAdapter adapter;
  private final DictionaryMode mode;

  public LogMinerEventSource(
      LogInventory inventory,
      LogMinerSource session,
      ResolvedObjects objects,
      MiningFilter filter,
      DictionaryMode mode) {
    this.inventory = inventory;
    this.session = session;
    this.filter = filter;
    this.adapter = new LogMinerRowAdapter(objects.byObject());
    this.mode = mode;
  }

  @Override
  public EventCursor open(long startScn, long endScn) throws SQLException {
    LogSet logs = inventory.forRange(startScn, endScn);
    session.setLogs(logs.logs());
    session.start(startScn, endScn, mode);
    adapter.reset();
    return adapter.adapt(session.query(filter, startScn, endScn));
  }

  /** Swaps the pushed-down ids after a DDL step cut (ADR-0001). */
  public void update(ResolvedObjects objects, MiningFilter newFilter) {
    this.filter = newFilter;
    this.adapter = new LogMinerRowAdapter(objects.byObject());
  }

  @Override
  public void recycle() throws SQLException {
    session.reset();
  }

  @Override
  public void close() throws SQLException {
    session.close();
  }
}
