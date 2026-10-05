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
package sh.oso.connect.oracle.core.testkit;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.snapshot.ChunkRange;
import sh.oso.connect.oracle.core.snapshot.SnapshotRow;
import sh.oso.connect.oracle.core.snapshot.SnapshotSource;

/**
 * A {@link SnapshotSource} over in-memory tables keyed by a numeric ID: chunks of {@link
 * #chunkSize} rows, an SCN that moves on with every batch, recorded reads and injectable read
 * faults. Every "connection" is this one instance.
 */
public class FakeSnapshotSource implements SnapshotSource {

  public final Map<TableId, NavigableMap<BigDecimal, Map<String, Object>>> tables =
      new ConcurrentHashMap<>();
  public volatile int chunkSize = 2;
  public volatile long scn = 5000;
  public volatile long scnStep = 0;

  /** "TABLE [lo,hi)@scn" per chunk read, in completion order. */
  public final List<String> reads = Collections.synchronizedList(new ArrayList<>());

  /** chunkRows asked for at each plan, so tests see SNAP-4 halving. */
  public final List<Integer> planned = Collections.synchronizedList(new ArrayList<>());

  /** Thrown by the next reads, one each. */
  public final ConcurrentLinkedDeque<SQLException> readFaults = new ConcurrentLinkedDeque<>();

  /** Thrown once by the read of the chunk starting at this key ("" for the table's start). */
  public final Map<String, SQLException> faultAt = new ConcurrentHashMap<>();

  public FakeSnapshotSource rows(TableId t, int from, int to) {
    NavigableMap<BigDecimal, Map<String, Object>> m =
        tables.computeIfAbsent(t, k -> Collections.synchronizedNavigableMap(new TreeMap<>()));
    for (int i = from; i <= to; i++) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("ID", new BigDecimal(i));
      row.put("SQL", "row" + i);
      m.put(new BigDecimal(i), row);
    }
    return this;
  }

  @Override
  public Kind kind(TableSchema schema) {
    return Kind.KEY;
  }

  @Override
  public List<ChunkRange> plan(
      TableSchema schema, Kind kind, List<String> lower, int count, int chunkRows) {
    planned.add(chunkRows);
    NavigableMap<BigDecimal, Map<String, Object>> rows = table(schema.table());
    List<BigDecimal> keys =
        new ArrayList<>(lower == null ? rows.keySet() : rows.tailMap(key(lower), true).keySet());
    List<ChunkRange> out = new ArrayList<>();
    List<String> lo = lower;
    for (int i = chunkSize; out.size() < count; i += chunkSize) {
      if (i >= keys.size()) {
        out.add(new ChunkRange(lo, null));
        break;
      }
      List<String> hi = List.of("n:" + keys.get(i).toPlainString());
      out.add(new ChunkRange(lo, hi));
      lo = hi;
    }
    return out;
  }

  @Override
  public synchronized long currentScn() {
    scn += scnStep;
    return scn;
  }

  @Override
  public List<SnapshotRow> read(
      TableSchema schema, Kind kind, ChunkRange range, long at, String where) throws SQLException {
    SQLException fault = readFaults.poll();
    if (fault == null) {
      fault = faultAt.remove(range.lower() == null ? "" : key(range.lower()).toPlainString());
    }
    if (fault != null) {
      throw fault;
    }
    NavigableMap<BigDecimal, Map<String, Object>> rows = table(schema.table());
    NavigableMap<BigDecimal, Map<String, Object>> slice = rows;
    if (range.lower() != null) {
      slice = slice.tailMap(key(range.lower()), true);
    }
    if (range.upper() != null) {
      slice = slice.headMap(key(range.upper()), false);
    }
    List<SnapshotRow> out = new ArrayList<>();
    for (Map<String, Object> r : slice.values()) {
      out.add(new SnapshotRow(Map.copyOf(r), null));
    }
    reads.add(
        schema.table().table()
            + " ["
            + (range.lower() == null ? "" : key(range.lower()))
            + ","
            + (range.upper() == null ? "" : key(range.upper()))
            + ")@"
            + at);
    return out;
  }

  private NavigableMap<BigDecimal, Map<String, Object>> table(TableId t) {
    return tables.getOrDefault(t, new TreeMap<>());
  }

  private static BigDecimal key(List<String> bound) {
    return new BigDecimal(bound.get(0).substring(2));
  }

  @Override
  public void close() {}
}
