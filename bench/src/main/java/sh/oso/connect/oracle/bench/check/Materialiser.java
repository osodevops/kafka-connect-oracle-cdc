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
package sh.oso.connect.oracle.bench.check;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Applies change records to an in-memory copy of each table keyed by the record key, and keeps the
 * facts the three checks need: committed XIDs with their event indices, the highest commit SCN,
 * duplicates and ordering violations (ADR-0012). Records must arrive in topic-partition order.
 */
public final class Materialiser {

  /**
   * Per transaction: the event indices seen, how often each (index, op) pair arrived and the
   * declared count. A key change is a delete and a create under one index (SRC-TOP-4), so only a
   * repeat of the same operation counts as a duplicate.
   */
  public static final class TxFacts {
    public final java.util.Set<Integer> indices = new java.util.TreeSet<>();
    public final Map<String, Integer> opCopies = new TreeMap<>();
    public int eventCount = -1;
  }

  private final Map<String, Map<String, JsonNode>> tables = new TreeMap<>();
  private final Map<String, TxFacts> transactions = new HashMap<>();
  private final Map<String, Long> lastCommitScnByPartition = new HashMap<>();
  private final List<String> orderViolations = new ArrayList<>();
  private long records;
  private long tombstones;
  private long maxCommitScn = -1;

  public void apply(RecordJson r) {
    records++;
    if (r.tombstone) {
      tombstones++;
      return; // the delete before it already removed the row
    }
    if (r.op == null || r.after == null && r.before == null) {
      return; // heartbeat or other non-row record
    }
    String table = tableName(r);
    Map<String, JsonNode> rows = tables.computeIfAbsent(table, t -> new LinkedHashMap<>());
    String key = r.keyText();
    switch (r.op) {
      case "c":
      case "r":
      case "u":
        rows.put(key == null ? r.after.toString() : key, r.after);
        break;
      case "d":
        rows.remove(key == null ? r.before.toString() : key);
        break;
      default:
        break;
    }
    if (r.xid != null) {
      TxFacts f = transactions.computeIfAbsent(r.xid, x -> new TxFacts());
      if (r.eventIndex != null) {
        f.indices.add(r.eventIndex);
        f.opCopies.merge(r.eventIndex + ":" + r.op, 1, Integer::sum);
      }
      if (r.eventCount != null) {
        f.eventCount = r.eventCount;
      }
    }
    if (r.commitScn >= 0) {
      maxCommitScn = Math.max(maxCommitScn, r.commitScn);
      String tp = r.topic + "-" + r.partition;
      Long last = lastCommitScnByPartition.get(tp);
      if (last != null && r.commitScn < last && orderViolations.size() < 20) {
        orderViolations.add(tp + "@" + r.offset + ": commit_scn " + r.commitScn + " after " + last);
      }
      lastCommitScnByPartition.put(tp, r.commitScn);
    }
  }

  static String tableName(RecordJson r) {
    if (r.schema != null && r.table != null) {
      return r.schema + "." + r.table;
    }
    return r.topic;
  }

  public Map<String, Map<String, JsonNode>> tables() {
    return tables;
  }

  public Map<String, TxFacts> transactions() {
    return transactions;
  }

  public TreeSet<String> xids() {
    return new TreeSet<>(transactions.keySet());
  }

  public long maxCommitScn() {
    return maxCommitScn;
  }

  public long records() {
    return records;
  }

  public long tombstones() {
    return tombstones;
  }

  public List<String> orderViolations() {
    return orderViolations;
  }
}
