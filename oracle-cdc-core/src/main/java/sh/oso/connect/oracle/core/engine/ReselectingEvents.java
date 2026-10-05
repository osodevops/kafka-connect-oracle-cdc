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

import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.errors.LobTooLargeException;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.schema.ColumnFilter;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * A committed transaction's events with the unavailable CLOB, NCLOB and BLOB values of each INSERT
 * and UPDATE after image fetched AS OF the commit SCN (cdc.lob.mode=reselect), one query per row,
 * as the sink reads them, so a spilled transaction is still never held in memory whole. A column
 * {@code cdc.columns.exclude} names is never selected (SRC-SEL-2).
 */
final class ReselectingEvents extends AbstractList<RowChange> implements CommittedTransaction.Lazy {

  private final List<RowChange> base;
  private final long scn;
  private final SchemaRegistry schemas;
  private final ColumnFilter excluded;
  private final LobReselector reselector;
  private final OraErrorClassifier classifier;
  private final long maxBytes;
  private final boolean oversizeFail;
  private int lastIndex = -1;
  private RowChange last;

  private ReselectingEvents(
      List<RowChange> base,
      long scn,
      SchemaRegistry schemas,
      ColumnFilter excluded,
      LobReselector reselector,
      OraErrorClassifier classifier,
      long maxBytes,
      boolean oversizeFail) {
    this.base = base;
    this.scn = scn;
    this.schemas = schemas;
    this.excluded = excluded;
    this.reselector = reselector;
    this.classifier = classifier;
    this.maxBytes = maxBytes;
    this.oversizeFail = oversizeFail;
  }

  static CommittedTransaction wrap(
      CommittedTransaction tx,
      SchemaRegistry schemas,
      ColumnFilter excluded,
      LobReselector reselector,
      OraErrorClassifier classifier,
      long maxBytes,
      boolean oversizeFail) {
    return new CommittedTransaction(
        tx.key(),
        tx.firstCaptured(),
        tx.startId(),
        tx.commitId(),
        tx.commitTimestamp(),
        tx.thread(),
        tx.username(),
        tx.clientId(),
        new ReselectingEvents(
            tx.events(),
            tx.commitScn(),
            schemas,
            excluded,
            reselector,
            classifier,
            maxBytes,
            oversizeFail));
  }

  @Override
  public int size() {
    return base.size();
  }

  @Override
  public RowChange get(int index) {
    if (index == lastIndex) {
      return last;
    }
    RowChange c = base.get(index);
    last = reselect(c);
    lastIndex = index;
    return last;
  }

  private RowChange reselect(RowChange c) {
    if (c.op() == Operation.DELETE || c.after() == null) {
      return c;
    }
    try {
      // SRC-SEL-2: an excluded LOB is absent from the image on purpose, never fetched
      TableSchema schema = excluded.project(schemas.version(c.table(), c.schemaVersion()));
      List<String> missing = new ArrayList<>();
      for (ColumnSpec col : schema.columns()) {
        OracleType t = col.type();
        boolean lob = t == OracleType.CLOB || t == OracleType.NCLOB || t == OracleType.BLOB;
        if (lob && !c.after().containsKey(col.name())) {
          missing.add(col.name());
        }
      }
      if (missing.isEmpty()) {
        return c;
      }
      Map<String, Object> found = reselector.reselect(schema, c, scn, missing);
      Map<String, Object> after = new LinkedHashMap<>(c.after());
      for (Map.Entry<String, Object> e : found.entrySet()) {
        Object v = e.getValue();
        long size =
            v instanceof byte[] b
                ? b.length
                : v == null
                    ? 0
                    : v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (size > maxBytes) {
          if (oversizeFail) {
            throw new LobTooLargeException(
                "Reselecting "
                    + schema.table().fqn()
                    + "."
                    + e.getKey()
                    + " AS OF SCN "
                    + scn
                    + " returned "
                    + size
                    + " bytes, above cdc.lob.max.bytes="
                    + maxBytes
                    + ".",
                "Raise cdc.lob.max.bytes, or set cdc.lob.oversize.action=placeholder.");
          }
          continue;
        }
        after.put(e.getKey(), v);
      }
      return new RowChange(
          c.table(),
          c.op(),
          c.before(),
          after,
          c.partial(),
          c.rowId(),
          c.id(),
          c.tx(),
          c.timestamp(),
          c.schemaVersion());
    } catch (SQLException e) {
      throw classifier.toException(e, "reselecting LOB values of " + c.table().fqn());
    }
  }
}
