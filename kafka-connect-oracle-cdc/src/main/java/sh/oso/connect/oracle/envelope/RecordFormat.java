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
package sh.oso.connect.oracle.envelope;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.snapshot.SnapshotRow;

/**
 * The shape of the records a change becomes (SRC-FMT, ADR-0028): the Debezium-compatible envelope
 * today. Every implementation keeps the source partition {@link #partition()}, which the heartbeat,
 * ops, journal, DLQ and schema writers share and stored offsets depend on.
 */
public interface RecordFormat {

  /** The source partition of every record this connector writes. */
  Map<String, Object> partition();

  /**
   * The records for change {@code index} of {@code tx}, all carrying {@code offset}; the sink gives
   * every record but the last the position before the change, so only a delivered last record
   * counts the change as delivered.
   */
  List<SourceRecord> records(
      CommittedTransaction tx,
      int index,
      long dataCollectionOrder,
      TableSchema schema,
      Position offset);

  /** The records for a row a snapshot chunk read as of {@code scn} (PRD-02 SNAP-8). */
  List<SourceRecord> snapshotRecords(
      TableId table,
      SnapshotRow row,
      TableSchema schema,
      long scn,
      long readAtMs,
      String marker,
      Position offset);

  /** The format {@code cdc.output.format} names. */
  static RecordFormat create(
      OracleCdcSourceConnectorConfig config, TopicRouter router, String databaseName) {
    return new DebeziumEnvelope(config, router, databaseName);
  }
}
