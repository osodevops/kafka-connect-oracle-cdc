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
package sh.oso.connect.oracle.core.snapshot;

import java.sql.SQLException;
import java.util.List;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * The database side of a snapshot, on a connection of its own (PRD-00 CORE-CONN-2): one per reader
 * thread.
 */
public interface SnapshotSource extends AutoCloseable {

  /** How a table is cut into chunks (ADR-0004). */
  enum Kind {
    /** Key ranges: a primary or chosen key whose columns are all NOT NULL and of a range type. */
    KEY,
    /** ROWID ranges from the table's extents: a keyless heap table. */
    ROWID,
    /** One chunk: neither applies, for example an index-organised table with a RAW key. */
    ALL
  }

  Kind kind(TableSchema schema) throws SQLException;

  /**
   * Up to {@code count} chunks from {@code lower} (inclusive, null for the table's start), each
   * about {@code chunkRows} rows. The chunks are contiguous; when the table ends within them the
   * last one is open-ended ({@link ChunkRange#last()}).
   */
  List<ChunkRange> plan(TableSchema schema, Kind kind, List<String> lower, int count, int chunkRows)
      throws SQLException;

  long currentScn() throws SQLException;

  /**
   * The rows of {@code range} as of {@code scn}, with {@code where} (null for none) as a filter.
   */
  List<SnapshotRow> read(TableSchema schema, Kind kind, ChunkRange range, long scn, String where)
      throws SQLException;

  @Override
  void close() throws SQLException;
}
