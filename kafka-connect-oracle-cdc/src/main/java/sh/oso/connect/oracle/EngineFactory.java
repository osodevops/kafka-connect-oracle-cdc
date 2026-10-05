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
package sh.oso.connect.oracle;

import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.EventSink;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;

/**
 * How the task obtains an engine. The JDBC factory is the real one; tests substitute a fake-driven
 * factory so the task's record and offset behaviour is proven without Oracle.
 */
public interface EngineFactory {

  /** Connects; the session's schema registry keeps its versions in {@code schemas}. */
  Session open(
      OracleCdcSourceConnectorConfig config, sh.oso.connect.oracle.core.schema.SchemaStore schemas)
      throws Exception;

  /** Connected to the database: identity and current SCN first, then the engine for a position. */
  interface Session extends AutoCloseable {
    DatabaseIdentity identity();

    long currentScn();

    boolean cdb();

    String databaseName();

    SchemaRegistry schemas();

    /** The engine for a position; {@code buffer} carries the journal setup and restored entries. */
    CaptureEngine engine(
        Position start, EventSink sink, sh.oso.connect.oracle.journal.BufferSetup buffer)
        throws Exception;

    /**
     * PRD-03 section 3 step 5: starts the scheduled dictionary builds when they are configured;
     * closing the session stops them.
     */
    /**
     * SRC-SEL-4: {@code listener} hears, on the engine thread, which tables a refresh of the object
     * ids added to or removed from the captured set.
     */
    default void onTablesChanged(
        java.util.function.BiConsumer<
                java.util.Set<sh.oso.connect.oracle.core.model.TableId>,
                java.util.Set<sh.oso.connect.oracle.core.model.TableId>>
            listener) {}

    /** The captured tables the engine resolved; valid once {@link #engine} has run. */
    default java.util.List<sh.oso.connect.oracle.core.model.TableId> capturedTables() {
      return java.util.List.of();
    }

    /** PRD-02: a snapshot source on a connection of its own. */
    default sh.oso.connect.oracle.core.snapshot.SnapshotSource openSnapshotSource()
        throws java.sql.SQLException {
      throw new java.sql.SQLException("this session cannot read snapshots");
    }

    default void startDictionaryBuilds(
        sh.oso.connect.oracle.core.logs.DictionaryBuildScheduler.Events events) throws Exception {}
  }
}
