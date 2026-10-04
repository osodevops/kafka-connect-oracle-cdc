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

  Session open(OracleCdcSourceConnectorConfig config) throws Exception;

  /** Connected to the database: identity and current SCN first, then the engine for a position. */
  interface Session extends AutoCloseable {
    DatabaseIdentity identity();

    long currentScn();

    boolean cdb();

    String databaseName();

    SchemaRegistry schemas();

    CaptureEngine engine(Position start, EventSink sink) throws Exception;
  }
}
