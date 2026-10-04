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
package sh.oso.connect.oracle.core.schema;

import java.util.Optional;
import sh.oso.connect.oracle.core.model.TableId;

/** Durable home of table schemas; the compacted schema topic in Phase 1c, in memory in tests. */
public interface SchemaStore {
  Optional<TableSchema> load(TableId table);

  void save(TableSchema schema);
}
