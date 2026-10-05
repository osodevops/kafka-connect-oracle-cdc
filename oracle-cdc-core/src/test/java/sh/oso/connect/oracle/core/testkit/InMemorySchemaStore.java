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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.SchemaStore;
import sh.oso.connect.oracle.core.schema.TableSchema;

/** Schema store for unit tests. */
public final class InMemorySchemaStore implements SchemaStore {
  public final Map<TableId, TableSchema> schemas = new ConcurrentHashMap<>();
  public int saves;

  @Override
  public Optional<TableSchema> load(TableId table) {
    return Optional.ofNullable(schemas.get(table));
  }

  @Override
  public void save(TableSchema schema) {
    saves++;
    schemas.put(schema.table(), schema);
  }

  @Override
  public void remove(TableId table) {
    schemas.remove(table);
  }
}
