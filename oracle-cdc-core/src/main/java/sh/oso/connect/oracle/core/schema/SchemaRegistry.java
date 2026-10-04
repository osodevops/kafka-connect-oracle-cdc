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

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * Current schema per captured table: the store first, the dictionary on a miss, with the key
 * selected on load. Versioning by SCN and DDL application arrive with the schema topic (P1-16).
 */
public final class SchemaRegistry {

  private final SchemaStore store;
  private final DictionaryReader dictionary;
  private final KeySelector keys;
  private final Map<TableId, TableSchema> cache = new ConcurrentHashMap<>();

  public SchemaRegistry(SchemaStore store, DictionaryReader dictionary, KeySelector keys) {
    this.store = store;
    this.dictionary = dictionary;
    this.keys = keys;
  }

  public TableSchema current(TableId table) throws SQLException {
    TableSchema cached = cache.get(table);
    if (cached != null) {
      return cached;
    }
    Optional<TableSchema> stored = store.load(table);
    TableSchema schema;
    if (stored.isPresent()) {
      schema = stored.get();
    } else {
      TableSchema fromDictionary =
          dictionary
              .read(table)
              .orElseThrow(
                  () ->
                      new DecodeException(
                          "Table " + table.fqn() + " is not in the dictionary",
                          "Check the include pattern and that the table exists in the PDB."));
      schema = keys.select(fromDictionary, dictionary.keyCandidates(table));
      store.save(schema);
    }
    cache.put(table, schema);
    return schema;
  }

  /** Forgets a table so the next lookup re-reads the dictionary (after DDL). */
  public void invalidate(TableId table) {
    cache.remove(table);
  }

  public void put(TableSchema schema) {
    store.save(schema);
    cache.put(schema.table(), schema);
  }
}
