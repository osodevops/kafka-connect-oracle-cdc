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
 * selected on load. A DDL on a captured table reads the dictionary again and stores a new version
 * effective from the DDL's SCN when the layout changed (PRD-03 section 3 step 3). The engine
 * applies rows in redo order, so the current version is the one valid at each row it decodes; redo
 * older than the dictionary (the lag case) is P1-17.
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

  /**
   * After a DDL at {@code scn}: reads the dictionary and, when the layout changed, stores it as the
   * next version effective from {@code scn}. Returns the new version, or empty when nothing changed
   * or the table no longer exists.
   */
  public Optional<TableSchema> applyDdl(TableId table, long scn) throws SQLException {
    Optional<TableSchema> read = dictionary.read(table);
    if (read.isEmpty()) {
      forget(table);
      return Optional.empty();
    }
    TableSchema fresh = keys.select(read.get(), dictionary.keyCandidates(table));
    TableSchema before = cache.get(table);
    if (before == null) {
      before = store.load(table).orElse(null);
    }
    if (before != null && before.sameLayout(fresh)) {
      return Optional.empty();
    }
    TableSchema next = fresh.withVersion(before == null ? 1 : before.version() + 1, scn);
    put(next);
    return Optional.of(next);
  }

  /** A table dropped or renamed away: the next lookup under this name reads the dictionary. */
  public void forget(TableId table) {
    cache.remove(table);
    store.remove(table);
  }

  /** Whether this table has been decoded (or stored) under this name. */
  public boolean known(TableId table) {
    return cache.containsKey(table) || store.load(table).isPresent();
  }

  public void put(TableSchema schema) {
    store.save(schema);
    cache.put(schema.table(), schema);
  }
}
