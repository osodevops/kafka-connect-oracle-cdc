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
 * older than the dictionary (the lag case) decodes with {@link #at} (P1-17).
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
      schema =
          keys.select(fromDictionary, dictionary.keyCandidates(table))
              .withVersion(1, layoutSince(table));
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
    boolean ahead = dictionaryAhead(table, scn);
    // an inexact version whose layout the dictionary now confirms at this DDL becomes exact
    if (before != null && before.sameLayout(fresh) && (before.exact() || ahead)) {
      return Optional.empty();
    }
    TableSchema next =
        fresh.withVersion(before == null ? 1 : before.version() + 1, scn).withExact(!ahead);
    put(next);
    return Optional.of(next);
  }

  /**
   * P1-17: the version valid at {@code scn}, for a row mined again with a redo dictionary because
   * the online catalog had moved past it. Only an exact version known to be valid at the SCN will
   * do; anything else is a {@code CDC-6001} stop rather than a decode with a guessed layout.
   */
  public TableSchema at(TableId table, long scn) throws SQLException {
    current(table);
    TableSchema valid = null;
    for (TableSchema v : store.versions(table)) {
      if (v.validFromScn() <= scn) {
        valid = v;
      }
    }
    if (valid == null || !valid.exact()) {
      throw new sh.oso.connect.oracle.core.errors.DictionaryUnavailableException(
          "Rows of "
              + table.fqn()
              + " at SCN "
              + scn
              + " were written before a later DDL on the table, and "
              + (valid == null
                  ? "no stored version of the table is known to be valid there (the connector"
                      + " first read the table after that DDL)"
                  : "version "
                      + valid.version()
                      + ", valid there, was read after a further DDL had already changed the"
                      + " table, so its layout is not known")
              + ".",
          "Keep cdc.kafka.bootstrap.servers set so versions persist across restarts, and avoid"
              + " several DDLs on one table while the connector is stopped or lagging. To go on,"
              + " move the offset past the DDL; the table's rows in between are not delivered.");
    }
    return valid;
  }

  /**
   * The version a buffered row was decoded with ({@link
   * sh.oso.connect.oracle.core.model.RowChange#schemaVersion()}), so it renders with the columns it
   * has; 0 is the current version.
   */
  public TableSchema version(TableId table, int version) throws SQLException {
    TableSchema latest = current(table);
    if (version <= 0 || latest.version() == version) {
      return latest;
    }
    for (TableSchema v : store.versions(table)) {
      if (v.version() == version) {
        return v;
      }
    }
    throw new sh.oso.connect.oracle.core.errors.SchemaMismatchException(
        "A buffered change of "
            + table.fqn()
            + " was decoded with schema version "
            + version
            + ", which is no longer stored (latest "
            + latest.version()
            + ").",
        "Report the connector logs; restart the task so the transaction is mined again.");
  }

  /**
   * A layout read from the dictionary has held since the table's last DDL: valid from that time
   * (plus the slack of the SCN-to-time mapping), or from any SCN when the DDL is older than the
   * mapping.
   */
  private long layoutSince(TableId table) throws SQLException {
    Optional<java.time.Instant> ddl = dictionary.lastDdlTime(table);
    if (ddl.isEmpty()) {
      return 0;
    }
    return dictionary.scnAt(ddl.get().plus(SCN_TIME_SLACK)).orElse(0L);
  }

  /**
   * True when the table's last DDL is later than the DDL at {@code scn}: the dictionary already
   * reflects a further change. Unknown times count as ahead, unknown DDL times as not.
   */
  private boolean dictionaryAhead(TableId table, long scn) throws SQLException {
    Optional<java.time.Instant> ddl = dictionary.lastDdlTime(table);
    if (ddl.isEmpty()) {
      return false;
    }
    Optional<java.time.Instant> at = dictionary.timeOfScn(scn);
    return at.isEmpty() || ddl.get().isAfter(at.get().plus(SCN_TIME_SLACK));
  }

  /**
   * SCN_TO_TIMESTAMP maps an SCN to a time only to within a few seconds: a DDL this close to the
   * resume point counts as ahead of it rather than stopping the task on a rounding difference.
   */
  static final java.time.Duration SCN_TIME_SLACK = java.time.Duration.ofSeconds(10);

  /**
   * SCH-6, at start: the stored version of {@code table} must match the dictionary, unless the
   * table's last DDL is later than the resume point, so the DDL is still ahead in the redo. A
   * difference nothing explains means a DDL was missed: a stop.
   */
  public void validate(TableId table, long resumeScn) throws SQLException {
    Optional<TableSchema> stored = store.load(table);
    if (stored.isEmpty()) {
      return;
    }
    Optional<TableSchema> read = dictionary.read(table);
    if (read.isEmpty()) {
      return; // dropped since: the DROP is ahead in the redo
    }
    TableSchema fresh = keys.select(read.get(), dictionary.keyCandidates(table));
    if (fresh.sameLayout(stored.get())) {
      return;
    }
    Optional<java.time.Instant> ddl = dictionary.lastDdlTime(table);
    Optional<java.time.Instant> resume = dictionary.timeOfScn(resumeScn);
    if (ddl.isPresent()
        && resume.isPresent()
        && ddl.get().isAfter(resume.get().minus(SCN_TIME_SLACK))) {
      return;
    }
    throw new sh.oso.connect.oracle.core.errors.SchemaMismatchException(
        "Stored schema version "
            + stored.get().version()
            + " of "
            + table.fqn()
            + " differs from the dictionary, and the table's last DDL ("
            + ddl.map(Object::toString).orElse("unknown")
            + ") is not after the resume point (SCN "
            + resumeScn
            + ", "
            + resume.map(Object::toString).orElse("time unknown")
            + ").",
        "A DDL on the table was not captured. Move the offset back before the DDL while its redo is"
            + " available, or, once the change is understood, write a tombstone for the table's key"
            + " on the schema topic and restart.");
  }

  /**
   * The current version if the registry holds it, without reading the dictionary: for threads that
   * must not use the engine's metadata connection (PRD-02 snapshot readers).
   */
  public Optional<TableSchema> cached(TableId table) {
    TableSchema c = cache.get(table);
    return c != null ? Optional.of(c) : store.load(table);
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
