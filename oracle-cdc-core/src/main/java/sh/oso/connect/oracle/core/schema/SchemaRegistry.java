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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * Current schema per captured table: the store first, the dictionary on a miss, with the key
 * selected on load. A DDL on a captured table reads the dictionary again and stores a new version
 * effective from the DDL's SCN when the layout changed (PRD-03 section 3 step 3). The engine
 * applies rows in redo order, so the current version is the one valid at each row it decodes; redo
 * older than the dictionary (the lag case) decodes with {@link #at} (P1-17). The task reads every
 * captured table's layout when it starts ({@link #readLayouts}), so the first version predates any
 * DDL after the start (ADR-0016 amendment).
 */
public final class SchemaRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);

  /**
   * What an operator does when rows of a table were written under a layout the connector does not
   * know (CDC-6001): from {@link #at}, and from the decoder for a column an inexact version lacks.
   */
  public static final String LAYOUT_UNKNOWN_ACTION =
      "Keep cdc.kafka.bootstrap.servers set so versions persist across restarts, and avoid several"
          + " DDLs on one table while the connector is stopped or lagging. To go on, move the"
          + " offset past the DDL; the table's rows in between are not delivered.";

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
                      + " first read the table after that DDL, or too soon after an earlier one"
                      + " to tell which came first)"
                  : "version "
                      + valid.version()
                      + ", valid there, was read after a further DDL had already changed the"
                      + " table, so its layout is not known")
              + ".",
          LAYOUT_UNKNOWN_ACTION);
    }
    return valid;
  }

  /**
   * ADR-0016 amendment: stores version 1 of every table in {@code tables} that has no version yet,
   * read in one pass ({@link DictionaryReader#readAll}). The task calls it when it starts and when
   * tables join the captured set, so a DDL soon after cannot leave the rows written before it
   * without a version.
   *
   * <p>A layout is valid from {@code fromScn} when the table's last DDL is more than {@link
   * #SCN_TIME_SLACK} before the time of {@code quietSinceScn}: no DDL can lie between that SCN and
   * the read. The slack leans the other way from that of {@link #applyDdl}, whose reference SCN is
   * a DDL itself; here a DDL just after the SCN must never pass for one before it. Otherwise the
   * layout is valid from its last DDL, as on a first read at decode time ({@link #current}). {@code
   * fromScn} may be below {@code quietSinceScn} only when every row decoded below it belongs to a
   * transaction that holds its table's lock across it (ADR-0019), so no DDL on the table falls
   * between the row and that SCN.
   *
   * <p>A table the dictionary no longer holds, or that cannot be keyed, is left to its first
   * decode, which reports it as before. Returns the versions stored.
   */
  public List<TableSchema> readLayouts(Collection<TableId> tables, long fromScn, long quietSinceScn)
      throws SQLException {
    List<TableId> wanted = new ArrayList<>();
    for (TableId t : new LinkedHashSet<>(tables)) {
      if (!known(t)) {
        wanted.add(t);
      }
    }
    if (wanted.isEmpty()) {
      return List.of();
    }
    Map<TableId, DictionaryReader.Layout> layouts = dictionary.readAll(wanted);
    Optional<Instant> quietSince = dictionary.timeOfScn(quietSinceScn);
    Map<Instant, Long> sinceDdl = new HashMap<>();
    List<TableSchema> stored = new ArrayList<>();
    for (TableId t : wanted) {
      DictionaryReader.Layout layout = layouts.get(t);
      if (layout == null) {
        continue; // dropped since it was resolved: the DROP is ahead in the redo
      }
      TableSchema keyed;
      try {
        keyed = keys.select(layout.columns(), layout.keys());
      } catch (DecodeException e) {
        LOG.warn("Layout of {} not stored ahead of its first change: {}", t.fqn(), e.getMessage());
        continue;
      }
      Instant ddl = layout.lastDdlTime();
      boolean quiet =
          ddl != null
              && quietSince.isPresent()
              && !ddl.isAfter(quietSince.get().minus(SCN_TIME_SLACK));
      TableSchema first = keyed.withVersion(1, quiet ? fromScn : since(ddl, sinceDdl));
      put(first);
      stored.add(first);
    }
    return stored;
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
    return since(dictionary.lastDdlTime(table).orElse(null), new HashMap<>());
  }

  /**
   * The SCN from which a layout whose last DDL ran at {@code ddl} has held; {@code known} keeps the
   * answers of one pass, as many tables share a DDL time.
   */
  private long since(Instant ddl, Map<Instant, Long> known) throws SQLException {
    if (ddl == null) {
      return 0;
    }
    Long scn = known.get(ddl);
    if (scn == null) {
      scn = dictionary.scnAt(ddl.plus(SCN_TIME_SLACK)).orElse(0L);
      known.put(ddl, scn);
    }
    return scn;
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
