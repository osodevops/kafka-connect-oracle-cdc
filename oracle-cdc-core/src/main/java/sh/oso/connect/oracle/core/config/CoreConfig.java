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
package sh.oso.connect.oracle.core.config;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/**
 * Engine-level configuration, the {@code cdc.*} keys owned by PRD-00 section 5. The connector
 * composes {@link #configDef()} into its own definition so the generated reference documents every
 * key once. Groups follow the PRD sections so the docs generator can order them.
 */
public class CoreConfig extends AbstractConfig {

  public static final String GROUP_DATABASE = "Database";
  public static final String GROUP_CAPTURE = "Capture";
  public static final String GROUP_MINING = "Mining";
  public static final String GROUP_BUFFER = "Transaction buffer";
  public static final String GROUP_JOURNAL = "Transaction journal";
  public static final String GROUP_TRANSACTIONS = "Transactions";
  public static final String GROUP_ERRORS = "Errors and retries";
  public static final String GROUP_LOBS = "LOBs";
  public static final String GROUP_SNAPSHOTS = "Snapshots";

  public static final String DATABASE_HOST = "cdc.database.host";
  public static final String DATABASE_PORT = "cdc.database.port";
  public static final String DATABASE_SERVICE = "cdc.database.service";
  public static final String DATABASE_SID = "cdc.database.sid";
  public static final String DATABASE_URL = "cdc.database.url";
  public static final String DATABASE_USER = "cdc.database.user";
  public static final String DATABASE_PASSWORD = "cdc.database.password";
  public static final String DATABASE_WALLET_LOCATION = "cdc.database.wallet.location";
  public static final String DATABASE_TLS_TRUSTSTORE_LOCATION =
      "cdc.database.tls.truststore.location";
  public static final String DATABASE_TLS_TRUSTSTORE_PASSWORD =
      "cdc.database.tls.truststore.password";
  public static final String DATABASE_TLS_TRUSTSTORE_TYPE = "cdc.database.tls.truststore.type";
  public static final String DATABASE_KERBEROS_CCACHE = "cdc.database.kerberos.ccache";
  public static final String DATABASE_CONNECTION_PROPERTIES = "cdc.database.connection.properties";
  public static final String DATABASE_IDLE_TIMEOUT_MS = "cdc.database.idle.timeout.ms";
  public static final String DATABASE_PDBS = "cdc.database.pdbs";
  public static final String DATABASE_FAN_ENABLED = "cdc.database.fan.enabled";

  public static final String CAPTURE_MODE = "cdc.capture.mode";
  public static final String ARCHIVE_DESTINATION = "cdc.archive.destination";
  public static final String START_SCN = "cdc.start.scn";

  public static final String MINING_TARGET_LATENCY_MS = "cdc.mining.target.latency.ms";
  public static final String MINING_MAX_LOGS_PER_STEP = "cdc.mining.max.logs.per.step";
  public static final String MINING_FETCH_SIZE = "cdc.mining.fetch.size";
  public static final String MINING_QUERY_TIMEOUT_MS = "cdc.mining.query.timeout.ms";
  public static final String MINING_SESSION_MAX_AGE_MS = "cdc.mining.session.max.age.ms";
  public static final String MINING_DECODE_THREADS = "cdc.mining.decode.threads";
  public static final String MINING_INLIST_MAX = "cdc.mining.inlist.max";
  public static final String MINING_CATCHUP_THRESHOLD_MS = "cdc.mining.catchup.threshold.ms";
  public static final String MINING_CATCHUP_PARALLELISM = "cdc.mining.catchup.parallelism";
  public static final String RAC_SAFETY_LAG_MS = "cdc.rac.safety.lag.ms";

  // PRD-02 snapshots
  public static final String SNAPSHOT_MODE = "cdc.snapshot.mode";
  public static final String SNAPSHOT_THREADS = "cdc.snapshot.threads";
  public static final String SNAPSHOT_CHUNK_ROWS = "cdc.snapshot.chunk.rows";
  public static final String SNAPSHOT_CHUNK_RETRIES = "cdc.snapshot.chunk.retries";
  public static final String SNAPSHOT_FETCH_SIZE = "cdc.snapshot.fetch.size";
  public static final String SNAPSHOT_MAX_PENDING_CHUNKS = "cdc.snapshot.max.pending.chunks";
  public static final String SNAPSHOT_TABLES_ORDER = "cdc.snapshot.tables.order";
  public static final String SNAPSHOT_SELECT_OVERRIDE_PREFIX = "cdc.snapshot.select.override.";

  // PRD-03 section 3 step 5: dictionary builds into the redo for the lag case
  public static final String DICTIONARY_BUILD_INTERVAL_MS = "cdc.dictionary.build.interval.ms";
  public static final String DICTIONARY_BUILD_TIME = "cdc.dictionary.build.time";

  public static final String BUFFER_MEMORY_MAX_BYTES = "cdc.buffer.memory.max.bytes";
  public static final String BUFFER_SPILL_DIR = "cdc.buffer.spill.dir";
  public static final String BUFFER_SPILL_MAX_BYTES = "cdc.buffer.spill.max.bytes";

  public static final String TXJOURNAL_TOPIC = "cdc.txjournal.topic";
  public static final String TXJOURNAL_THRESHOLD_MS = "cdc.txjournal.threshold.ms";
  public static final String TXJOURNAL_THRESHOLD_EVENTS = "cdc.txjournal.threshold.events";
  public static final String TXJOURNAL_CHUNK_MAX_BYTES = "cdc.txjournal.chunk.max.bytes";

  public static final String TRANSACTION_MAX_AGE_MS = "cdc.transaction.max.age.ms";
  public static final String TRANSACTION_MAX_AGE_ACTION = "cdc.transaction.max.age.action";
  public static final String TRANSACTION_ORPHAN_CHECK_INTERVAL_MS =
      "cdc.transaction.orphan.check.interval.ms";
  public static final String TRANSACTION_ORPHAN_ACTION = "cdc.transaction.orphan.action";

  public static final String LOB_MODE = "cdc.lob.mode";
  public static final String LOB_MAX_BYTES = "cdc.lob.max.bytes";
  public static final String LOB_OVERSIZE_ACTION = "cdc.lob.oversize.action";
  public static final String UNAVAILABLE_PLACEHOLDER = "cdc.unavailable.placeholder";

  public static final String ON_DECODE_ERROR = "cdc.on.decode.error";
  public static final String RETRY_MAX_TIME_MS = "cdc.retry.max.time.ms";
  public static final String RETRY_EXTRA_ERROR_CODES = "cdc.retry.extra.error.codes";
  public static final String LOG_SENSITIVE_DATA = "cdc.log.sensitive.data";

  public enum CaptureMode {
    ONLINE,
    ARCHIVE_ONLY
  }

  public enum MaxAgeAction {
    FAIL,
    DISCARD
  }

  public enum OrphanAction {
    RELEASE,
    FAIL
  }

  /** PRD-02 SNAP-1. */
  public enum SnapshotMode {
    INITIAL,
    NONE,
    SNAPSHOT_ONLY,
    ON_SIGNAL
  }

  public enum LobMode {
    SKIP,
    INLINE,
    RESELECT
  }

  public enum LobOversizeAction {
    FAIL,
    PLACEHOLDER
  }

  public enum DecodeErrorAction {
    FAIL,
    DLQ
  }

  public CoreConfig(Map<String, String> props) {
    super(configDef(), props, false);
    validateConnection();
  }

  private void validateConnection() {
    boolean hasUrl = notBlank(getString(DATABASE_URL));
    boolean hasHost = notBlank(getString(DATABASE_HOST));
    boolean hasService = notBlank(getString(DATABASE_SERVICE));
    boolean hasSid = notBlank(getString(DATABASE_SID));
    if (!hasUrl && !hasHost) {
      throw new ConfigException(
          DATABASE_HOST, null, "Set either " + DATABASE_URL + " or " + DATABASE_HOST + ".");
    }
    if (hasHost && !hasUrl && !hasService && !hasSid) {
      throw new ConfigException(
          DATABASE_SERVICE,
          null,
          "Set "
              + DATABASE_SERVICE
              + " or "
              + DATABASE_SID
              + " together with "
              + DATABASE_HOST
              + ".");
    }
    if (getLong(TRANSACTION_MAX_AGE_MS) != -1 && getLong(TRANSACTION_MAX_AGE_MS) < 1000) {
      throw new ConfigException(
          TRANSACTION_MAX_AGE_MS,
          getLong(TRANSACTION_MAX_AGE_MS),
          "Use -1 (unlimited) or at least 1000 ms.");
    }
  }

  private static boolean notBlank(String s) {
    return s != null && !s.isBlank();
  }

  public CaptureMode captureMode() {
    return CaptureMode.valueOf(getString(CAPTURE_MODE).toUpperCase(Locale.ROOT));
  }

  public MaxAgeAction maxAgeAction() {
    return MaxAgeAction.valueOf(getString(TRANSACTION_MAX_AGE_ACTION).toUpperCase(Locale.ROOT));
  }

  public OrphanAction orphanAction() {
    return OrphanAction.valueOf(getString(TRANSACTION_ORPHAN_ACTION).toUpperCase(Locale.ROOT));
  }

  /** {@code cdc.start.scn}, or null to start at the current SCN. */
  public Long startScn() {
    return getLong(START_SCN);
  }

  public SnapshotMode snapshotMode() {
    return SnapshotMode.valueOf(getString(SNAPSHOT_MODE).toUpperCase(Locale.ROOT));
  }

  /**
   * SNAP-5: {@code cdc.snapshot.select.override.<table>} filters, keyed by the table as written
   * (PDB.OWNER.TABLE, or OWNER.TABLE without a PDB).
   */
  public Map<String, String> snapshotSelectOverrides() {
    Map<String, String> out = new java.util.LinkedHashMap<>();
    originalsWithPrefix(SNAPSHOT_SELECT_OVERRIDE_PREFIX)
        .forEach((k, v) -> out.put(k.toUpperCase(Locale.ROOT), String.valueOf(v)));
    return out;
  }

  public LobMode lobMode() {
    return LobMode.valueOf(getString(LOB_MODE).toUpperCase(Locale.ROOT));
  }

  public LobOversizeAction lobOversizeAction() {
    return LobOversizeAction.valueOf(getString(LOB_OVERSIZE_ACTION).toUpperCase(Locale.ROOT));
  }

  public DecodeErrorAction decodeErrorAction() {
    return DecodeErrorAction.valueOf(getString(ON_DECODE_ERROR).toUpperCase(Locale.ROOT));
  }

  public Password databasePassword() {
    return getPassword(DATABASE_PASSWORD);
  }

  public List<String> pdbs() {
    return getList(DATABASE_PDBS);
  }

  /** Extra transient ORA codes as integers. */
  public List<Integer> extraRetryErrorCodes() {
    return getList(RETRY_EXTRA_ERROR_CODES).stream()
        .map(s -> s.trim().toUpperCase(Locale.ROOT).replace("ORA-", ""))
        .filter(s -> !s.isEmpty())
        .map(Integer::valueOf)
        .toList();
  }

  private static ConfigDef.Validator enumOf(Class<? extends Enum<?>> e) {
    String[] names = new String[e.getEnumConstants().length];
    for (int i = 0; i < names.length; i++) {
      names[i] = e.getEnumConstants()[i].name().toLowerCase(Locale.ROOT);
    }
    return ValidString.in(names);
  }

  /** Lower-case enum validator that also accepts upper case input. */
  private static ConfigDef.Validator caseInsensitiveEnum(Class<? extends Enum<?>> e) {
    ConfigDef.Validator lower = enumOf(e);
    return (name, value) ->
        lower.ensureValid(name, value == null ? null : value.toString().toLowerCase(Locale.ROOT));
  }

  public static ConfigDef configDef() {
    return addTo(new ConfigDef());
  }

  /** Adds every engine key to an existing definition (used by the connector). */
  public static ConfigDef addTo(ConfigDef def) {
    int o = 0;
    // Database
    def.define(
        DATABASE_HOST,
        Type.STRING,
        null,
        Importance.HIGH,
        "Oracle Database host. Alternative to " + DATABASE_URL + ".",
        GROUP_DATABASE,
        ++o,
        Width.MEDIUM,
        "Host");
    def.define(
        DATABASE_PORT,
        Type.INT,
        1521,
        Range.between(1, 65535),
        Importance.MEDIUM,
        "Oracle listener port.",
        GROUP_DATABASE,
        ++o,
        Width.SHORT,
        "Port");
    def.define(
        DATABASE_SERVICE,
        Type.STRING,
        null,
        Importance.HIGH,
        "Service name (preferred over SID). In a CDB this is the CDB$ROOT service.",
        GROUP_DATABASE,
        ++o,
        Width.MEDIUM,
        "Service");
    def.define(
        DATABASE_SID,
        Type.STRING,
        null,
        Importance.LOW,
        "SID, for databases without a service name.",
        GROUP_DATABASE,
        ++o,
        Width.MEDIUM,
        "SID");
    def.define(
        DATABASE_URL,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Full JDBC URL (TNS descriptor, LDAP naming or wallet). Overrides host, port, service and"
            + " SID.",
        GROUP_DATABASE,
        ++o,
        Width.LONG,
        "JDBC URL");
    def.define(
        DATABASE_USER,
        Type.STRING,
        ConfigDef.NO_DEFAULT_VALUE,
        Importance.HIGH,
        "Mining user. In a CDB this must be a common user (C## prefix) with the grants from"
            + " oracle-cdc-doctor setup-sql, including CONTAINER_DATA=ALL.",
        GROUP_DATABASE,
        ++o,
        Width.MEDIUM,
        "User");
    def.define(
        DATABASE_PASSWORD,
        Type.PASSWORD,
        ConfigDef.NO_DEFAULT_VALUE,
        Importance.HIGH,
        "Password for the mining user. Use a Connect config provider; the value is never logged.",
        GROUP_DATABASE,
        ++o,
        Width.MEDIUM,
        "Password");
    def.define(
        DATABASE_WALLET_LOCATION,
        Type.STRING,
        null,
        Importance.LOW,
        "Directory of an Oracle wallet for TLS or stored credentials.",
        GROUP_DATABASE,
        ++o,
        Width.LONG,
        "Wallet location");
    def.define(
        DATABASE_TLS_TRUSTSTORE_LOCATION,
        Type.STRING,
        null,
        Importance.LOW,
        "Trust store file for TLS connections.",
        GROUP_DATABASE,
        ++o,
        Width.LONG,
        "TLS trust store");
    def.define(
        DATABASE_TLS_TRUSTSTORE_PASSWORD,
        Type.PASSWORD,
        null,
        Importance.LOW,
        "Trust store password.",
        GROUP_DATABASE,
        ++o,
        Width.MEDIUM,
        "TLS trust store password");
    def.define(
        DATABASE_TLS_TRUSTSTORE_TYPE,
        Type.STRING,
        "JKS",
        Importance.LOW,
        "Trust store type (JKS or PKCS12).",
        GROUP_DATABASE,
        ++o,
        Width.SHORT,
        "TLS trust store type");
    def.define(
        DATABASE_KERBEROS_CCACHE,
        Type.STRING,
        null,
        Importance.LOW,
        "Kerberos credential cache file. Reserved: Kerberos authentication is not built yet.",
        GROUP_DATABASE,
        ++o,
        Width.LONG,
        "Kerberos credential cache");
    def.define(
        DATABASE_CONNECTION_PROPERTIES,
        Type.STRING,
        null,
        Importance.LOW,
        "Extra Oracle JDBC driver properties as k=v;k=v.",
        GROUP_DATABASE,
        ++o,
        Width.LONG,
        "Driver properties");
    def.define(
        DATABASE_IDLE_TIMEOUT_MS,
        Type.LONG,
        300000L,
        Range.atLeast(10000L),
        Importance.LOW,
        "Reserved for an idle network guard, not built yet, that would probe the connection at half"
            + " this interval so a load balancer idle timeout cannot hang the connector.",
        GROUP_DATABASE,
        ++o,
        Width.SHORT,
        "Idle timeout (ms)");
    def.define(
        DATABASE_PDBS,
        Type.LIST,
        "",
        Importance.HIGH,
        "Pluggable databases to capture, comma separated. Leave empty for a non-CDB database.",
        GROUP_DATABASE,
        ++o,
        Width.LONG,
        "PDBs");
    def.define(
        DATABASE_FAN_ENABLED,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        "Reserved for subscribing to RAC Fast Application Notification events. RAC is not"
            + " supported yet.",
        GROUP_DATABASE,
        ++o,
        Width.SHORT,
        "FAN events");
    // Capture
    def.define(
        CAPTURE_MODE,
        Type.STRING,
        "online",
        caseInsensitiveEnum(CaptureMode.class),
        Importance.MEDIUM,
        "online mines online and archived logs; archive_only mines archived logs only and never"
            + " adds online logs.",
        GROUP_CAPTURE,
        ++o,
        Width.SHORT,
        "Capture mode");
    def.define(
        START_SCN,
        Type.LONG,
        null,
        (name, value) -> {
          if (value != null && (Long) value < 1) {
            throw new org.apache.kafka.common.config.ConfigException(
                name, value, "must be positive");
          }
        },
        Importance.LOW,
        "SCN to start streaming from when the connector has no stored offset, for example when it"
            + " takes over from another connector; ignored once an offset exists. Every archived"
            + " log from it onwards must still exist, or the task stops with CDC-2002. Empty starts"
            + " at the current SCN.",
        GROUP_CAPTURE,
        ++o,
        Width.MEDIUM,
        "Start SCN");
    def.define(
        ARCHIVE_DESTINATION,
        Type.STRING,
        null,
        Importance.LOW,
        "Archive destination name (for example LOG_ARCHIVE_DEST_1). Default: the lowest valid local"
            + " destination.",
        GROUP_CAPTURE,
        ++o,
        Width.MEDIUM,
        "Archive destination");
    // Mining
    def.define(
        MINING_TARGET_LATENCY_MS,
        Type.LONG,
        2000L,
        Range.atLeast(100L),
        Importance.MEDIUM,
        "Latency goal for the adaptive mining window. The only tuning knob: window size follows"
            + " from it.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Target latency (ms)");
    def.define(
        MINING_MAX_LOGS_PER_STEP,
        Type.INT,
        8,
        Range.between(1, 256),
        Importance.LOW,
        "Upper bound on redo logs mined in one step while catching up.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Max logs per step");
    def.define(
        MINING_FETCH_SIZE,
        Type.INT,
        10000,
        Range.between(100, 1000000),
        Importance.LOW,
        "JDBC fetch size for V$LOGMNR_CONTENTS.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Fetch size");
    def.define(
        MINING_QUERY_TIMEOUT_MS,
        Type.LONG,
        600000L,
        Range.atLeast(10000L),
        Importance.LOW,
        "Per-step timeout. On expiry the step is discarded, the session restarted and the window"
            + " halved; it never fails the task by itself. A socket read on any database"
            + " connection times out one minute after this, so a connection the network drops"
            + " silently is reopened rather than waited on.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Query timeout (ms)");
    def.define(
        MINING_SESSION_MAX_AGE_MS,
        Type.LONG,
        3600000L,
        Range.atLeast(60000L),
        Importance.LOW,
        "The LogMiner session is restarted at this age to release PGA.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Session max age (ms)");
    def.define(
        MINING_DECODE_THREADS,
        Type.INT,
        0,
        Range.between(0, 64),
        Importance.LOW,
        "Decode threads; 0 means the number of cores minus one, at most 8.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Decode threads");
    def.define(
        MINING_INLIST_MAX,
        Type.INT,
        1000,
        Range.between(1, 1000), // Oracle's IN-list limit (ORA-01795)
        Importance.LOW,
        "Most captured object ids in one IN list of the mining query; more ids are split across"
            + " several IN lists. Oracle allows at most 1000 entries in one list, and the task"
            + " fails at start with a larger value.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "IN-list limit");
    def.define(
        MINING_CATCHUP_THRESHOLD_MS,
        Type.LONG,
        300000L,
        Range.atLeast(1000L),
        Importance.LOW,
        "Reserved for the lag that would enable parallel catch-up mining, which is not built yet.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Catch-up threshold (ms)");
    def.define(
        MINING_CATCHUP_PARALLELISM,
        Type.INT,
        2,
        Range.between(1, 8),
        Importance.LOW,
        "Reserved for the number of catch-up sessions over adjacent SCN windows. Parallel"
            + " catch-up mining is not built yet.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Catch-up parallelism");
    def.define(
        RAC_SAFETY_LAG_MS,
        Type.LONG,
        -1L,
        Importance.LOW,
        "Reserved for the hold-back from the cluster SCN on RAC so late-archiving threads are not"
            + " missed. RAC is not supported yet.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "RAC safety lag (ms)");
    def.define(
        DICTIONARY_BUILD_INTERVAL_MS,
        Type.LONG,
        86_400_000L,
        Range.atLeast(0L),
        Importance.LOW,
        "Interval between data dictionary builds into the redo (DBMS_LOGMNR_D.BUILD), which let"
            + " the connector decode rows written before a later DDL on their table. Needs EXECUTE"
            + " ON DBMS_LOGMNR_D; without it builds are switched off with an ops event. One build"
            + " also runs at start when the archived logs hold none. 0 switches builds off.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Dictionary build interval (ms)");
    def.define(
        DICTIONARY_BUILD_TIME,
        Type.STRING,
        "02:00",
        (name, value) -> {
          try {
            java.time.LocalTime.parse(String.valueOf(value));
          } catch (java.time.format.DateTimeParseException e) {
            throw new org.apache.kafka.common.config.ConfigException(
                name, value, "must be a time of day as HH:mm");
          }
        },
        Importance.LOW,
        "Time of day, in the database's time, of the first scheduled dictionary build; later"
            + " builds follow every cdc.dictionary.build.interval.ms.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Dictionary build time");
    // Buffer
    def.define(
        BUFFER_MEMORY_MAX_BYTES,
        Type.LONG,
        268435456L,
        Range.atLeast(16777216L),
        Importance.MEDIUM,
        "Heap budget for buffered uncommitted changes; the largest transactions spill to disk above"
            + " it.",
        GROUP_BUFFER,
        ++o,
        Width.SHORT,
        "Buffer heap budget (bytes)");
    def.define(
        BUFFER_SPILL_DIR,
        Type.STRING,
        null,
        Importance.LOW,
        "Spill directory. Default: the worker's temporary directory plus the connector name. On"
            + " Strimzi that is a 5 MiB in-memory volume: mount a disk-backed volume under /mnt and"
            + " point this at it, with cdc.buffer.spill.max.bytes below its size.",
        GROUP_BUFFER,
        ++o,
        Width.LONG,
        "Spill directory");
    def.define(
        BUFFER_SPILL_MAX_BYTES,
        Type.LONG,
        10737418240L,
        Range.atLeast(67108864L),
        Importance.LOW,
        "Spill cap. Exceeding it stops the task with BufferExhaustedException naming the largest"
            + " transactions.",
        GROUP_BUFFER,
        ++o,
        Width.SHORT,
        "Spill cap (bytes)");
    // Journal
    def.define(
        TXJOURNAL_TOPIC,
        Type.STRING,
        null,
        Importance.LOW,
        "Compacted journal topic for long transactions. Default:"
            + " ${cdc.topic.prefix}.cdc.txjournal.",
        GROUP_JOURNAL,
        ++o,
        Width.LONG,
        "Journal topic");
    def.define(
        TXJOURNAL_THRESHOLD_MS,
        Type.LONG,
        300000L,
        Range.atLeast(1000L),
        Importance.LOW,
        "A transaction open longer than this is journaled and stops pinning the restart position.",
        GROUP_JOURNAL,
        ++o,
        Width.SHORT,
        "Journal age threshold (ms)");
    def.define(
        TXJOURNAL_THRESHOLD_EVENTS,
        Type.LONG,
        100000L,
        Range.atLeast(1L),
        Importance.LOW,
        "A transaction with more buffered events than this is journaled.",
        GROUP_JOURNAL,
        ++o,
        Width.SHORT,
        "Journal size threshold (events)");
    def.define(
        TXJOURNAL_CHUNK_MAX_BYTES,
        Type.INT,
        524288,
        Range.between(16384, 8388608),
        Importance.LOW,
        "Target size of one journal record; a step's changes for a journaled transaction are split"
            + " into chunks of about this many bytes. Keep it under the broker's message size"
            + " limit.",
        GROUP_JOURNAL,
        ++o,
        Width.SHORT,
        "Journal chunk size (bytes)");
    // Transactions
    def.define(
        TRANSACTION_MAX_AGE_MS,
        Type.LONG,
        -1L,
        Importance.LOW,
        "Long transaction limit; -1 is unlimited. On breach the action below applies and is never"
            + " silent.",
        GROUP_TRANSACTIONS,
        ++o,
        Width.SHORT,
        "Transaction max age (ms)");
    def.define(
        TRANSACTION_MAX_AGE_ACTION,
        Type.STRING,
        "fail",
        caseInsensitiveEnum(MaxAgeAction.class),
        Importance.LOW,
        "fail stops the task; discard drops the transaction after writing its details to the ops"
            + " topic and DLQ.",
        GROUP_TRANSACTIONS,
        ++o,
        Width.SHORT,
        "Transaction max age action");
    def.define(
        TRANSACTION_ORPHAN_CHECK_INTERVAL_MS,
        Type.LONG,
        300000L,
        Range.atLeast(1000L),
        Importance.LOW,
        "Interval for checking buffered transactions against GV$TRANSACTION.",
        GROUP_TRANSACTIONS,
        ++o,
        Width.SHORT,
        "Orphan check interval (ms)");
    def.define(
        TRANSACTION_ORPHAN_ACTION,
        Type.STRING,
        "release",
        caseInsensitiveEnum(OrphanAction.class),
        Importance.LOW,
        "release treats a vanished transaction as rolled back (a later COMMIT for it is a stop);"
            + " fail stops the task.",
        GROUP_TRANSACTIONS,
        ++o,
        Width.SHORT,
        "Orphan action");
    // LOBs
    def.define(
        LOB_MODE,
        Type.STRING,
        "skip",
        caseInsensitiveEnum(LobMode.class),
        Importance.MEDIUM,
        "skip leaves CLOB, NCLOB, BLOB and XMLTYPE columns out of the records; inline assembles"
            + " values from redo and publishes cdc.unavailable.placeholder where a value is not in"
            + " the redo; reselect does the same and then queries the values that are still"
            + " unavailable AS OF the commit SCN, one query per row.",
        GROUP_LOBS,
        ++o,
        Width.SHORT,
        "LOB mode");
    def.define(
        LOB_MAX_BYTES,
        Type.LONG,
        1048576L,
        ConfigDef.Range.atLeast(1),
        Importance.MEDIUM,
        "Largest LOB value published, in bytes (UTF-8 for CLOB and NCLOB). Larger values follow"
            + " cdc.lob.oversize.action; assembly memory per value is bounded by this limit.",
        GROUP_LOBS,
        ++o,
        Width.SHORT,
        "LOB size limit");
    def.define(
        LOB_OVERSIZE_ACTION,
        Type.STRING,
        "fail",
        caseInsensitiveEnum(LobOversizeAction.class),
        Importance.MEDIUM,
        "fail stops the task with CDC-3003 when a committed transaction wrote a LOB value above"
            + " cdc.lob.max.bytes; placeholder publishes cdc.unavailable.placeholder instead.",
        GROUP_LOBS,
        ++o,
        Width.SHORT,
        "LOB oversize action");
    def.define(
        UNAVAILABLE_PLACEHOLDER,
        Type.STRING,
        "__cdc_unavailable_value",
        Importance.LOW,
        "Value published for a LOB column whose value is not available (a before image, an"
            + " unchanged LOB in an update, a partial write or an oversize value); BLOB columns"
            + " carry its UTF-8 bytes.",
        GROUP_LOBS,
        ++o,
        Width.MEDIUM,
        "Unavailable placeholder");

    // Errors
    def.define(
        ON_DECODE_ERROR,
        Type.STRING,
        "fail",
        caseInsensitiveEnum(DecodeErrorAction.class),
        Importance.MEDIUM,
        "fail stops on an undecodable DML row; dlq routes the raw row to the DLQ with an ops event."
            + " Corruption and MISSING_SCN always stop.",
        GROUP_ERRORS,
        ++o,
        Width.SHORT,
        "On decode error");
    def.define(
        RETRY_MAX_TIME_MS,
        Type.LONG,
        86400000L,
        Range.atLeast(1000L),
        Importance.LOW,
        "Total time to keep retrying transient database errors before the task fails.",
        GROUP_ERRORS,
        ++o,
        Width.SHORT,
        "Retry budget (ms)");
    def.define(
        RETRY_EXTRA_ERROR_CODES,
        Type.LIST,
        "",
        Importance.LOW,
        "Extra ORA codes to treat as transient, for example ORA-12345,ORA-54321.",
        GROUP_ERRORS,
        ++o,
        Width.LONG,
        "Extra transient codes");
    def.define(
        LOG_SENSITIVE_DATA,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        "Include row values in error messages and the log: the literal a decode error could not"
            + " read, and the SQL_REDO text around a parse error. Off by default, when those"
            + " messages say the value was withheld; the decode dead letter queue holds the redo"
            + " either way. Values of columns matched by cdc.columns.exclude are never included.",
        GROUP_ERRORS,
        ++o,
        Width.SHORT,
        "Log sensitive data");
    // Snapshots (PRD-02)
    def.define(
        SNAPSHOT_MODE,
        Type.STRING,
        "initial",
        caseInsensitiveEnum(SnapshotMode.class),
        Importance.HIGH,
        "initial reads every captured table's existing rows on the connector's first start, while"
            + " streaming, then streams on; none only streams; snapshot_only reads the tables and"
            + " then stays idle without streaming; on_signal starts no snapshot by itself. A"
            + " snapshot that a stored offset records as unfinished resumes in every mode.",
        GROUP_SNAPSHOTS,
        ++o,
        Width.SHORT,
        "Snapshot mode");
    def.define(
        SNAPSHOT_THREADS,
        Type.INT,
        4,
        Range.between(1, 64),
        Importance.MEDIUM,
        "Chunks of a table read in parallel, each on a connection of its own.",
        GROUP_SNAPSHOTS,
        ++o,
        Width.SHORT,
        "Snapshot threads");
    def.define(
        SNAPSHOT_CHUNK_ROWS,
        Type.INT,
        100_000,
        Range.atLeast(1000),
        Importance.MEDIUM,
        "Target rows per chunk. Each chunk is one flashback query, so smaller chunks need less"
            + " undo; up to cdc.snapshot.max.pending.chunks chunks are held in memory.",
        GROUP_SNAPSHOTS,
        ++o,
        Width.SHORT,
        "Rows per chunk");
    def.define(
        SNAPSHOT_CHUNK_RETRIES,
        Type.INT,
        5,
        Range.atLeast(0),
        Importance.LOW,
        "Times a failed chunk is read again with a fresh SCN before the task stops.",
        GROUP_SNAPSHOTS,
        ++o,
        Width.SHORT,
        "Chunk retries");
    def.define(
        SNAPSHOT_FETCH_SIZE,
        Type.INT,
        5000,
        Range.atLeast(1),
        Importance.LOW,
        "JDBC fetch size of chunk reads.",
        GROUP_SNAPSHOTS,
        ++o,
        Width.SHORT,
        "Snapshot fetch size");
    def.define(
        SNAPSHOT_MAX_PENDING_CHUNKS,
        Type.INT,
        8,
        Range.atLeast(1),
        Importance.LOW,
        "Chunks read but not yet published before reads pause; chunks wait until streaming has"
            + " passed their SCN.",
        GROUP_SNAPSHOTS,
        ++o,
        Width.SHORT,
        "Pending chunks");
    def.define(
        SNAPSHOT_TABLES_ORDER,
        Type.LIST,
        "",
        Importance.LOW,
        "Tables to read first, as PDB.OWNER.TABLE; the others follow in name order. A filter for"
            + " one table's snapshot goes in cdc.snapshot.select.override.PDB.OWNER.TABLE as a SQL"
            + " condition, for example cdc.snapshot.select.override.FREEPDB1.APP.ORDERS=STATUS <>"
            + " 'ARCHIVED'.",
        GROUP_SNAPSHOTS,
        ++o,
        Width.LONG,
        "Snapshot order");
    return def;
  }
}
