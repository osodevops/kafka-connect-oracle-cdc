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
        "Kerberos credential cache file (Phase 2).",
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
        "Idle network guard: an application-level probe runs at half this interval so load"
            + " balancers with idle timeouts never hang the connector.",
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
        "Subscribe to RAC Fast Application Notification events.",
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
        "online mines online and archived logs; archive_only never adds online logs (required for"
            + " standby capture).",
        GROUP_CAPTURE,
        ++o,
        Width.SHORT,
        "Capture mode");
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
            + " halved; it never fails the task by itself.",
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
        Range.between(1, 100000),
        Importance.LOW,
        "Above this many object ids the mining query joins a temporary table instead of using an IN"
            + " list.",
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
        "Lag that enables parallel catch-up mining (Phase 2).",
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
        "Catch-up sessions over adjacent SCN windows (Phase 2).",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "Catch-up parallelism");
    def.define(
        RAC_SAFETY_LAG_MS,
        Type.LONG,
        -1L,
        Importance.LOW,
        "Hold-back from the cluster SCN on RAC so late-archiving threads are not missed; -1 means"
            + " 3000 on RAC and 0 otherwise.",
        GROUP_MINING,
        ++o,
        Width.SHORT,
        "RAC safety lag (ms)");
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
        "Spill directory. Default: the worker's temporary directory plus the connector name.",
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
        "Allow row values in log output. Off by default.",
        GROUP_ERRORS,
        ++o,
        Width.SHORT,
        "Log sensitive data");
    return def;
  }
}
