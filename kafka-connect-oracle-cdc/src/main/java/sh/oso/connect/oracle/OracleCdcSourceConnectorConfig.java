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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.schema.ColumnFilter;
import sh.oso.connect.oracle.core.schema.KeySelector;

/**
 * Connector configuration: every engine key from {@link CoreConfig} plus the Kafka-facing keys of
 * PRD-01 (topics, keys, formats, polling). The generated configuration reference is produced from
 * {@link #configDef()}.
 */
public class OracleCdcSourceConnectorConfig extends AbstractConfig {

  public static final String GROUP_TOPICS = "Topics";
  public static final String GROUP_FORMAT = "Record format";
  public static final String GROUP_TASK = "Task";
  public static final String GROUP_EOS = "Exactly-once";

  // table selection (PRD-01 SRC-SEL)
  public static final String TABLES_INCLUDE = "cdc.tables.include";
  public static final String TABLES_EXCLUDE = "cdc.tables.exclude";
  public static final String TABLES_CASE_SENSITIVE = "cdc.tables.case.sensitive";
  public static final String COLUMNS_EXCLUDE = "cdc.columns.exclude";
  public static final String USERS_EXCLUDE = "cdc.users.exclude";

  // topics and keys (SRC-TOP)
  public static final String TOPIC_PREFIX = "cdc.topic.prefix";
  public static final String TOPIC_TEMPLATE = "cdc.topic.template";
  public static final String KEY_MISSING = "cdc.key.missing";
  public static final String KEY_COLUMNS = "cdc.key.columns";
  public static final String TOMBSTONES_ON_DELETE = "cdc.tombstones.on.delete";

  // record format (SRC-FMT)
  public static final String OUTPUT_FORMAT = "cdc.output.format";
  public static final String DECIMAL_MODE = "cdc.decimal.mode";
  public static final String TEMPORAL_MODE = "cdc.temporal.mode";

  // internal topics and broker access (SRC-TOP-6)
  public static final String OPS_TOPIC = "cdc.ops.topic";
  public static final String SIGNALS_TOPIC = "cdc.signals.topic";
  public static final String SCHEMA_TOPIC = "cdc.schema.topic";
  public static final String TRANSACTIONS_TOPIC_ENABLED = "cdc.transactions.topic.enabled";
  public static final String TRANSACTIONS_TOPIC = "cdc.transactions.topic";
  public static final String DLQ_TOPIC = "cdc.dlq.topic";
  public static final String KAFKA_BOOTSTRAP_SERVERS = "cdc.kafka.bootstrap.servers";
  public static final String KAFKA_CLIENT_PREFIX = "cdc.kafka.";
  public static final String INTERNAL_TOPIC_REPLICATION = "cdc.internal.topic.replication.factor";
  public static final String JOURNAL_CONVERTER = "cdc.journal.converter";
  public static final String JOURNAL_CONVERTER_PREFIX = "cdc.journal.converter.";

  // heartbeats (SRC-HB-1, CORE-POS-5)
  public static final String HEARTBEAT_INTERVAL_MS = "cdc.heartbeat.interval.ms";
  public static final String HEARTBEAT_TOPIC = "cdc.heartbeat.topic";

  // task (SRC-LC)
  public static final String POLL_MAX_RECORDS = "cdc.poll.max.records";

  // exactly-once (SRC-EOS, ADR-0007)
  public static final String EOS_BATCH_MAX_RECORDS = "cdc.eos.batch.max.records";
  public static final String EOS_BATCH_MAX_MS = "cdc.eos.batch.max.ms";
  public static final String EOS_BATCH_MAX_BYTES = "cdc.eos.batch.max.bytes";
  public static final String EOS_SPLIT_MAX_RECORDS = "cdc.eos.split.max.records";
  public static final String EOS_SPLIT_MAX_BYTES = "cdc.eos.split.max.bytes";
  public static final String POLL_LINGER_MS = "cdc.poll.linger.ms";
  public static final String SHUTDOWN_TIMEOUT_MS = "cdc.shutdown.timeout.ms";

  public static final String DEFAULT_TEMPLATE_CDB = "${prefix}.${pdb}.${schema}.${table}";
  public static final String DEFAULT_TEMPLATE_NON_CDB = "${prefix}.${schema}.${table}";

  public enum DecimalMode {
    PRECISE,
    STRING,
    DOUBLE
  }

  public enum TemporalMode {
    ADAPTIVE,
    ISO_STRING
  }

  private final CoreConfig core;
  private final ColumnFilter columnFilter;

  public OracleCdcSourceConnectorConfig(Map<String, String> props) {
    super(configDef(), props, false);
    this.core = new CoreConfig(props);
    if (!List.of("fail", "rowid", "none")
        .contains(getString(KEY_MISSING).toLowerCase(Locale.ROOT))) {
      throw new ConfigException(KEY_MISSING, getString(KEY_MISSING), "must be fail, rowid or none");
    }
    this.columnFilter = ColumnFilter.of(getList(COLUMNS_EXCLUDE), tablesCaseSensitive());
    for (Map.Entry<String, List<String>> e : keyOverrides().entrySet()) {
      for (String column : e.getValue()) {
        if (columnFilter.excludesName(e.getKey() + "." + column)) {
          throw new ConfigException(
              COLUMNS_EXCLUDE,
              String.join(",", getList(COLUMNS_EXCLUDE)),
              "matches key column "
                  + e.getKey()
                  + "."
                  + column
                  + " named in "
                  + KEY_COLUMNS
                  + "; a key column cannot be excluded");
        }
      }
    }
  }

  /** SRC-SEL-2: the columns kept out of records, shared by the engine, envelope and snapshots. */
  public ColumnFilter columnFilter() {
    return columnFilter;
  }

  public CoreConfig core() {
    return core;
  }

  public String topicPrefix() {
    return getString(TOPIC_PREFIX);
  }

  /** The topic template, defaulting by database kind when not set. */
  public String topicTemplate(boolean cdb) {
    String t = getString(TOPIC_TEMPLATE);
    if (t == null || t.isBlank()) {
      return cdb ? DEFAULT_TEMPLATE_CDB : DEFAULT_TEMPLATE_NON_CDB;
    }
    return t;
  }

  public List<String> tablesInclude() {
    return getList(TABLES_INCLUDE);
  }

  public List<String> tablesExclude() {
    return getList(TABLES_EXCLUDE);
  }

  public List<String> usersExclude() {
    return getList(USERS_EXCLUDE);
  }

  public Password databasePassword() {
    return core.databasePassword();
  }

  public KeySelector.MissingKeyPolicy keyMissing() {
    return KeySelector.MissingKeyPolicy.parse(getString(KEY_MISSING));
  }

  /** {@code SCHEMA.TABLE:COL1,COL2;PDB.SCHEMA.TABLE:COL} as upper-cased table to columns. */
  public Map<String, List<String>> keyOverrides() {
    String raw = getString(KEY_COLUMNS);
    Map<String, List<String>> out = new LinkedHashMap<>();
    if (raw == null || raw.isBlank()) {
      return out;
    }
    for (String entry : raw.split(";")) {
      if (entry.isBlank()) {
        continue;
      }
      int colon = entry.indexOf(':');
      if (colon <= 0 || colon == entry.length() - 1) {
        throw new ConfigException(KEY_COLUMNS, raw, "entries are TABLE:COL1,COL2 separated by ;");
      }
      List<String> cols = new ArrayList<>();
      for (String c : entry.substring(colon + 1).split(",")) {
        if (!c.isBlank()) {
          cols.add(c.trim().toUpperCase(Locale.ROOT));
        }
      }
      out.put(entry.substring(0, colon).trim().toUpperCase(Locale.ROOT), cols);
    }
    return out;
  }

  public int eosBatchMaxRecords() {
    return getInt(EOS_BATCH_MAX_RECORDS);
  }

  public long eosBatchMaxMs() {
    return getLong(EOS_BATCH_MAX_MS);
  }

  public long eosBatchMaxBytes() {
    return getLong(EOS_BATCH_MAX_BYTES);
  }

  public int eosSplitMaxRecords() {
    return getInt(EOS_SPLIT_MAX_RECORDS);
  }

  public long eosSplitMaxBytes() {
    return getLong(EOS_SPLIT_MAX_BYTES);
  }

  public boolean tombstonesOnDelete() {
    return getBoolean(TOMBSTONES_ON_DELETE);
  }

  public DecimalMode decimalMode() {
    return DecimalMode.valueOf(getString(DECIMAL_MODE).toUpperCase(Locale.ROOT));
  }

  public TemporalMode temporalMode() {
    return TemporalMode.valueOf(getString(TEMPORAL_MODE).toUpperCase(Locale.ROOT));
  }

  private String expand(String key) {
    return getString(key).replace("${prefix}", topicPrefix());
  }

  public String opsTopic() {
    return expand(OPS_TOPIC);
  }

  public String signalsTopic() {
    return expand(SIGNALS_TOPIC);
  }

  public String schemaTopic() {
    return expand(SCHEMA_TOPIC);
  }

  public String dlqTopic() {
    return expand(DLQ_TOPIC);
  }

  public boolean transactionsTopicEnabled() {
    return getBoolean(TRANSACTIONS_TOPIC_ENABLED);
  }

  public String transactionsTopic() {
    return expand(TRANSACTIONS_TOPIC);
  }

  /** Bootstrap servers for the connector's own clients (admin, journal, schema), or null. */
  public String kafkaBootstrapServers() {
    String s = getString(KAFKA_BOOTSTRAP_SERVERS);
    return s == null || s.isBlank() ? null : s;
  }

  /** {@code cdc.kafka.*} properties as client properties, with the prefix removed. */
  public java.util.Properties kafkaClientProperties() {
    java.util.Properties p = new java.util.Properties();
    for (Map.Entry<String, Object> e : originalsWithPrefix(KAFKA_CLIENT_PREFIX).entrySet()) {
      p.put(e.getKey(), String.valueOf(e.getValue()));
    }
    return p;
  }

  public short internalTopicReplication() {
    return getShort(INTERNAL_TOPIC_REPLICATION);
  }

  /** Converter class the journal loader uses to read the journal topic back. */
  public String journalConverter() {
    return getString(JOURNAL_CONVERTER);
  }

  /** {@code cdc.journal.converter.*} as converter configuration, prefix removed. */
  public Map<String, Object> journalConverterProperties() {
    return new HashMap<>(originalsWithPrefix(JOURNAL_CONVERTER_PREFIX));
  }

  /** CORE-TX-4 thresholds as a policy; -1 disables the corresponding threshold. */
  public sh.oso.connect.oracle.core.buffer.JournalPolicy journalPolicy() {
    long ageMs =
        core().getLong(sh.oso.connect.oracle.core.config.CoreConfig.TXJOURNAL_THRESHOLD_MS);
    long events =
        core().getLong(sh.oso.connect.oracle.core.config.CoreConfig.TXJOURNAL_THRESHOLD_EVENTS);
    return new sh.oso.connect.oracle.core.buffer.JournalPolicy(
        ageMs <= 0 ? null : java.time.Duration.ofMillis(ageMs),
        events <= 0 ? Long.MAX_VALUE : events,
        core().getInt(sh.oso.connect.oracle.core.config.CoreConfig.TXJOURNAL_CHUNK_MAX_BYTES));
  }

  /** The journal topic name with the default next to the other internal topics. */
  public String journalTopic() {
    String t = core().getString(sh.oso.connect.oracle.core.config.CoreConfig.TXJOURNAL_TOPIC);
    return t == null || t.isBlank() ? topicPrefix() + ".cdc.txjournal" : t;
  }

  public long heartbeatIntervalMs() {
    return getLong(HEARTBEAT_INTERVAL_MS);
  }

  /** The heartbeat topic; {@code ${prefix}} expands to the topic prefix. */
  public String heartbeatTopic() {
    return getString(HEARTBEAT_TOPIC).replace("${prefix}", topicPrefix());
  }

  public int pollMaxRecords() {
    return getInt(POLL_MAX_RECORDS);
  }

  public long pollLingerMs() {
    return getLong(POLL_LINGER_MS);
  }

  public long shutdownTimeoutMs() {
    return getLong(SHUTDOWN_TIMEOUT_MS);
  }

  public boolean tablesCaseSensitive() {
    return getBoolean(TABLES_CASE_SENSITIVE);
  }

  /** The raw properties, for building the core configuration elsewhere. */
  public Map<String, String> rawProperties() {
    return new HashMap<>(originalsStrings());
  }

  /** Every entry of a pattern list must compile as a regular expression. */
  static final ConfigDef.Validator PATTERNS =
      (name, value) -> {
        if (value instanceof List<?> list) {
          for (Object p : list) {
            try {
              java.util.regex.Pattern.compile(String.valueOf(p).trim());
            } catch (java.util.regex.PatternSyntaxException e) {
              throw new ConfigException(name, p, "not a regular expression: " + e.getDescription());
            }
          }
        }
      };

  public static ConfigDef configDef() {
    ConfigDef def = CoreConfig.configDef();
    int o = 0;
    def.define(
        TOPIC_PREFIX,
        Type.STRING,
        ConfigDef.NO_DEFAULT_VALUE,
        new ConfigDef.NonEmptyString(),
        Importance.HIGH,
        "Prefix of every topic this connector writes; also the logical server name in the"
            + " Debezium-compatible envelope and the offset partition.",
        GROUP_TOPICS,
        ++o,
        Width.MEDIUM,
        "Topic prefix");
    def.define(
        TOPIC_TEMPLATE,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Topic name template. Variables: ${prefix}, ${pdb}, ${schema}, ${table}, ${database}."
            + " Default "
            + DEFAULT_TEMPLATE_CDB
            + " in a CDB and "
            + DEFAULT_TEMPLATE_NON_CDB
            + " otherwise. Characters Kafka does not allow become underscores.",
        GROUP_TOPICS,
        ++o,
        Width.LONG,
        "Topic template");
    def.define(
        TABLES_INCLUDE,
        Type.LIST,
        ConfigDef.NO_DEFAULT_VALUE,
        Importance.HIGH,
        "Comma-separated regular expressions over PDB.SCHEMA.TABLE (CDB) or SCHEMA.TABLE (non-CDB)"
            + " selecting the tables to capture.",
        GROUP_TOPICS,
        ++o,
        Width.LONG,
        "Tables to include");
    def.define(
        TABLES_EXCLUDE,
        Type.LIST,
        "",
        Importance.MEDIUM,
        "Comma-separated regular expressions removing tables from the included set.",
        GROUP_TOPICS,
        ++o,
        Width.LONG,
        "Tables to exclude");
    def.define(
        TABLES_CASE_SENSITIVE,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        "Match table and column patterns case-sensitively. Oracle stores unquoted names in upper"
            + " case.",
        GROUP_TOPICS,
        ++o,
        Width.SHORT,
        "Case-sensitive patterns");
    def.define(
        COLUMNS_EXCLUDE,
        Type.LIST,
        "",
        PATTERNS,
        Importance.MEDIUM,
        "Comma-separated regular expressions over PDB.SCHEMA.TABLE.COLUMN (CDB) or"
            + " SCHEMA.TABLE.COLUMN (non-CDB) naming columns to leave out of every record, matched"
            + " against the whole name and case-insensitively unless"
            + " cdc.tables.case.sensitive=true. An excluded column is dropped while its row is"
            + " decoded, before its value is converted, so it never reaches a record, a Connect"
            + " schema, the transaction buffer, the spill files, the transaction journal or a log"
            + " line; snapshots do not select it and reselect never fetches it. Rows of a table"
            + " these patterns may match go to the DLQ without their SQL_REDO and SQL_UNDO, and a"
            + " statement that fails to parse is reported without quoting it. A column of the"
            + " record key (primary key, unique index or cdc.key.columns) cannot be excluded:"
            + " validation reports it, and a task that finds one at start or after a DDL stops with"
            + " CDC-3001. A column added later that matches is excluded from its first row.",
        GROUP_TOPICS,
        ++o,
        Width.LONG,
        "Columns to exclude");
    def.define(
        USERS_EXCLUDE,
        Type.LIST,
        "",
        Importance.LOW,
        "Oracle users whose transactions are dropped in the mining query, so they never create"
            + " transactions in the buffer (for example a replication or GoldenGate user).",
        GROUP_TOPICS,
        ++o,
        Width.LONG,
        "Users to exclude");
    def.define(
        KEY_MISSING,
        Type.STRING,
        "fail",
        Importance.MEDIUM,
        "What to do with a captured table that has neither a primary key nor a NOT NULL unique"
            + " index: fail (at validation), rowid (key records by ROWID; a moved row changes its"
            + " key) or none (no key).",
        GROUP_TOPICS,
        ++o,
        Width.SHORT,
        "Missing key policy");
    def.define(
        KEY_COLUMNS,
        Type.STRING,
        "",
        Importance.LOW,
        "Per-table key override as SCHEMA.TABLE:COL1,COL2;SCHEMA.OTHER:COL, taking precedence over"
            + " the primary key.",
        GROUP_TOPICS,
        ++o,
        Width.LONG,
        "Key column overrides");
    def.define(
        TOMBSTONES_ON_DELETE,
        Type.BOOLEAN,
        true,
        Importance.MEDIUM,
        "Emit a null-valued record after every delete so compacted topics drop the key.",
        GROUP_TOPICS,
        ++o,
        Width.SHORT,
        "Tombstones on delete");
    int f = 0;
    def.define(
        OUTPUT_FORMAT,
        Type.STRING,
        "debezium",
        ConfigDef.ValidString.in("debezium"),
        Importance.MEDIUM,
        "Record envelope. Only debezium (before, after, source, op, ts_ms) is available; a"
            + " Confluent-compatible flat format is not built yet.",
        GROUP_FORMAT,
        ++f,
        Width.SHORT,
        "Output format");
    def.define(
        DECIMAL_MODE,
        Type.STRING,
        "precise",
        ConfigDef.CaseInsensitiveValidString.in("precise", "string", "double"),
        Importance.MEDIUM,
        "NUMBER handling: precise (Connect Decimal; unconstrained NUMBER and FLOAT as a variable"
            + " scale decimal struct), string, or double.",
        GROUP_FORMAT,
        ++f,
        Width.SHORT,
        "Decimal mode");
    def.define(
        TEMPORAL_MODE,
        Type.STRING,
        "adaptive",
        ConfigDef.CaseInsensitiveValidString.in("adaptive", "iso_string"),
        Importance.MEDIUM,
        "DATE, TIMESTAMP and INTERVAL handling: adaptive (Debezium semantic types sized to the"
            + " column precision) or iso_string (ISO 8601 text).",
        GROUP_FORMAT,
        ++f,
        Width.SHORT,
        "Temporal mode");
    def.define(
        OPS_TOPIC,
        Type.STRING,
        "${prefix}.cdc.ops",
        Importance.LOW,
        "Topic for the connector's operational events (task start and stop, DDL seen, decode"
            + " failures, reconnects, discarded or released transactions, signal acknowledgements);"
            + " ${prefix} expands to the topic prefix.",
        GROUP_TOPICS,
        ++o,
        Width.MEDIUM,
        "Ops topic");
    def.define(
        SIGNALS_TOPIC,
        Type.STRING,
        "${prefix}.cdc.signals",
        Importance.LOW,
        "Topic the connector reads signals from (snapshot, refresh-tables, log-state).",
        GROUP_TOPICS,
        ++o,
        Width.MEDIUM,
        "Signals topic");
    def.define(
        SCHEMA_TOPIC,
        Type.STRING,
        "${prefix}.cdc.schema",
        Importance.LOW,
        "Compacted topic holding table schema versions.",
        GROUP_TOPICS,
        ++o,
        Width.MEDIUM,
        "Schema topic");
    def.define(
        DLQ_TOPIC,
        Type.STRING,
        "${prefix}.cdc.dlq",
        Importance.LOW,
        "Dead letter topic for rows that could not be decoded or that LogMiner marked unsupported"
            + " (used only with cdc.on.decode.error=dlq) and for transactions discarded by"
            + " cdc.transaction.max.age.action=discard. Records carry the raw SQL_REDO, SCN, XID,"
            + " table and exception; for a table whose columns cdc.columns.exclude may match, the"
            + " SQL_REDO and SQL_UNDO are withheld.",
        GROUP_TOPICS,
        ++o,
        Width.MEDIUM,
        "DLQ topic");
    def.define(
        TRANSACTIONS_TOPIC_ENABLED,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        "Creates the transaction metadata topic when the connector has broker access. Writing BEGIN"
            + " and END records per Oracle transaction to it is not built yet, so the topic stays"
            + " empty in this release.",
        GROUP_TOPICS,
        ++o,
        Width.SHORT,
        "Transaction metadata");
    def.define(
        TRANSACTIONS_TOPIC,
        Type.STRING,
        "${prefix}.cdc.transactions",
        Importance.LOW,
        "Transaction metadata topic (see cdc.transactions.topic.enabled).",
        GROUP_TOPICS,
        ++o,
        Width.MEDIUM,
        "Transactions topic");
    def.define(
        KAFKA_BOOTSTRAP_SERVERS,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Bootstrap servers for the connector's own Kafka clients: the admin client that creates the"
            + " internal topics with the right cleanup policy, and the readers of the schema and"
            + " journal topics. Other client settings go under cdc.kafka.*. When unset the"
            + " connector relies on the worker's topic creation and on pre-created topics.",
        GROUP_TOPICS,
        ++o,
        Width.LONG,
        "Kafka bootstrap servers");
    def.define(
        INTERNAL_TOPIC_REPLICATION,
        Type.SHORT,
        (short) -1,
        Importance.LOW,
        "Replication factor for internal topics the connector creates; -1 uses the broker default.",
        GROUP_TOPICS,
        ++o,
        Width.SHORT,
        "Internal topic replication");
    def.define(
        JOURNAL_CONVERTER,
        Type.STRING,
        "sh.oso.connect.oracle.journal.TolerantJsonConverter",
        Importance.LOW,
        "Converter the task uses to read the transaction journal and schema topics back at start."
            + " The default"
            + " reads JSON written with or without the schema envelope, so it matches a worker"
            + " using the JSON converter in either mode. Set it to the worker's converter class"
            + " when the worker uses another converter (Avro, Protobuf); settings for it go under"
            + " cdc.journal.converter.*.",
        GROUP_TOPICS,
        ++o,
        Width.LONG,
        "Journal converter");
    def.define(
        HEARTBEAT_INTERVAL_MS,
        Type.LONG,
        10_000L,
        ConfigDef.Range.between(0L, 3_600_000L),
        Importance.MEDIUM,
        "How often a heartbeat record carrying the current position is written when no change"
            + " records flow, so Connect commits offsets on a quiet database and the start position"
            + " of a new connector becomes durable at once. 0 disables periodic heartbeats; the"
            + " start heartbeat is always written.",
        GROUP_TOPICS,
        ++o,
        Width.SHORT,
        "Heartbeat interval");
    def.define(
        HEARTBEAT_TOPIC,
        Type.STRING,
        "${prefix}.cdc.heartbeat",
        Importance.LOW,
        "Topic for heartbeat records; ${prefix} expands to the topic prefix.",
        GROUP_TOPICS,
        ++o,
        Width.MEDIUM,
        "Heartbeat topic");
    int t = 0;
    def.define(
        POLL_MAX_RECORDS,
        Type.INT,
        2000,
        ConfigDef.Range.between(1, 100_000),
        Importance.LOW,
        "Most records one poll() returns.",
        GROUP_TASK,
        ++t,
        Width.SHORT,
        "Poll batch size");
    def.define(
        POLL_LINGER_MS,
        Type.LONG,
        50L,
        ConfigDef.Range.between(0L, 60_000L),
        Importance.LOW,
        "How long poll() waits for the first record before returning nothing.",
        GROUP_TASK,
        ++t,
        Width.SHORT,
        "Poll linger");
    def.define(
        SHUTDOWN_TIMEOUT_MS,
        Type.LONG,
        30_000L,
        ConfigDef.Range.between(1_000L, 600_000L),
        Importance.LOW,
        "How long stop() waits for the engine thread before interrupting it.",
        GROUP_TASK,
        ++t,
        Width.SHORT,
        "Shutdown timeout");
    int e = 0;
    def.define(
        EOS_BATCH_MAX_RECORDS,
        Type.INT,
        5000,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        "With exactly.once.support and transaction.boundary=connector: a Kafka transaction is"
            + " committed at the first Oracle commit after this many records.",
        GROUP_EOS,
        ++e,
        Width.SHORT,
        "Batch records");
    def.define(
        EOS_BATCH_MAX_MS,
        Type.LONG,
        500L,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        "A Kafka transaction is committed at the first Oracle commit after it has been open this"
            + " long, or as soon as no more records are waiting.",
        GROUP_EOS,
        ++e,
        Width.SHORT,
        "Batch time");
    def.define(
        EOS_BATCH_MAX_BYTES,
        Type.LONG,
        16777216L,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        "A Kafka transaction is committed at the first Oracle commit after about this many bytes"
            + " of change data (ADR-0007).",
        GROUP_EOS,
        ++e,
        Width.SHORT,
        "Batch bytes");
    def.define(
        EOS_SPLIT_MAX_RECORDS,
        Type.INT,
        500000,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        "An Oracle transaction with more changes than this is split into consecutive Kafka"
            + " transactions at exact event indexes, so none outlives transaction.max.timeout.ms;"
            + " its records carry the cdc.split header and an ops event names it.",
        GROUP_EOS,
        ++e,
        Width.SHORT,
        "Split records");
    def.define(
        EOS_SPLIT_MAX_BYTES,
        Type.LONG,
        268435456L,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        "An Oracle transaction is also split after about this many bytes of change data.",
        GROUP_EOS,
        ++e,
        Width.SHORT,
        "Split bytes");
    return def;
  }
}
