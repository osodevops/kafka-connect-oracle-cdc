# Copyright 2026 OSO DevOps Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""``migrate_from_debezium``: Debezium Oracle connector configuration to ``cdc.*`` (PRD-01
Appendix A, PRD-04 MIG-1 to MIG-7)."""

from __future__ import annotations

import re
import sys

from . import OSO_CONNECTOR_CLASS
from . import translate as t
from .redact import is_secret_key
from .tables import group, pdb_prefix, split_list, strip_anchors, strip_first_segment
from .translate import Classification, Context, Manual, Outcome, RuleSet
from .translator_cli import TranslatorSpec, translator_main

DEBEZIUM_CLASS = "io.debezium.connector.oracle.OracleConnector"
DEBEZIUM_PLACEHOLDER = "__debezium_unavailable_value"
DEFAULT_NAMING_STRATEGIES = {
    "io.debezium.schema.SchemaTopicNamingStrategy",
    "io.debezium.schema.DefaultTopicNamingStrategy",
}

TAKEOVER_SNAPSHOT_NOTE = (
    "The takeover starts the new connector at the Debezium position (`cdc.start.scn` from"
    " takeover_scn.py), so no snapshot runs: `cdc.snapshot.mode=none`."
)
FRESH_SNAPSHOT_MODES = {
    "initial": "initial",
    "initial_only": "snapshot_only",
    "no_data": "none",
    "schema_only": "none",
    "never": "none",
}


def _bool(value: str) -> bool:
    return value.strip().lower() == "true"


def _int(key: str, value: str) -> int:
    try:
        return int(value.strip())
    except ValueError:
        raise Manual(f"`{key}` is not a whole number; set the target by hand.") from None


# Rules -----------------------------------------------------------------------------------------


def connector_class(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("connector.class", OSO_CONNECTOR_CLASS, key)
    return Classification.MAPPED_WITH_CHANGE, "Replaced by the OSO CDC Connector class."


def name(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("name", ctx.target_name, key)
    return (
        Classification.MAPPED_WITH_CHANGE,
        "The new connector has its own name: Kafka Connect keeps offsets per connector name, and"
        " this connector does not read the Debezium offset format.",
    )


def tasks_max(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("tasks.max", "1", key)
    if value.strip() == "1":
        return Classification.MAPPED, ""
    return (
        Classification.MAPPED_WITH_CHANGE,
        "The connector always runs one task, as the Debezium Oracle connector does.",
    )


def connection_adapter(ctx: Context, key: str, value: str) -> Outcome:
    adapter = value.strip().lower()
    if adapter in ("logminer", "logminer_unbuffered"):
        return (
            Classification.DROPPED,
            "The connector reads redo only through LogMiner and buffers open transactions"
            " itself, spilling to disk and journaling long ones.",
        )
    if adapter in ("xstream", "olr"):
        raise Manual(
            f"The source used the `{adapter}` adapter. This connector reads redo only through"
            " LogMiner: grant the LogMiner privileges and check supplemental logging with"
            " `oracle-cdc-doctor check` before migrating. The Debezium offset of this adapter may"
            " carry no SCN that `takeover_scn.py` can use."
        )
    raise Manual(f"Unknown adapter `{adapter}`; the connector reads redo through LogMiner.")


def topic_prefix(ctx: Context, key: str, value: str) -> Outcome:
    if key == "database.server.name" and ctx.has("topic.prefix"):
        return (
            Classification.DROPPED,
            "Debezium 1.x name of the topic prefix; `topic.prefix` is also set and wins.",
        )
    ctx.set("cdc.topic.prefix", value.strip(), key)
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Topic prefix and logical server name. `cdc.topic.template` is pinned so the topic names"
        " stay `prefix.SCHEMA.TABLE`.",
    )


def topic_delimiter(ctx: Context, key: str, value: str) -> Outcome:
    return Classification.MAPPED_WITH_CHANGE, "Used as the separator in `cdc.topic.template`."


def naming_strategy(ctx: Context, key: str, value: str) -> Outcome:
    if value.strip() in DEFAULT_NAMING_STRATEGIES:
        if value.strip().endswith("DefaultTopicNamingStrategy"):
            raise Manual(
                "`DefaultTopicNamingStrategy` names topics after the full data collection id."
                " Compare the existing topic names with `cdc.topic.template` and set the template"
                " so they match."
            )
        return (
            Classification.DROPPED,
            "The pinned `cdc.topic.template` reproduces this naming.",
        )
    raise Manual(
        "A custom topic naming strategy has no equivalent. Express the topic names with"
        " `cdc.topic.template` (variables `${prefix}`, `${pdb}`, `${schema}`, `${table}`,"
        " `${database}`) or route them with a transform."
    )


def filter_list(ctx: Context, key: str, value: str) -> Outcome:
    split_list(value, key)  # an escaped comma is manual
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over"
        " `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB).",
    )


COLUMN_FILTER = (
    "Write the columns this property kept out in `cdc.columns.exclude`, as regular expressions"
    " over `PDB.SCHEMA.TABLE.COLUMN` (or `SCHEMA.TABLE.COLUMN` without a PDB); the patterns are"
    " not rewritten for you because the name forms differ. Key columns cannot be excluded. Until"
    " it is set, every column of a captured table is published."
)

COLUMN_INCLUDE = (
    "There is no column include list: name the columns to leave out in `cdc.columns.exclude`,"
    " as regular expressions over `PDB.SCHEMA.TABLE.COLUMN` (or `SCHEMA.TABLE.COLUMN` without a"
    " PDB). Until it is set, every column of a captured table is published."
)


def message_key_columns(ctx: Context, key: str, value: str) -> Outcome:
    entries = []
    for entry in value.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        table, sep, columns = entry.partition(":")
        table = table.strip().replace("\\.", ".")
        if not sep or not re.fullmatch(r"[A-Za-z0-9_$#]+(\.[A-Za-z0-9_$#]+){1,2}", table):
            raise Manual(
                f"`{entry}` names its table with a regular expression or an unexpected form."
                " `cdc.key.columns` takes literal `SCHEMA.TABLE:COL1,COL2` entries; write them"
                " out by hand."
            )
        cols = [c.strip() for c in columns.split(",") if c.strip()]
        if not cols or any(not re.fullmatch(r"[A-Za-z0-9_$#]+", c) for c in cols):
            raise Manual(f"`{entry}` has no usable column list; set `cdc.key.columns` by hand.")
        entries.append(f"{table.upper()}:{','.join(c.upper() for c in cols)}")
    ctx.set("cdc.key.columns", ";".join(entries), key)
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Rewritten as `SCHEMA.TABLE:COL1,COL2` entries in upper case.",
    )


def snapshot_mode(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("cdc.snapshot.mode", "none", key)
    mode = value.strip().lower()
    fresh = FRESH_SNAPSHOT_MODES.get(mode)
    if fresh:
        tail = f" For a fresh deployment instead, `{mode}` corresponds to `{fresh}`."
    else:
        tail = f" `{mode}` has no equivalent for a fresh deployment."
    return Classification.MAPPED_WITH_CHANGE, TAKEOVER_SNAPSHOT_NOTE + tail


def decimal_mode(ctx: Context, key: str, value: str) -> Outcome:
    mode = value.strip().lower()
    if mode in ("precise", "string", "double"):
        ctx.set("cdc.decimal.mode", mode, key)
        return Classification.MAPPED, ""
    raise Manual(f"`{mode}` is not a known decimal handling mode; choose `cdc.decimal.mode`.")


def time_precision(ctx: Context, key: str, value: str) -> Outcome:
    mode = value.strip().lower()
    if mode == "adaptive":
        ctx.set("cdc.temporal.mode", "adaptive", key)
        return Classification.MAPPED, ""
    if mode == "adaptive_time_microseconds":
        ctx.set("cdc.temporal.mode", "adaptive", key)
        return (
            Classification.MAPPED_WITH_CHANGE,
            "Oracle Database has no TIME type, so this behaves as `adaptive`.",
        )
    if mode == "isostring":
        ctx.set("cdc.temporal.mode", "iso_string", key)
        return Classification.MAPPED_WITH_CHANGE, "ISO 8601 text for every temporal type."
    raise Manual(
        f"`time.precision.mode={mode}` has no equivalent. The connector offers `adaptive`"
        " (Debezium semantic types sized to the column precision) and `iso_string`; consumers"
        " that read the old representation must change."
    )


def binary_mode(ctx: Context, key: str, value: str) -> Outcome:
    if value.strip().lower() == "bytes":
        return (
            Classification.DROPPED,
            "Binary columns are published as bytes, the only representation in this release.",
        )
    raise Manual(
        f"`binary.handling.mode={value.strip()}` has no equivalent: binary columns are published"
        " as bytes in this release. Consumers that expect text must decode the bytes."
    )


def interval_mode(ctx: Context, key: str, value: str) -> Outcome:
    if value.strip().lower() == "numeric":
        return (
            Classification.DROPPED,
            "With `cdc.temporal.mode=adaptive` intervals are published as microseconds"
            " (`io.debezium.time.MicroDuration`), as in numeric mode.",
        )
    raise Manual(
        "Intervals as strings are only available with `cdc.temporal.mode=iso_string`, which also"
        " turns dates and timestamps into text. Choose one by hand."
    )


def lob_enabled(ctx: Context, key: str, value: str) -> Outcome:
    if _bool(value):
        ctx.set("cdc.lob.mode", "inline", key)
        return (
            Classification.MAPPED_WITH_CHANGE,
            "LOB values are assembled from redo; values the redo does not carry are published as"
            " `cdc.unavailable.placeholder`. `reselect` also reads them from the table.",
        )
    ctx.set("cdc.lob.mode", "skip", key)
    return Classification.MAPPED, "LOB columns are left out of the records."


def heartbeat_interval(ctx: Context, key: str, value: str) -> Outcome:
    if _int(key, value) > 0:
        ctx.set("cdc.heartbeat.interval.ms", value.strip(), key)
        return Classification.MAPPED, ""
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Not copied: the connector's heartbeats (default every 10 seconds) keep offsets moving on"
        " a quiet database and never write to the source database.",
    )


def retention(ctx: Context, key: str, value: str) -> Outcome:
    amount = _int(key, value)
    if amount <= 0:
        return Classification.DROPPED, "No limit, which is also the connector's default."
    ms = amount * 3_600_000 if key.endswith(".hours") else amount
    ms = max(ms, 1000)
    ctx.set("cdc.transaction.max.age.ms", str(ms), key)
    ctx.set("cdc.transaction.max.age.action", "discard", key)
    return (
        Classification.MAPPED_WITH_CHANGE,
        "A transaction open longer than this is discarded, as Debezium did, but its details go to"
        " the ops topic and the DLQ. `cdc.transaction.max.age.action=fail` stops instead.",
    )


def archive_only(ctx: Context, key: str, value: str) -> Outcome:
    if _bool(value):
        ctx.set("cdc.capture.mode", "archive_only", key)
        return Classification.MAPPED_WITH_CHANGE, "Mines archived logs only, never online logs."
    ctx.set("cdc.capture.mode", "online", key)
    return Classification.MAPPED, ""


def archive_destination(ctx: Context, key: str, value: str) -> Outcome:
    if "," in value:
        raise Manual(
            "The connector reads one archive destination; choose one for `cdc.archive.destination`."
        )
    ctx.set("cdc.archive.destination", value.strip(), key)
    return Classification.MAPPED, ""


def session_max(ctx: Context, key: str, value: str) -> Outcome:
    if _int(key, value) > 0:
        ctx.set("cdc.mining.session.max.age.ms", value.strip(), key)
        return Classification.MAPPED, ""
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Not copied: the connector restarts the LogMiner session every"
        " `cdc.mining.session.max.age.ms` (default one hour) to release PGA memory.",
    )


def failure_mode(ctx: Context, key: str, value: str) -> Outcome:
    mode = value.strip().lower()
    if mode == "fail":
        ctx.set("cdc.on.decode.error", "fail", key)
        return Classification.MAPPED, ""
    if mode in ("warn", "skip"):
        ctx.set("cdc.on.decode.error", "dlq", key)
        ctx.follow_up(
            f"`event.processing.failure.handling.mode={mode}` skipped rows that could not be"
            " decoded. The connector never skips silently: such rows go to the DLQ topic"
            " (`cdc.dlq.topic`) with an ops event. Make sure someone watches the DLQ.",
            key,
        )
        return Classification.MAPPED_WITH_CHANGE, "Undecodable rows go to the DLQ topic."
    raise Manual(f"Unknown failure handling mode `{mode}`; choose `cdc.on.decode.error`.")


def skipped_operations(ctx: Context, key: str, value: str) -> Outcome:
    ops = {o.strip().lower() for o in value.split(",") if o.strip()}
    if ops == {"t"}:
        return (
            Classification.DROPPED,
            "Truncate records are not published in this release, which matches skipping `t`.",
        )
    if ops in ({"none"}, set()):
        raise Manual(
            "The source published truncate records; this release does not. Consumers that"
            " act on truncates must be told by other means."
        )
    raise Manual(
        "There is no operation filter: creates, updates and deletes are always published. Filter"
        " them downstream or with a transform."
    )


def transaction_metadata(ctx: Context, key: str, value: str) -> Outcome:
    if not _bool(value):
        return Classification.DROPPED, "Off in the source as well."
    raise Manual(
        "Every record carries the `transaction` block (id, total order, data collection order),"
        " but BEGIN and END records on a transaction topic are not written in this release."
        " Consumers of the Debezium transaction topic must wait or change."
    )


def schema_changes(ctx: Context, key: str, value: str) -> Outcome:
    if _bool(value):
        return (
            Classification.DROPPED,
            "Schema change events are not published; see the follow-up on the schema change topic.",
        )
    return Classification.DROPPED, "Off in the source as well."


def history(ctx: Context, key: str, value: str) -> Outcome:
    if ctx.once("history"):
        ctx.follow_up(
            "The schema history topic is no longer needed: the connector keeps table schema"
            " versions in its own compacted schema topic (`cdc.schema.topic`). Keep the history"
            " topic until the cutover is verified, then delete it."
        )
    return (
        Classification.DROPPED,
        "Schema history is replaced by the connector's schema topic, created on start.",
    )


def signal_table(ctx: Context, key: str, value: str) -> Outcome:
    ctx.follow_up(
        "Signals are read from a Kafka topic (`cdc.signals.topic`) in this connector's own"
        " command format, never from a table. Move any automation that writes to the signal"
        " table, and drop the table after the cutover.",
        key,
    )
    return Classification.DROPPED, "The signal table is replaced by the signal topic."


def flag_off_or_manual(instruction: str, off_values: tuple[str, ...] = ("false", "none")):
    def rule(ctx: Context, key: str, value: str) -> Outcome:
        if value.strip().lower() in off_values:
            return Classification.DROPPED, "Off in the source as well."
        raise Manual(instruction)

    return rule


SCHEMA_ADJUSTMENT = "cdc.schema.name.adjustment.mode"
FIELD_ADJUSTMENT = "cdc.field.name.adjustment.mode"
ADJUSTMENT_MODES = ("none", "avro", "avro_unicode")


def adjustment_mode(target: str):
    def rule(ctx: Context, key: str, value: str) -> Outcome:
        mode = value.strip().lower()
        if mode not in ADJUSTMENT_MODES:
            raise Manual(
                f"`{mode}` is not a known adjustment mode; set `{target}` to `none`, `avro` or"
                " `avro_unicode`."
            )
        ctx.set(target, mode, key)
        return Classification.MAPPED, ""

    return rule


def sanitize_field_names(ctx: Context, key: str, value: str) -> Outcome:
    flag = value.strip().lower()
    if flag not in ("true", "false"):
        raise Manual(f"`{key}` is neither `true` nor `false`; set `{FIELD_ADJUSTMENT}` by hand.")
    mode = "avro" if flag == "true" else "none"
    ctx.set(FIELD_ADJUSTMENT, mode, key)
    return (
        Classification.MAPPED_WITH_CHANGE,
        f"Debezium 1.x switch; `{mode}` adjusts field names the same way.",
    )


_TRUSTSTORE = {
    "driver.javax.net.ssl.trustStore": "cdc.database.tls.truststore.location",
    "driver.javax.net.ssl.trustStorePassword": "cdc.database.tls.truststore.password",
    "driver.javax.net.ssl.trustStoreType": "cdc.database.tls.truststore.type",
}


def driver_property(ctx: Context, key: str, value: str) -> Outcome:
    if key in _TRUSTSTORE:
        ctx.set(_TRUSTSTORE[key], value.strip(), key)
        return Classification.MAPPED, ""
    if key == "driver.oracle.net.wallet_location":
        m = re.search(r"DIRECTORY\s*=\s*([^)]+)\)", value, re.IGNORECASE)
        directory = m.group(1).strip() if m else value.strip()
        if "(" in directory:
            raise Manual("Set `cdc.database.wallet.location` to the wallet directory by hand.")
        ctx.set("cdc.database.wallet.location", directory, key)
        return Classification.MAPPED_WITH_CHANGE, "The wallet directory."
    prop = key[len("driver.") :]
    if is_secret_key(prop):
        raise Manual(
            f"`{prop}` holds a secret. `cdc.database.connection.properties` is not a password"
            " field, so the value could appear in logs; use a wallet instead."
        )
    if ";" in value or "=" in value:
        raise Manual(
            f"The value of `{prop}` contains `;` or `=`, which"
            " `cdc.database.connection.properties` cannot carry; use `cdc.database.url`."
        )
    ctx.state.setdefault("driver", []).append((key, f"{prop}={value.strip()}"))
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Added to `cdc.database.connection.properties`.",
    )


def rac_nodes(ctx: Context, key: str, value: str) -> Outcome:
    raise Manual(
        "RAC (more than one redo thread) is not supported in this release; the per-thread"
        " position arrives in Phase 2. Do not migrate a RAC database yet."
    )


DROPPED_TUNING = (
    "Mining tuning of the Debezium connector; the connector sizes its window adaptively from"
    " `cdc.mining.target.latency.ms` and spills open transactions to disk."
)
DROPPED_RETRY = (
    "Transient database errors are retried for `cdc.retry.max.time.ms` (default one day)."
)
DROPPED_SNAPSHOT = "The takeover starts from the Debezium offset without a snapshot."


def converter_settings(source: dict[str, str], key: str) -> t.Rule | None:
    for name in (n.strip() for n in source.get("converters", "").split(",")):
        if name and key.startswith(name + "."):
            return t.manual(
                f"A setting of the custom converter `{name}`; custom converters are not supported."
            )
    return None


RULES = RuleSet(
    exact={
        "connector.class": connector_class,
        "name": name,
        "tasks.max": tasks_max,
        "database.hostname": t.to("cdc.database.host"),
        "database.port": t.to("cdc.database.port"),
        "database.user": t.to(
            "cdc.database.user",
            "In a CDB this must be a common user with the grants from"
            " `oracle-cdc-doctor setup-sql`.",
        ),
        "database.password": t.to("cdc.database.password"),
        "database.url": t.to("cdc.database.url"),
        "database.dbname": t.to(
            "cdc.database.service",
            "Debezium connects with this name as the service name; in a CDB it names the root.",
        ),
        "database.pdb.name": t.to("cdc.database.pdbs"),
        "database.connection.adapter": connection_adapter,
        "database.out.server.name": t.dropped(
            "XStream outbound server; the connector reads redo through LogMiner."
        ),
        "database.query.timeout.ms": t.dropped(
            "Each mining step has its own timeout, `cdc.mining.query.timeout.ms`."
        ),
        "database.tablename.case.insensitive": t.dropped("Debezium 1.x setting; not used."),
        "database.oracle.version": t.dropped("Debezium 1.x setting; not used."),
        "database.server.name": topic_prefix,
        "topic.prefix": topic_prefix,
        "topic.delimiter": topic_delimiter,
        "topic.naming.strategy": naming_strategy,
        "topic.heartbeat.prefix": t.dropped(
            "Heartbeat records go to `cdc.heartbeat.topic` (default `${prefix}.cdc.heartbeat`) in"
            " the connector's own format."
        ),
        "heartbeat.topics.prefix": t.dropped(
            "Heartbeat records go to `cdc.heartbeat.topic` in the connector's own format."
        ),
        "topic.transaction": t.dropped(
            "Transaction BEGIN and END records are not written in this release."
        ),
        "topic.cache.size": t.dropped("Internal cache of the Debezium connector."),
        "table.include.list": filter_list,
        "table.whitelist": filter_list,
        "table.exclude.list": filter_list,
        "table.blacklist": filter_list,
        "schema.include.list": filter_list,
        "schema.whitelist": filter_list,
        "schema.exclude.list": filter_list,
        "schema.blacklist": filter_list,
        "column.include.list": t.manual(COLUMN_INCLUDE),
        "column.whitelist": t.manual(COLUMN_INCLUDE),
        "column.exclude.list": t.manual(COLUMN_FILTER),
        "column.blacklist": t.manual(COLUMN_FILTER),
        "column.propagate.source.type": t.manual(
            "Source type parameters on field schemas are not supported."
        ),
        "datatype.propagate.source.type": t.manual(
            "Source type parameters on field schemas are not supported."
        ),
        "message.key.columns": message_key_columns,
        "snapshot.mode": snapshot_mode,
        "snapshot.max.threads": t.to("cdc.snapshot.threads"),
        "snapshot.fetch.size": t.to("cdc.snapshot.fetch.size"),
        "snapshot.locking.mode": t.dropped("Snapshots read AS OF an SCN and take no table locks."),
        "snapshot.lock.timeout.ms": t.dropped(
            "Snapshots read AS OF an SCN and take no table locks."
        ),
        "snapshot.include.collection.list": t.dropped(DROPPED_SNAPSHOT),
        "snapshot.delay.ms": t.dropped(DROPPED_SNAPSHOT),
        "streaming.delay.ms": t.dropped("Streaming starts as soon as the task starts."),
        "snapshot.tables.order.by.row.count": t.dropped(
            "Snapshot order is set with `cdc.snapshot.tables.order`."
        ),
        "snapshot.database.errors.max.retries": t.dropped(
            "A failed snapshot chunk is read again up to `cdc.snapshot.chunk.retries` times."
        ),
        "snapshot.max.threads.multiplier": t.dropped(DROPPED_SNAPSHOT),
        "snapshot.select.statement.overrides": t.manual(
            "The connector takes one row filter per table as a SQL condition, in"
            " `cdc.snapshot.select.override.PDB.OWNER.TABLE`, not a whole statement. Rewrite it"
            " by hand if a later snapshot needs it."
        ),
        "incremental.snapshot.chunk.size": t.changed(
            "cdc.snapshot.chunk.rows",
            lambda v: v,
            "Target rows per chunk for every snapshot.",
        ),
        "decimal.handling.mode": decimal_mode,
        "time.precision.mode": time_precision,
        "binary.handling.mode": binary_mode,
        "interval.handling.mode": interval_mode,
        "lob.enabled": lob_enabled,
        "unavailable.value.placeholder": t.to("cdc.unavailable.placeholder"),
        "tombstones.on.delete": t.to("cdc.tombstones.on.delete"),
        "heartbeat.interval.ms": heartbeat_interval,
        "heartbeat.action.query": t.dropped(
            "Not needed: heartbeats carry the position without writing to the source database,"
            " so offsets advance on a quiet database."
        ),
        "include.schema.changes": schema_changes,
        "include.schema.comments": t.dropped("Schema change events are not published."),
        "signal.data.collection": signal_table,
        "provide.transaction.metadata": transaction_metadata,
        "skipped.operations": skipped_operations,
        "skip.messages.without.change": flag_off_or_manual(
            "Updates that change no captured column are always published."
        ),
        "event.processing.failure.handling.mode": failure_mode,
        "max.batch.size": t.to("cdc.poll.max.records"),
        "max.queue.size": t.dropped("Records queue in batches of `cdc.poll.max.records`."),
        "max.queue.size.in.bytes": t.dropped("Records queue in batches of `cdc.poll.max.records`."),
        "poll.interval.ms": t.dropped(
            "`cdc.poll.linger.ms` sets how long a poll waits for the first record."
        ),
        "query.fetch.size": t.dropped(
            "Fetch sizes are `cdc.mining.fetch.size` and `cdc.snapshot.fetch.size`."
        ),
        "rac.nodes": rac_nodes,
        "log.mining.strategy": t.dropped(
            "The connector mines with the online catalog and replays a step with a dictionary"
            " from the redo when a row predates a later DDL."
        ),
        "log.mining.query.filter.mode": t.dropped(
            "Captured tables are always filtered by object id in the mining query."
        ),
        "log.mining.buffer.type": t.dropped(DROPPED_TUNING),
        "log.mining.transaction.retention.ms": retention,
        "log.mining.transaction.retention.hours": retention,
        "log.mining.archive.log.only.mode": archive_only,
        "log.mining.archive.destination.name": archive_destination,
        "archive.destination.name": archive_destination,
        "log.mining.archive.log.hours": t.dropped(
            "The connector reads the logs its position needs."
        ),
        "archive.log.hours": t.dropped("The connector reads the logs its position needs."),
        "log.mining.username.exclude.list": t.to("cdc.users.exclude"),
        "log.mining.username.include.list": t.manual(
            "There is no user include filter in this release; only `cdc.users.exclude`. List"
            " the users to leave out instead."
        ),
        "log.mining.clientid.include.list": t.manual("There is no client id filter."),
        "log.mining.clientid.exclude.list": t.manual("There is no client id filter."),
        "log.mining.session.max.ms": session_max,
        "log.mining.restart.connection": t.dropped(
            "The connector reconnects on its own after connection errors."
        ),
        "log.mining.flush.table.name": t.dropped(
            "The connector never writes to the source database; drop the flush table after"
            " the cutover."
        ),
        "log.mining.scn.gap.detection.gap.size.min": t.dropped(DROPPED_TUNING),
        "log.mining.scn.gap.detection.time.interval.max.ms": t.dropped(DROPPED_TUNING),
        "log.mining.window.max.ms": t.dropped(DROPPED_TUNING),
        "log.mining.log.query.max.retries": t.dropped(DROPPED_RETRY),
        "log.mining.log.backoff.initial.delay.ms": t.dropped(DROPPED_RETRY),
        "log.mining.log.backoff.max.delay.ms": t.dropped(DROPPED_RETRY),
        "log.mining.read.only": t.manual(
            "The connector never writes to the source database. To capture from a standby use"
            " `cdc.capture.mode=archive_only`."
        ),
        "errors.max.retries": t.dropped(DROPPED_RETRY),
        "retriable.restart.connector.wait.ms": t.dropped(DROPPED_RETRY),
        "custom.metric.tags": t.dropped(
            "Metric names differ; see the metrics reference for dashboards and alerts."
        ),
        "post.processors": t.manual(
            "Post processors are not supported. For LOB values the redo does not carry, use"
            " `cdc.lob.mode=reselect`."
        ),
        "converters": t.manual(
            "Custom converters are not supported; type mapping follows `cdc.decimal.mode` and"
            " `cdc.temporal.mode`."
        ),
        "sanitize.field.names": sanitize_field_names,
        "field.name.adjustment.mode": adjustment_mode(FIELD_ADJUSTMENT),
        "schema.name.adjustment.mode": adjustment_mode(SCHEMA_ADJUSTMENT),
        "legacy.decimal.handling.strategy": t.dropped("Debezium internal setting; not used."),
    },
    prefixes={
        "openlogreplicator.": t.dropped(
            "OpenLogReplicator setting; the connector reads redo through LogMiner."
        ),
        "snapshot.select.statement.overrides.": t.manual(
            "The connector takes one row filter per table as a SQL condition, in"
            " `cdc.snapshot.select.override.PDB.OWNER.TABLE`, not a whole statement. Rewrite it"
            " by hand if a later snapshot needs it."
        ),
        "incremental.snapshot.": t.dropped(
            "Snapshot chunking is set with `cdc.snapshot.chunk.rows`."
        ),
        "schema.history.internal": history,
        "database.history": history,
        "signal.": t.dropped(
            "Signals are read from `cdc.signals.topic` in the connector's own command format."
        ),
        "notification.": t.dropped("Progress is reported on the ops topic."),
        "post.processors.": t.manual(
            "Post processors are not supported. For LOB values the redo does not carry, use"
            " `cdc.lob.mode=reselect`."
        ),
        "column.mask.": t.manual("Column masking is not supported."),
        "column.truncate.to.": t.manual("Column truncation is not supported."),
        "log.mining.batch.size.": t.dropped(DROPPED_TUNING),
        "log.mining.sleep.time.": t.dropped(DROPPED_TUNING),
        "log.mining.buffer.": t.dropped(DROPPED_TUNING),
        "driver.": driver_property,
    },
    dynamic=converter_settings,
)


# Finalisers ------------------------------------------------------------------------------------


def finalise_topics(ctx: Context) -> None:
    strategy = ctx.get("topic.naming.strategy")
    if strategy and strategy != "io.debezium.schema.SchemaTopicNamingStrategy":
        return
    d = ctx.get("topic.delimiter", ".") or "."
    ctx.setting(
        "cdc.topic.template",
        "${prefix}" + d + "${schema}" + d + "${table}",
        "Keeps Debezium's topic names, `prefix.SCHEMA.TABLE`. The connector's own default adds"
        " the PDB name in a CDB.",
    )
    for key in ("topic.prefix", "database.server.name", "topic.delimiter"):
        ctx.attribute(key, "cdc.topic.template")


def _strip_pdb(pattern: str, pdb: str | None) -> str:
    if not pdb:
        return pattern
    rest = strip_first_segment(pattern, [pdb], wildcards=False)
    return rest if rest is not None else pattern


def finalise_tables(ctx: Context) -> None:
    pdb = ctx.get("database.pdb.name")
    prefix = pdb_prefix(pdb)
    sources: dict[str, list[str]] = {}
    for kind, keys in {
        "tables_in": ("table.include.list", "table.whitelist"),
        "tables_out": ("table.exclude.list", "table.blacklist"),
        "schemas_in": ("schema.include.list", "schema.whitelist"),
        "schemas_out": ("schema.exclude.list", "schema.blacklist"),
    }.items():
        patterns: list[str] = []
        for key in keys:
            value = ctx.get(key)
            if value is None or ctx.results[key].classification is Classification.MANUAL:
                continue
            patterns += [_strip_pdb(strip_anchors(p), pdb) for p in split_list(value, key)]
        sources[kind] = patterns

    include: list[str] = []
    if sources["tables_in"]:
        look = ""
        if sources["schemas_in"]:
            look = "(?=(?:" + "|".join(sources["schemas_in"]) + ")\\.)"
        include = [prefix + look + group(p) for p in sources["tables_in"]]
    elif sources["schemas_in"]:
        include = [prefix + group(s) + "\\..+" for s in sources["schemas_in"]]
    exclude = [prefix + group(p) for p in sources["tables_out"]]
    exclude += [prefix + group(s) + "\\..+" for s in sources["schemas_out"]]

    if include:
        ctx.set("cdc.tables.include", ",".join(include), "table and schema lists")
    if exclude:
        ctx.set("cdc.tables.exclude", ",".join(exclude), "table and schema lists")
    for key in (
        "table.include.list",
        "table.whitelist",
        "schema.include.list",
        "schema.whitelist",
    ):
        ctx.attribute(key, "cdc.tables.include")
    for key in ("table.exclude.list", "table.blacklist", "schema.exclude.list", "schema.blacklist"):
        ctx.attribute(key, "cdc.tables.exclude")
    if not include and not ctx.has("table.include.list") and not ctx.has("schema.include.list"):
        ctx.follow_up(
            "The Debezium connector captured every table outside the system schemas. The"
            " connector needs an explicit `cdc.tables.include`; list the schemas or tables to"
            " capture."
        )


def finalise_driver(ctx: Context) -> None:
    entries = ctx.state.get("driver", [])
    if entries:
        ctx.set(
            "cdc.database.connection.properties",
            ";".join(e for _, e in entries),
            "driver properties",
        )
        for key, _ in entries:
            ctx.attribute(key, "cdc.database.connection.properties")


def finalise_pins(ctx: Context) -> None:
    ctx.setting(
        "cdc.output.format",
        "debezium",
        "The Debezium-compatible envelope, so consumers keep working (MIG-2).",
    )
    ctx.setting("cdc.decimal.mode", "precise", "Debezium's default, pinned (MIG-3).")
    ctx.setting("cdc.temporal.mode", "adaptive", "Debezium's default, pinned (MIG-3).")
    ctx.setting("cdc.tombstones.on.delete", "true", "Debezium's default, pinned (MIG-3).")
    ctx.setting("cdc.lob.mode", "skip", "Debezium's default (`lob.enabled=false`), pinned (MIG-3).")
    if ctx.out.get("cdc.lob.mode") in ("inline", "reselect"):
        ctx.setting(
            "cdc.unavailable.placeholder",
            DEBEZIUM_PLACEHOLDER,
            "Debezium's placeholder for LOB values the redo does not carry, so consumers that"
            " test for it keep working (MIG-3).",
        )
    ctx.setting(
        "cdc.key.missing",
        "none",
        "Debezium published tables without a primary key with a null key; the connector would"
        " otherwise reject them at validation (MIG-3).",
    )
    if not ctx.has("include.schema.changes") or _bool(ctx.get("include.schema.changes", "")):
        ctx.follow_up(
            "Debezium published schema change events to the topic named after `topic.prefix`;"
            " this connector does not. Move or retire the consumers of that topic."
        )


def _debezium_1x(ctx: Context) -> bool:
    """Debezium 2.0 renamed `database.server.name` to `topic.prefix` and the `database.history`
    settings to `schema.history.internal`; a 1.x configuration needs both old names."""
    return (ctx.has("database.server.name") and not ctx.has("topic.prefix")) or any(
        k.startswith("database.history") for k in ctx.source
    )


def _avro_converter(ctx: Context) -> bool:
    return any(
        (ctx.get(k) or "").endswith("AvroConverter") for k in ("key.converter", "value.converter")
    )


def finalise_names(ctx: Context) -> None:
    """Debezium 1.x adjusted names for Avro by default; 2.0 made `none` the default."""
    if not _debezium_1x(ctx):
        return
    ctx.setting(
        SCHEMA_ADJUSTMENT,
        "avro",
        "Debezium 1.x adjusted schema names for Avro by default; pinned so the record names, and"
        " the schemas registered under the existing subjects, stay the same.",
    )
    if _avro_converter(ctx):
        ctx.setting(
            FIELD_ADJUSTMENT,
            "avro",
            "Debezium 1.x sanitised field names by default when the connector set an Avro"
            " converter (`sanitize.field.names`); pinned so the field names stay the same.",
        )


def next_steps(source_name: str | None, target_name: str, output: str | None) -> list[str]:
    old = source_name or "<debezium-connector>"
    out = output or "<translated-config>"
    return [
        f"Resolve the follow-ups, then run `oracle-cdc-doctor check --config {out}`.",
        f"Stop the Debezium connector with `PUT /connectors/{old}/stop`; pausing is not enough,"
        " the offset must be final.",
        f"Run `takeover_scn.py --target-config {out}` to read the Debezium offset, check the"
        " archived logs and write `cdc.start.scn` into the configuration.",
        f"Create `{target_name}` from that configuration. With no stored offset it starts"
        " streaming at `cdc.start.scn`.",
        "After the overlap has passed, run `verify_cutover.py` and archive the evidence file.",
        "Point dashboards and alerts at the connector's JMX metrics; the names differ from"
        " Debezium's (see the metrics reference).",
        "Keep the Debezium schema history topic until the cutover is verified, then delete it"
        " and the old connector's other internal topics after the agreed retention.",
    ]


SPEC = TranslatorSpec(
    prog="migrate_from_debezium.py",
    source_kind="Debezium Oracle connector",
    source_classes=(DEBEZIUM_CLASS,),
    other_tool="migrate_from_confluent.py",
    rules=RULES,
    finalisers=[
        t.finalise_identity,
        finalise_topics,
        finalise_tables,
        finalise_driver,
        finalise_pins,
        finalise_names,
        t.finalise_start,
        t.finalise_exactly_once,
        t.finalise_required,
    ],
    next_steps=next_steps,
    description=(
        "Translate a Debezium Oracle connector configuration into a configuration for the OSO"
        " CDC Connector for Oracle Database that takes over at the Debezium position"
        " (cdc.snapshot.mode=none, and cdc.start.scn from takeover_scn.py), with a report"
        " that classifies every source property"
        " as mapped, mapped with a change, dropped (with the reason) or manual (with an"
        " instruction). Secrets are never written to the report or the output: literal values"
        " are masked, config provider references are kept."
    ),
)


def main(argv: list[str] | None = None) -> int:
    return translator_main(SPEC, argv)


if __name__ == "__main__":
    sys.exit(main())
