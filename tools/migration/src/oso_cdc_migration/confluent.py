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
"""``migrate_from_confluent``: Confluent Oracle CDC Source connector configuration to ``cdc.*``
(PRD-04 MIG-1 to MIG-7, property list from Confluent's public configuration reference).

The Confluent-compatible record format (``cdc.output.format=confluent``) and LOB topics are not
built yet, so a translated configuration publishes the Debezium-compatible envelope and the report
says so as its first follow-up.
"""

from __future__ import annotations

import re
import sys

from . import OSO_CONNECTOR_CLASS
from . import translate as t
from .tables import strip_anchors, strip_first_segment, top_level_alternatives
from .translate import Classification, Context, Manual, Outcome, RuleSet
from .translator_cli import TranslatorSpec, translator_main

CONFLUENT_CLASS = "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector"
DEFAULT_TEMPLATE = "${fullyQualifiedTableName}"

RECORD_FORMAT = (
    "This release publishes the Debezium-compatible envelope only (`before`, `after`, `source`,"
    " `op`); the Confluent-compatible flat format (`cdc.output.format=confluent`) is not built"
    " yet. Records change shape and keys become structs of the key columns, so consumers and"
    " schemas of the existing table topics break. Point `cdc.topic.template` at new topic names"
    " and move consumers to the new format, or wait for the Confluent-compatible format before"
    " cutting over."
)
FORMAT_PROPERTY = (
    "Part of Confluent's record format, which this release does not produce. See the record"
    " format follow-up."
)
TAKEOVER_SNAPSHOT_NOTE = (
    "The takeover starts the new connector at the Confluent position (`cdc.start.scn` from"
    " takeover_scn.py), so no snapshot runs: `cdc.snapshot.mode=none`."
)


def _bool(value: str) -> bool:
    return value.strip().lower() == "true"


def _int(key: str, value: str) -> int:
    try:
        return int(value.strip())
    except ValueError:
        raise Manual(f"`{key}` is not a whole number; set the target by hand.") from None


def _non_empty(target: str, empty_reason: str, note: str = "") -> t.Rule:
    def rule(ctx: Context, key: str, value: str) -> Outcome:
        if not value.strip():
            return Classification.DROPPED, empty_reason
        ctx.set(target, value.strip(), key)
        return Classification.MAPPED, note

    return rule


def connector_class(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("connector.class", OSO_CONNECTOR_CLASS, key)
    return Classification.MAPPED_WITH_CHANGE, "Replaced by the OSO CDC Connector class."


def name(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("name", ctx.target_name, key)
    return (
        Classification.MAPPED_WITH_CHANGE,
        "The new connector has its own name: Kafka Connect keeps offsets per connector name, and"
        " this connector does not read the Confluent offset format.",
    )


def tasks_max(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("tasks.max", "1", key)
    if value.strip() == "1":
        return Classification.MAPPED, ""
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Reduced to one (MIG-5). Confluent spread table topics over tasks that all read its redo"
        " log topic; this connector mines every table in one task with one LogMiner session, so"
        " more tasks would add nothing. A higher value is accepted and ignored.",
    )


def table_regex(ctx: Context, key: str, value: str) -> Outcome:
    patterns = rewrite_regex(ctx, key, value)
    target = "cdc.tables.include" if key == "table.inclusion.regex" else "cdc.tables.exclude"
    ctx.set(target, ",".join(patterns), key)
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Rewritten as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB);"
        " matching stays case-sensitive (`cdc.tables.case.sensitive=true`).",
    )


def rewrite_regex(ctx: Context, key: str, value: str) -> list[str]:
    regex = strip_anchors(value.strip())
    if "," in regex:
        raise Manual(
            f"`{key}` contains a comma. The connector splits its pattern lists at every comma;"
            " rewrite the expression without one (for example `{1,3}` as an alternation)."
        )
    pdb = ctx.get("oracle.pdb.name")
    if pdb:
        return [regex]  # Confluent matches PDB.SCHEMA.TABLE as the connector does
    names = [n for n in (ctx.get("oracle.sid"), ctx.get("oracle.service.name")) if n]
    out = []
    for alternative in top_level_alternatives(regex):
        alternative = strip_anchors(alternative.strip())
        if alternative.startswith("(") and alternative.endswith(")") and "|" not in alternative:
            alternative = alternative[1:-1]
        rest = strip_first_segment(alternative, names, wildcards=True)
        if rest is None:
            raise Manual(
                f"Could not remove the database name from `{alternative}`. Without a PDB the"
                " connector matches `SCHEMA.TABLE`; rewrite the expression by hand."
            )
        out.append(rest)
    return out


def topic_template(ctx: Context, key: str, value: str) -> Outcome:
    converted = convert_template(ctx, value)
    ctx.set("cdc.topic.template", converted, key)
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Template variables translated; check the result against the existing topic names.",
    )


_VARIABLE = re.compile(r"\$\{([^}]*)\}")


def convert_template(ctx: Context, template: str) -> str:
    if not template.strip():
        raise Manual(
            "A blank template wrote only the redo log topic, which this connector does not"
            " have. Choose table topic names with `cdc.topic.template`."
        )
    if "\\" in template:
        raise Manual("The template uses escapes, which `cdc.topic.template` does not support.")
    pdb = ctx.get("oracle.pdb.name")
    database = "${pdb}" if pdb else "${database}"
    mapping = {
        "schemaName": "${schema}",
        "tableName": "${table}",
        "databaseName": database,
        "fullyQualifiedTableName": database + ".${schema}.${table}",
        "emptyString": "",
    }
    if ctx.source_name:
        mapping["connectorName"] = ctx.source_name
    unknown = sorted({m for m in _VARIABLE.findall(template) if m not in mapping})
    if unknown:
        raise Manual(
            "The template uses variables with no equivalent: "
            + ", ".join(f"`${{{u}}}`" for u in unknown)
            + ". `cdc.topic.template` knows `${prefix}`, `${pdb}`, `${schema}`, `${table}` and"
            " `${database}`."
        )
    if ctx.once("topic-names"):
        ctx.follow_up(
            "Confluent's documentation describes `${databaseName}` and"
            " `${fullyQualifiedTableName}` loosely. Compare the topic names the translated"
            " `cdc.topic.template` produces with the existing topics before cutting over."
        )
    return _VARIABLE.sub(lambda m: mapping[m.group(1)], template)


def start_from(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("cdc.snapshot.mode", "none", key)
    return Classification.MAPPED_WITH_CHANGE, TAKEOVER_SNAPSHOT_NOTE


def transaction_age(ctx: Context, key: str, value: str) -> Outcome:
    action = (ctx.get("log.mining.transaction.threshold.breached.action") or "warn").lower()
    threshold = _int(key, ctx.get("log.mining.transaction.age.threshold.ms") or "-1")
    if threshold <= 0:
        return Classification.DROPPED, "No age limit, which is also the connector's default."
    if action == "discard":
        if key == "log.mining.transaction.age.threshold.ms":
            ctx.set("cdc.transaction.max.age.ms", str(max(threshold, 1000)), key)
        else:
            ctx.set("cdc.transaction.max.age.action", "discard", key)
        return (
            Classification.MAPPED,
            "A discarded transaction's details go to the ops topic and the DLQ.",
        )
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Not copied: the connector has no warn-only age limit. Long transactions are kept,"
        " spilled to disk and journaled, and their age shows in the metrics; set"
        " `cdc.transaction.max.age.ms` to stop or discard instead.",
    )


def decode_behaviour(ctx: Context, key: str, value: str) -> Outcome:
    keys = ("behavior.on.dictionary.mismatch", "behavior.on.unparsable.statement")
    modes = {(ctx.get(k) or "fail").lower() for k in keys}
    unknown = modes - {"fail", "log"}
    if unknown:
        raise Manual(f"Unknown value `{sorted(unknown)[0]}`; choose `cdc.on.decode.error`.")
    target = "dlq" if "log" in modes else "fail"
    ctx.set("cdc.on.decode.error", target, key)
    if value.strip().lower() == "log":
        ctx.follow_up(
            "The source logged and skipped rows it could not parse. The connector never skips"
            " silently: such rows go to the DLQ topic (`cdc.dlq.topic`) with an ops event."
            " Make sure someone watches the DLQ.",
            key,
        )
        return Classification.MAPPED_WITH_CHANGE, "Undecodable rows go to the DLQ topic."
    if target == "dlq":
        return (
            Classification.MAPPED_WITH_CHANGE,
            "One decode setting for both cases; the other property asked to log and skip.",
        )
    return Classification.MAPPED, ""


def lob_topic(ctx: Context, key: str, value: str) -> Outcome:
    if not value.strip():
        return (
            Classification.DROPPED,
            "LOB columns were not captured; `cdc.lob.mode=skip` keeps them out of the records.",
        )
    raise Manual(
        "LOB topics are not built yet (MIG-5). Until they are, `cdc.lob.mode=inline` publishes"
        " LOB values inside the row records; consumers of the LOB topics must change."
    )


def numeric_mapping(ctx: Context, key: str, value: str) -> Outcome:
    mode = value.strip().lower()
    if mode == "none":
        ctx.set("cdc.decimal.mode", "precise", key)
        return (
            Classification.MAPPED_WITH_CHANGE,
            "Kept exact (MIG-3). Confluent published every NUMBER as a Connect Decimal (bytes);"
            " `precise` publishes NUMBER(p,0) up to 18 digits as integers, other constrained"
            " NUMBER as Decimal and unconstrained NUMBER as a variable scale decimal struct.",
        )
    if mode in ("best_fit", "best_fit_or_decimal"):
        ctx.set("cdc.decimal.mode", "precise", key)
        return (
            Classification.MAPPED_WITH_CHANGE,
            "`precise` also uses integers for NUMBER(p,0) and Decimal for other constrained"
            " NUMBER; unconstrained NUMBER is a variable scale decimal struct.",
        )
    raise Manual(
        f"`numeric.mapping={mode}` has no exact equivalent. `cdc.decimal.mode` offers `precise`,"
        " `string` (every NUMBER as text) and `double` (every NUMBER as a double); choose one."
    )


def default_scale(ctx: Context, key: str, value: str) -> Outcome:
    if value.strip() == "127":
        return (
            Classification.DROPPED,
            "Unconstrained NUMBER values are published as a variable scale decimal, with their"
            " own scale.",
        )
    raise Manual(
        "There is no default scale: unconstrained NUMBER values are published as a variable"
        " scale decimal with their own scale."
    )


def timezone(ctx: Context, key: str, value: str) -> Outcome:
    if value.strip().upper() in ("UTC", "Z", "+00:00", "GMT", "ETC/UTC"):
        return Classification.DROPPED, "DATE and TIMESTAMP values are read as UTC."
    raise Manual(
        "DATE and TIMESTAMP values are read as UTC in this release; there is no time zone"
        " setting. Consumers that expect another zone must convert."
    )


def fan_events(ctx: Context, key: str, value: str) -> Outcome:
    if not _bool(value):
        return Classification.DROPPED, "Off in the source as well."
    raise Manual(
        "RAC is not supported in this release, so there are no FAN events to follow. Do not"
        " migrate a RAC database yet."
    )


def kerberos(ctx: Context, key: str, value: str) -> Outcome:
    if not value.strip():
        return Classification.DROPPED, "Not used in the source."
    raise Manual("Kerberos authentication is not built yet; use a password or a wallet.")


def ldap_url(ctx: Context, key: str, value: str) -> Outcome:
    ctx.set("cdc.database.url", "jdbc:oracle:thin:@" + value.strip(), key)
    ctx.follow_up(
        "The LDAP naming URL became `cdc.database.url`. Test the connection with"
        " `oracle-cdc-doctor check`; LDAP authentication settings are not translated.",
        key,
    )
    return Classification.MAPPED_WITH_CHANGE, "Connect through LDAP naming in the JDBC URL."


def retry_codes(ctx: Context, key: str, value: str) -> Outcome:
    codes = []
    for raw in value.split(","):
        code = raw.strip().upper().removeprefix("ORA-")
        if not code:
            continue
        if not code.isdigit():
            raise Manual(f"`{raw.strip()}` is not an ORA error number.")
        codes.append(f"ORA-{int(code):05d}")
    ctx.set("cdc.retry.extra.error.codes", ",".join(codes), key)
    return Classification.MAPPED_WITH_CHANGE, "Written as ORA-nnnnn codes."


def licence(ctx: Context, key: str, value: str) -> Outcome:
    ctx.follow_up(
        "`confluent.license` is not needed by this connector (MIG-5). Remove the licence from"
        " the configuration and the secret store once no other Confluent premium connector uses"
        " it.",
        key,
    )
    return Classification.DROPPED, "Confluent licence; not used."


def redo_topic(ctx: Context, key: str, value: str) -> Outcome:
    return Classification.DROPPED, "There is no redo log topic; see the follow-up."


def heartbeat_interval(ctx: Context, key: str, value: str) -> Outcome:
    if _int(key, value) > 0:
        ctx.set("cdc.heartbeat.interval.ms", value.strip(), key)
        return Classification.MAPPED, ""
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Not copied: the connector's heartbeats (default every 10 seconds) keep offsets moving on"
        " a quiet database and never write to the source database.",
    )


def users_include(ctx: Context, key: str, value: str) -> Outcome:
    if not value.strip():
        return Classification.DROPPED, "Empty in the source."
    raise Manual(
        "There is no user include filter in this release; only `cdc.users.exclude`. List the"
        " users to leave out instead."
    )


NO_REDO_TOPIC = "There is no redo log topic, so its consumer settings do not apply."

RULES = RuleSet(
    exact={
        "connector.class": connector_class,
        "name": name,
        "tasks.max": tasks_max,
        "oracle.server": t.to("cdc.database.host"),
        "oracle.port": t.to("cdc.database.port"),
        "oracle.username": t.to(
            "cdc.database.user",
            "In a CDB this must be a common user with the grants from"
            " `oracle-cdc-doctor setup-sql`.",
        ),
        "oracle.password": t.to("cdc.database.password"),
        "oracle.sid": _non_empty("cdc.database.sid", "Empty in the source."),
        "oracle.service.name": _non_empty("cdc.database.service", "Empty in the source."),
        "oracle.pdb.name": t.to("cdc.database.pdbs"),
        "table.inclusion.regex": table_regex,
        "table.exclusion.regex": table_regex,
        "table.topic.name.template": topic_template,
        "redo.log.topic.name": redo_topic,
        "start.from": start_from,
        "key.template": t.manual(
            "Record keys are structs of the key columns (Debezium style). Confluent's key"
            " template (default `${primaryKeyStructOrValue}`, a plain value for a single-column"
            " key) has no equivalent; consumers and compacted topics keyed by the old form must"
            " change."
        ),
        "max.batch.size": t.to("cdc.poll.max.records"),
        "query.timeout.ms": t.changed(
            "cdc.mining.query.timeout.ms",
            lambda v: v,
            "Applies to each mining step; on expiry the step is retried with a smaller window.",
        ),
        "max.retry.time.ms": t.to("cdc.retry.max.time.ms"),
        "redo.log.poll.interval.ms": t.dropped(
            "The mining window adapts to `cdc.mining.target.latency.ms` (default two seconds)."
        ),
        "snapshot.row.fetch.size": t.to("cdc.snapshot.fetch.size"),
        "redo.log.row.fetch.size": t.to("cdc.mining.fetch.size"),
        "poll.linger.ms": t.to("cdc.poll.linger.ms"),
        "snapshot.threads.per.task": t.to("cdc.snapshot.threads"),
        "heartbeat.interval.ms": heartbeat_interval,
        "heartbeat.topic.name": t.dropped(
            "Heartbeat records go to `cdc.heartbeat.topic` (default `${prefix}.cdc.heartbeat`) in"
            " the connector's own format."
        ),
        "use.transaction.begin.for.mining.session": t.dropped(
            "Always on: the connector restarts from the oldest open transaction."
        ),
        "log.mining.transaction.age.threshold.ms": transaction_age,
        "log.mining.transaction.threshold.breached.action": transaction_age,
        "redo.log.corruption.topic": t.dropped(
            "Corrupt redo stops the task with a typed error and an ops event; it is never skipped."
        ),
        "behavior.on.dictionary.mismatch": decode_behaviour,
        "behavior.on.unparsable.statement": decode_behaviour,
        "oracle.dictionary.mode": t.dropped(
            "The connector mines with the online catalog and replays a step with a dictionary"
            " from the redo when a row predates a later DDL (`cdc.dictionary.build.*`)."
        ),
        "log.mining.archive.destination.name": _non_empty(
            "cdc.archive.destination", "Empty: the lowest valid local destination is used."
        ),
        "record.buffer.mode": t.dropped("The connector buffers in its own transaction buffer."),
        "max.batch.timeout.ms": t.dropped("Deprecated in the source; not used."),
        "max.buffer.size": t.dropped("The connector buffers in its own transaction buffer."),
        "lob.topic.name.template": lob_topic,
        "enable.large.lob.object.support": t.dropped(
            "`cdc.lob.max.bytes` limits LOB values (default 1 MiB)."
        ),
        "log.sensitive.data": t.to("cdc.log.sensitive.data"),
        "numeric.mapping": numeric_mapping,
        "numeric.default.scale": default_scale,
        "oracle.date.mapping": t.manual(
            "DATE columns are published as `io.debezium.time.Timestamp` (milliseconds) with the"
            " time of day kept, or as ISO text with `cdc.temporal.mode=iso_string`. Consumers"
            " that read Confluent's DATE mapping must change."
        ),
        "emit.tombstone.on.delete": t.to("cdc.tombstones.on.delete"),
        "oracle.fan.events.enable": fan_events,
        "table.task.reconfig.checking.interval.ms": t.dropped(
            "One task captures every table, so there is no table placement to rebalance."
        ),
        "table.rps.logging.interval.ms": t.dropped("Per-table rates are JMX metrics."),
        "log.mining.end.scn.deviation.ms": t.dropped(
            "A RAC setting; RAC is not supported in this release."
        ),
        "redo.log.startup.polling.limit.ms": t.dropped(NO_REDO_TOPIC),
        "snapshot.by.table.partitions": t.dropped(
            "Snapshots are split into chunks of `cdc.snapshot.chunk.rows` rows."
        ),
        "oracle.validation.result.fetch.size": t.dropped("Validation is the doctor's fast mode."),
        "redo.log.row.poll.fields.include": t.dropped(NO_REDO_TOPIC),
        "redo.log.row.poll.fields.exclude": t.dropped(NO_REDO_TOPIC),
        "redo.log.row.poll.username.include": users_include,
        "redo.log.row.poll.username.exclude": _non_empty("cdc.users.exclude", "Empty."),
        "db.timezone": timezone,
        "db.timezone.date": timezone,
        "oracle.supplemental.log.level": t.dropped(
            "`oracle-cdc-doctor check` verifies supplemental logging per captured table."
        ),
        "ldap.url": ldap_url,
        "ldap.security.principal": t.manual(
            "LDAP authentication is not translated; use the database user and password, or a"
            " wallet."
        ),
        "ldap.security.credentials": t.manual(
            "LDAP authentication is not translated; use the database user and password, or a"
            " wallet."
        ),
        "oracle.ssl.truststore.file": _non_empty(
            "cdc.database.tls.truststore.location", "Empty in the source."
        ),
        "oracle.ssl.truststore.password": _non_empty(
            "cdc.database.tls.truststore.password", "Empty in the source."
        ),
        "oracle.kerberos.cache.file": kerberos,
        "retry.error.codes": retry_codes,
        "enable.metrics.collection": t.dropped("JMX metrics are always on."),
        "confluent.license": licence,
        "redo.log.initial.delay.interval.ms": t.dropped("Streaming starts with the task."),
    },
    prefixes={
        "redo.log.consumer.": t.dropped(NO_REDO_TOPIC),
        "output.": t.manual(FORMAT_PROPERTY),
        "confluent.topic": t.dropped("Confluent licence topic settings; not used."),
        "connection.pool.": t.dropped(
            "The connector manages its own connections (MIG-5); driver options go in"
            " `cdc.database.connection.properties`."
        ),
    },
    keep_empty=frozenset({"table.topic.name.template", "lob.topic.name.template"}),
)


# Finalisers ------------------------------------------------------------------------------------


def finalise_format(ctx: Context) -> None:
    ctx.follow_up(RECORD_FORMAT, first=True)
    ctx.setting(
        "cdc.output.format",
        "debezium",
        "The only format in this release; Confluent's flat format is not built yet.",
    )


def finalise_topics(ctx: Context) -> None:
    prefix = ctx.state.get("topic_prefix") or re.sub(
        r"[^A-Za-z0-9._-]", "_", ctx.source_name or "oracle-cdc"
    )
    ctx.setting(
        "cdc.topic.prefix",
        prefix,
        "Names the connector's internal topics and its offset partition; Confluent table topic"
        " names do not use it.",
    )
    if not ctx.has("table.topic.name.template"):
        try:
            template = convert_template(ctx, DEFAULT_TEMPLATE)
        except Manual as m:
            ctx.follow_up(m.instruction)
        else:
            ctx.setting(
                "cdc.topic.template",
                template,
                "Confluent's default table topic template, `${fullyQualifiedTableName}`,"
                " translated.",
            )
    ctx.follow_up(
        "The redo log topic (`redo.log.topic.name`, default"
        " `${connectorName}-${databaseName}-redo-log`) stops receiving data at the cutover"
        " (MIG-5). Move or retire its consumers, then delete it after the agreed retention."
    )


def finalise_tables(ctx: Context) -> None:
    if "cdc.tables.include" in ctx.out or "cdc.tables.exclude" in ctx.out:
        ctx.setting(
            "cdc.tables.case.sensitive",
            "true",
            "Confluent matches table expressions case-sensitively (MIG-3).",
        )
    if ctx.has("oracle.pdb.name") is False and ctx.has("table.inclusion.regex"):
        ctx.follow_up(
            "Without `oracle.pdb.name` the translator assumed a non-CDB database and removed the"
            " database name from the table expressions. If the tables live in the root container"
            " of a CDB, rewrite `cdc.tables.include` by hand."
        )


def finalise_pins(ctx: Context) -> None:
    ctx.setting(
        "cdc.tombstones.on.delete",
        "false",
        "Confluent's default (`emit.tombstone.on.delete=false`), pinned (MIG-3).",
    )
    ctx.setting(
        "cdc.decimal.mode",
        "precise",
        "Confluent's default `numeric.mapping=none` kept numbers exact; `precise` does too"
        " (MIG-3). NUMBER(p,0) up to 18 digits become integers rather than Decimal bytes.",
    )
    ctx.setting("cdc.lob.mode", "skip", "LOB columns stay out of the records, as before.")


def _prefix_option(p) -> None:
    p.add_argument(
        "--topic-prefix",
        help="cdc.topic.prefix for the new connector (default: the source connector's name)",
    )


def next_steps(source_name: str | None, target_name: str, output: str | None) -> list[str]:
    old = source_name or "<confluent-connector>"
    out = output or "<translated-config>"
    return [
        "Resolve the follow-ups, above all the record format, then run"
        f" `oracle-cdc-doctor check --config {out}`.",
        f"Stop the Confluent connector with `PUT /connectors/{old}/stop`; pausing is not enough,"
        " the offset must be final.",
        f"Run `takeover_scn.py --target-config {out}` to read the Confluent offset, check the"
        " archived logs and write `cdc.start.scn` into the configuration.",
        f"Create `{target_name}` from that configuration. With no stored offset it starts"
        " streaming at `cdc.start.scn`.",
        "Point dashboards and alerts at the connector's JMX metrics; the names differ from"
        " Confluent's (see the metrics reference).",
        "Remove the redo log topic and the old connector's other topics after the agreed"
        " retention.",
    ]


SPEC = TranslatorSpec(
    prog="migrate_from_confluent.py",
    source_kind="Confluent Oracle CDC Source connector",
    source_classes=(CONFLUENT_CLASS,),
    other_tool="migrate_from_debezium.py",
    rules=RULES,
    finalisers=[
        t.finalise_identity,
        finalise_format,
        finalise_topics,
        finalise_tables,
        finalise_pins,
        t.finalise_start,
        t.finalise_exactly_once,
        t.finalise_required,
    ],
    next_steps=next_steps,
    extra_options=_prefix_option,
    prepare=lambda args: {"topic_prefix": args.topic_prefix},
    description=(
        "Translate a Confluent Oracle CDC Source connector configuration into a configuration"
        " for the OSO CDC Connector for Oracle Database, with a report that classifies every"
        " source property as mapped, mapped with a change, dropped (with the reason) or manual"
        " (with an instruction). The Confluent-compatible record format is not built yet, so"
        " the translated connector publishes the Debezium-compatible envelope; the report's"
        " first follow-up explains what that means for existing consumers."
    ),
)


def main(argv: list[str] | None = None) -> int:
    return translator_main(SPEC, argv)


if __name__ == "__main__":
    sys.exit(main())
