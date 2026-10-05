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
"""``verify_cutover``: compare the database AS OF a check SCN with the state materialised from
the topics, table by table, and write evidence with a SHA-256 over its canonical JSON
(PRD-04 s5, VER-1 to VER-5)."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from . import __version__, configio
from .common import (
    EXIT_FAILURE,
    EXIT_OK,
    ToolError,
    add_common_options,
    configure_logging,
    emit,
    parser,
    read_secret,
    run,
    write_text,
)
from .connect import ConnectApi, ConnectClient, ConnectError
from .kafka import KafkaTopicSource, TopicSource
from .materialise import Materialiser, TableSpec, TableStats
from .normalise import Column, NormaliseError, Normaliser
from .oracle import OracleVerifyDatabase, SnapshotTooOld, connect
from .records import RecordError, parse
from .redact import MASK, Redactor, is_secret_key
from .rowstore import Comparison, RowStore, canonical

LOG = logging.getLogger(__name__)
PROG = "verify_cutover.py"
OSO_PLACEHOLDER = "__cdc_unavailable_value"
DEBEZIUM_PLACEHOLDER = "__debezium_unavailable_value"
PASS, FAIL, INCONCLUSIVE = "PASS", "FAIL", "INCONCLUSIVE"


class VerifyDatabase(Protocol):
    def current_scn(self) -> int: ...

    def scn_seconds_ago(self, seconds: int) -> int: ...

    def database_name(self) -> str: ...

    def container_name(self) -> str: ...

    def columns(self, owner: str, table: str) -> list[Column]: ...

    def key_columns(self, owner: str, table: str) -> list[str]: ...

    def rows(
        self, owner: str, table: str, columns: list[Column], key: list[str], scn: int, batch: int
    ) -> Iterator[list[dict[str, Any]]]: ...


# What the connector configuration says ---------------------------------------------------------


@dataclass
class TableRequest:
    owner: str
    table: str
    topic: str | None = None


def _identifier(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1]
    return text.upper()


def parse_table(arg: str) -> TableRequest:
    name, _, topic = arg.partition("=")
    owner, dot, table = name.partition(".")
    if not dot or not owner or not table:
        raise ToolError(f"--table {arg}: give OWNER.TABLE or OWNER.TABLE=TOPIC.")
    return TableRequest(_identifier(owner), _identifier(table), topic.strip() or None)


def sanitise_topic(topic: str) -> str:
    """As the connector's TopicRouter: characters Kafka rejects become underscores."""
    s = re.sub(r"[^A-Za-z0-9._-]", "_", topic)
    s = re.sub(r"\.\.+", ".", s).lstrip(".")
    return s[:249]


@dataclass
class Target:
    kind: str = "none"  # oso, debezium or none
    name: str | None = None
    prefix: str | None = None
    template: str | None = None
    decimal_mode: str = "precise"
    temporal_mode: str = "adaptive"
    placeholder: str = OSO_PLACEHOLDER
    lob_skip: bool = True
    cdb: bool = False
    key_overrides: dict[str, list[str]] = field(default_factory=dict)

    def topic(self, owner: str, table: str, pdb: str | None, database: str | None) -> str | None:
        if not self.template or self.prefix is None:
            return None
        values = {
            "${prefix}": self.prefix,
            "${pdb}": pdb or "",
            "${schema}": owner,
            "${table}": table,
            "${database}": database or "",
        }
        out = self.template
        for k, v in values.items():
            out = out.replace(k, v)
        return sanitise_topic(out)


def _key_overrides(raw: str | None) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for entry in (raw or "").split(";"):
        table, sep, cols = entry.partition(":")
        if sep and table.strip():
            parts = table.strip().upper().split(".")
            out[".".join(parts[-2:])] = [c.strip().upper() for c in cols.split(",") if c.strip()]
    return out


def target_from_config(cfg: configio.ConnectorConfig | None) -> Target:
    if cfg is None:
        return Target(template="${prefix}.${schema}.${table}")
    c = cfg.config
    cls = c.get("connector.class", "")
    if cls.startswith("io.debezium") or "topic.prefix" in c or "database.server.name" in c:
        d = c.get("topic.delimiter") or "."
        mode = (c.get("time.precision.mode") or "adaptive").lower()
        temporal = {"adaptive": "adaptive", "adaptive_time_microseconds": "adaptive"}.get(mode)
        if mode == "isostring":
            temporal = "iso_string"
        if temporal is None:
            raise ToolError(f"time.precision.mode={mode} is not supported by the verifier.")
        return Target(
            kind="debezium",
            name=cfg.name,
            prefix=c.get("topic.prefix") or c.get("database.server.name"),
            template="${prefix}" + d + "${schema}" + d + "${table}",
            decimal_mode=(c.get("decimal.handling.mode") or "precise").lower(),
            temporal_mode=temporal,
            placeholder=c.get("unavailable.value.placeholder") or DEBEZIUM_PLACEHOLDER,
            lob_skip=(c.get("lob.enabled") or "false").lower() != "true",
            key_overrides=_key_overrides(c.get("message.key.columns")),
        )
    if cls.startswith("io.confluent"):
        raise ToolError(
            "Topics of the Confluent Oracle CDC Source connector use Confluent's flat record"
            " format, which the verifier does not read."
        )
    cdb = bool((c.get("cdc.database.pdbs") or "").strip())
    template = c.get("cdc.topic.template") or (
        "${prefix}.${pdb}.${schema}.${table}" if cdb else "${prefix}.${schema}.${table}"
    )
    return Target(
        kind="oso",
        name=cfg.name,
        prefix=c.get("cdc.topic.prefix"),
        template=template,
        decimal_mode=(c.get("cdc.decimal.mode") or "precise").lower(),
        temporal_mode=(c.get("cdc.temporal.mode") or "adaptive").lower(),
        placeholder=c.get("cdc.unavailable.placeholder") or OSO_PLACEHOLDER,
        lob_skip=(c.get("cdc.lob.mode") or "skip").lower() == "skip",
        cdb=cdb,
        key_overrides=_key_overrides(c.get("cdc.key.columns")),
    )


# The check SCN and the connector position (VER-1) ----------------------------------------------


def connector_position(api: ConnectApi, connector: str) -> int | None:
    """The lowest resume SCN over the connector's offsets: ``resume_scn`` (this connector) or
    ``scn`` (Debezium, Confluent). Every commit below it has been delivered and acknowledged."""
    values = []
    for e in api.offsets(connector).get("offsets", []):
        off = e.get("offset") or {}
        for k in ("resume_scn", "scn"):
            raw = off.get(k)
            if raw not in (None, "") and str(raw).strip().isdigit():
                values.append(int(str(raw).strip()))
                break
    return min(values) if values else None


def confirm_position(
    api: ConnectApi,
    connector: str,
    check_scn: int,
    wait_s: int,
    sleep: Callable[[float], None] = time.sleep,
    poll_s: float = 5.0,
) -> dict[str, Any]:
    waited = 0.0
    while True:
        position = connector_position(api, connector)
        if position is not None and position > check_scn:
            return {"connector": connector, "position_scn": position, "passed": True}
        if waited >= wait_s:
            raise ToolError(
                f"The position of `{connector}` ({position}) has not passed the check SCN"
                f" {check_scn} after {int(waited)} seconds. Wait for the connector to catch up,"
                " or give an earlier --check-scn."
            )
        sleep(poll_s)
        waited += poll_s


# Verification ---------------------------------------------------------------------------------


@dataclass
class TableResult:
    table: str
    topic: str
    status: str
    reason: str
    key_columns: list[str]
    compared_columns: list[str]
    ignored_columns: dict[str, str]
    comparison: Comparison | None
    stats: TableStats
    samples: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Settings:
    check_scn: int
    tables: list[TableRequest]
    target: Target
    batch_rows: int = 50_000
    samples: int = 100
    show_values: bool = False
    ignore_columns: set[str] = field(default_factory=set)
    pdb: str | None = None


def _spec(db: VerifyDatabase, req: TableRequest, s: Settings, database: str, pdb: str | None):
    """The columns to compare, the record key and the batching key of one table."""
    problems: list[str] = []
    columns = db.columns(req.owner, req.table)
    if not columns:
        problems.append("the table does not exist or the user cannot read it")
    batch_key = db.key_columns(req.owner, req.table) if columns else []
    key = s.target.key_overrides.get(f"{req.owner}.{req.table}") or batch_key
    compared: list[Column] = []
    ignored: dict[str, str] = {}
    for c in columns:
        if f"{req.owner}.{req.table}.{c.name}" in s.ignore_columns:
            ignored[c.name] = "--ignore-column"
        elif c.kind == "unsupported":
            ignored[c.name] = f"type {c.data_type} is not compared"
        elif c.kind == "xml":
            ignored[c.name] = "XMLTYPE values are not published with their content"
        elif c.is_lob and s.target.lob_skip:
            ignored[c.name] = "LOB columns are left out of the records"
        else:
            compared.append(c)
    names = {c.name for c in compared}
    for k in key:
        if k not in names:
            problems.append(f"key column {k} is not compared")
    topic = req.topic or s.target.topic(req.owner, req.table, pdb, database)
    if not topic:
        raise ToolError(
            f"No topic for {req.owner}.{req.table}: give --table {req.owner}.{req.table}=TOPIC,"
            " or a connector configuration with --input."
        )
    spec = TableSpec(req.owner, req.table, topic, compared, key, columns, ignored)
    return spec, batch_key, problems


def _scan_database(
    db: VerifyDatabase,
    store: RowStore,
    normaliser: Normaliser,
    spec: TableSpec,
    batch_key: list[str],
    s: Settings,
) -> str | None:
    """Reads the table AS OF the check SCN into the store; returns a problem or None."""
    store.clear("db", spec.name)
    try:
        for batch in db.rows(
            spec.owner, spec.table, spec.columns, batch_key, s.check_scn, s.batch_rows
        ):
            keyed: list[tuple[str, str]] = []
            keyless: list[str] = []
            for raw in batch:
                values = {
                    c.name: normaliser.from_database(raw.get(c.name), c) for c in spec.columns
                }
                row = canonical([(c.name, values[c.name]) for c in spec.columns])
                if spec.key:
                    keyed.append((canonical([(k, values[k]) for k in spec.key]), row))
                else:
                    keyless.append(row)
            if keyed:
                store.put_many("db", spec.name, keyed)
            if keyless:
                store.count_many("db", spec.name, keyless)
    except SnapshotTooOld as e:
        return (
            f"the flashback query could not reach the check SCN ({e}); choose a later check SCN"
            " or raise UNDO_RETENTION"
        )
    except NormaliseError as e:
        return f"a database value could not be normalised: {e}"
    except Exception as e:
        return f"reading the table failed: {e}"
    return None


def _samples(c: Comparison, spec: TableSpec, show_values: bool) -> list[dict[str, Any]]:
    out = []
    for d in c.samples:
        entry: dict[str, Any] = {}
        if d.db_row is None:
            entry["difference"] = "in the topics only"
        elif d.kafka_row is None:
            entry["difference"] = "in the database only"
        else:
            entry["difference"] = "different values" if d.db_row != d.kafka_row else "copies"
        entry["key"] = json.loads(d.key) if spec.key or show_values else MASK
        if d.db_row is not None and d.kafka_row is not None:
            a, b = dict(json.loads(d.db_row)), dict(json.loads(d.kafka_row))
            entry["columns"] = [k for k in a if a.get(k) != b.get(k)]
            if show_values:
                entry["values"] = {
                    k: {"database": a.get(k), "topic": b.get(k)} for k in entry["columns"]
                }
        elif show_values:
            entry["row"] = json.loads(d.db_row if d.db_row is not None else d.kafka_row)
        if not spec.key:
            entry["copies"] = {"database": d.db_copies, "topic": d.kafka_copies}
        out.append(entry)
    return out


def verify(
    db: VerifyDatabase, topics: TopicSource, store: RowStore, s: Settings
) -> list[TableResult]:
    normaliser = Normaliser(s.target.decimal_mode, s.target.temporal_mode)
    database = db.database_name()
    pdb = s.pdb or (db.container_name() if s.target.cdb else None)
    resolved = []
    for req in s.tables:
        spec, batch_key, problems = _spec(db, req, s, database, pdb)
        resolved.append((spec, batch_key, problems))
    specs = [r[0] for r in resolved]
    materialiser = Materialiser(store, normaliser, s.check_scn, s.target.placeholder, specs)
    for topic in sorted({sp.topic for sp in specs}):
        LOG.info("Reading %s", topic)
        for record in topics.read(topic):
            try:
                materialiser.apply(parse(record))
            except RecordError as e:
                for sp in materialiser.by_topic.get(topic, []):
                    materialiser.stats[sp.name].error(
                        f"{record.topic}-{record.partition}@{record.offset}", str(e)
                    )
    results = []
    for spec, batch_key, problems in resolved:
        stats = materialiser.stats[spec.name]
        if not problems:
            LOG.info("Reading %s AS OF SCN %d", spec.name, s.check_scn)
            problem = _scan_database(db, store, normaliser, spec, batch_key, s)
            if problem:
                problems.append(problem)
        if stats.error_count:
            problems.append(f"{stats.error_count} records could not be read: {stats.errors[0]}")
        if stats.without_commit_scn:
            problems.append(f"{stats.without_commit_scn} records carry no commit SCN")
        if stats.snapshot_after_check_scn:
            problems.append(
                f"{stats.snapshot_after_check_scn} snapshot rows were read after the check SCN;"
                " choose a check SCN after the snapshot completed"
            )
        comparison = None if problems else store.compare(spec.name, s.samples)
        if problems:
            status, reason = INCONCLUSIVE, "; ".join(problems)
        elif comparison.equal:
            status, reason = PASS, "row count and hash equal"
        else:
            status = FAIL
            reason = (
                f"{_rows(comparison.missing_count)} only in the database,"
                f" {_rows(comparison.extra_count)} only in the topics,"
                f" {_rows(comparison.mismatch_count)} with different values"
            )
        results.append(
            TableResult(
                table=spec.name,
                topic=spec.topic,
                status=status,
                reason=reason,
                key_columns=spec.key,
                compared_columns=[c.name for c in spec.columns],
                ignored_columns=spec.ignored,
                comparison=comparison,
                stats=stats,
                samples=_samples(comparison, spec, s.show_values) if comparison else [],
            )
        )
    return results


def _rows(n: int) -> str:
    return f"{n} row" if n == 1 else f"{n} rows"


# Evidence (VER-4) ------------------------------------------------------------------------------


def canonical_json(document: Any) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def seal(document: dict[str, Any]) -> dict[str, Any]:
    body = {k: v for k, v in document.items() if k != "sha256"}
    return {**body, "sha256": hashlib.sha256(canonical_json(body)).hexdigest()}


def check_seal(document: dict[str, Any]) -> bool:
    return seal(document).get("sha256") == document.get("sha256")


def evidence(
    results: list[TableResult], inputs: dict[str, Any], started: datetime
) -> dict[str, Any]:
    tables = []
    for r in results:
        entry: dict[str, Any] = {
            "table": r.table,
            "topic": r.topic,
            "status": r.status,
            "reason": r.reason,
            "key_columns": r.key_columns,
            "compared_columns": r.compared_columns,
            "ignored_columns": r.ignored_columns,
            "records": asdict(r.stats),
        }
        if r.comparison:
            c = r.comparison
            entry.update(
                {
                    "database_rows": c.db_rows,
                    "topic_rows": c.kafka_rows,
                    "database_hash": c.db_hash,
                    "topic_hash": c.kafka_hash,
                    "only_in_database": c.missing_count,
                    "only_in_topics": c.extra_count,
                    "different": c.mismatch_count,
                    "samples": r.samples,
                }
            )
        tables.append(entry)
    overall = PASS if results and all(r.status == PASS for r in results) else FAIL
    return seal(
        {
            "tool": PROG,
            "tool_version": __version__,
            "started_at": started.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "result": overall,
            "inputs": inputs,
            "tables": tables,
        }
    )


def render_markdown(doc: dict[str, Any]) -> str:
    i = doc["inputs"]
    lines = [
        f"# Cutover verification: {doc['result']}",
        "",
        "| Item | Value |",
        "|---|---|",
        f"| Tool | `{doc['tool']}` {doc['tool_version']} |",
        f"| Check SCN | {i['check_scn']} ({i['check_scn_source']}) |",
        f"| Connector position | {_position_text(i['position_check'])} |",
        f"| Record format | {_KINDS.get(i['connector_kind'], i['connector_kind'])},"
        f" decimal mode {i['decimal_mode']},"
        f" temporal mode {i['temporal_mode']} |",
        f"| Started | {doc['started_at']} |",
        f"| Evidence SHA-256 | `{doc['sha256']}` |",
        "",
        "| Table | Topic | Result | Database rows | Topic rows | Reason |",
        "|---|---|---|---|---|---|",
    ]
    for t in doc["tables"]:
        lines.append(
            f"| `{t['table']}` | `{t['topic']}` | {t['status']} | {t.get('database_rows', '')}"
            f" | {t.get('topic_rows', '')} | {t['reason'].replace('|', '/')} |"
        )
    for t in doc["tables"]:
        if t.get("samples"):
            lines += ["", f"## Differences in `{t['table']}` (first {len(t['samples'])})", ""]
            lines += ["```json", *[json.dumps(x) for x in t["samples"]], "```"]
        if t["ignored_columns"]:
            lines += ["", f"Columns of `{t['table']}` not compared:", ""]
            lines += [f"- `{k}`: {v}" for k, v in t["ignored_columns"].items()]
    return "\n".join(lines) + "\n"


_KINDS = {
    "oso": "OSO CDC Connector",
    "debezium": "Debezium Oracle connector",
    "none": "set on the command line",
}


def _position_text(p: dict[str, Any]) -> str:
    if not p.get("checked"):
        return "not checked (--no-position-check)"
    return f"`{p['connector']}` at SCN {p['position_scn']}, past the check SCN"


def render_summary(doc: dict[str, Any], output: str) -> str:
    lines = [f"Cutover verification at SCN {doc['inputs']['check_scn']}: {doc['result']}"]
    for t in doc["tables"]:
        lines.append(f"  {t['status']:<12} {t['table']} ({t['topic']}): {t['reason']}")
    lines.append(f"Evidence: {output} (SHA-256 {doc['sha256']})")
    return "\n".join(lines) + "\n"


# Command line ----------------------------------------------------------------------------------

DESCRIPTION = (
    "Compare each table in the database, read AS OF a check SCN in key-range batches, with the"
    " state materialised from its topic up to the same SCN, using one normalisation for both"
    " sides. Prints PASS or FAIL per table with up to 100 differing keys (values masked unless"
    " --show-values) and writes evidence JSON with a SHA-256 over its canonical form. Works on"
    " topics written by this connector and by the Debezium Oracle connector, with the JSON"
    " converter."
)
EPILOG = """\
examples:
  %(prog)s --input new-connector.json --table APP.ORDERS --table APP.CUSTOMERS \\
      --db-dsn db:1521/ORCLPDB1 --db-user c##cdc --db-password-env ORACLE_PASSWORD \\
      --bootstrap-servers kafka:9092 --connect-url http://connect:8083 \\
      --output evidence.json --report verification.md
  %(prog)s --check-evidence evidence.json

result: 0 when every table passes, 1 otherwise (FAIL or INCONCLUSIVE)."""


def build_parser():
    p = parser(PROG, DESCRIPTION, EPILOG % {"prog": PROG})
    add_common_options(
        p,
        input_help="connector configuration (this connector's or Debezium's) that gives the"
        " topic names, the decimal and temporal modes, the LOB mode and the placeholder",
        output_help="write the evidence JSON to FILE (default cutover-evidence.json)",
    )
    p.add_argument(
        "--check-evidence",
        metavar="FILE",
        help="only recompute the SHA-256 of an evidence file and say whether it matches",
    )
    p.add_argument(
        "--table",
        action="append",
        default=[],
        metavar="OWNER.TABLE[=TOPIC]",
        help="a table to verify, optionally with its topic; repeat for more tables",
    )
    p.add_argument("--db-dsn", help="the PDB (or non-CDB) holding the tables, host:port/service")
    p.add_argument("--db-user", help="a user with SELECT and FLASHBACK on the tables")
    secret = p.add_mutually_exclusive_group()
    secret.add_argument("--db-password-env", metavar="VAR", help="variable holding the password")
    secret.add_argument("--db-password-file", metavar="FILE", help="file holding the password")
    p.add_argument("--bootstrap-servers", help="Kafka bootstrap servers")
    p.add_argument(
        "--kafka-config",
        metavar="FILE",
        help="properties file with further client settings (security.protocol, sasl.*, ssl.*)",
    )
    p.add_argument("--topic-prefix", help="topic prefix, when --input is not given")
    p.add_argument(
        "--topic-template",
        help="topic template with ${prefix}, ${pdb}, ${schema}, ${table} and ${database}"
        " (default from --input, else ${prefix}.${schema}.${table})",
    )
    p.add_argument("--pdb", help="PDB name for ${pdb} (default: the connected container)")
    p.add_argument("--decimal-mode", choices=("precise", "string", "double"))
    p.add_argument("--temporal-mode", choices=("adaptive", "iso_string"))
    p.add_argument("--unavailable-placeholder", metavar="TEXT")
    p.add_argument(
        "--lob-columns",
        choices=("compare", "skip"),
        help="compare LOB columns, or leave them out (default from --input, else skip)",
    )
    p.add_argument(
        "--ignore-column",
        action="append",
        default=[],
        metavar="OWNER.TABLE.COLUMN",
        help="a column not to compare (for example one the old connector excluded); repeatable",
    )
    p.add_argument("--check-scn", type=int, help="the SCN to compare at")
    p.add_argument(
        "--safety-margin-seconds",
        type=int,
        default=30,
        help="without --check-scn, use the SCN of this many seconds ago (default 30)",
    )
    p.add_argument("--connect-url", metavar="URL", help="REST URL of the Kafka Connect cluster")
    p.add_argument(
        "--connect-auth-env",
        metavar="VAR",
        help="environment variable holding user:password for the Connect REST API",
    )
    p.add_argument("--connector", help="connector whose position must pass the check SCN")
    p.add_argument(
        "--wait-seconds",
        type=int,
        default=300,
        help="how long to wait for the connector position to pass the check SCN (default 300)",
    )
    p.add_argument(
        "--no-position-check",
        action="store_true",
        help="do not confirm the connector position; the evidence records that it was skipped",
    )
    p.add_argument("--batch-rows", type=int, default=50_000, help="rows per flashback query")
    p.add_argument("--show-values", action="store_true", help="show values in the samples")
    p.add_argument("--work-dir", metavar="DIR", help="directory for the temporary row store")
    return p


def _kafka_config(path: str | None, redactor: Redactor) -> dict[str, str]:
    if not path:
        return {}
    config = configio.parse_properties(Path(path).read_text(encoding="utf-8"))
    for k, v in config.items():
        if is_secret_key(k) or k.endswith(".pem") or "key" in k.split("."):
            redactor.add(v)
    return config


def main(
    argv: list[str] | None = None,
    *,
    db: VerifyDatabase | None = None,
    topics: TopicSource | None = None,
    api: ConnectApi | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    args = build_parser().parse_args(argv)
    redactor = Redactor()
    configure_logging(redactor, args.verbose)

    def check_only() -> int:
        doc = json.loads(Path(args.check_evidence).read_text(encoding="utf-8"))
        ok = check_seal(doc)
        emit(
            f"{args.check_evidence}: SHA-256 "
            + ("matches; the evidence is unchanged." if ok else "DOES NOT MATCH."),
            redactor,
        )
        return EXIT_OK if ok else EXIT_FAILURE

    def body() -> int:
        if args.check_evidence:
            return check_only()
        started = datetime.now(UTC)
        if not args.table:
            raise ToolError("Give at least one --table.")
        cfg = configio.load(args.input) if args.input else None
        if cfg:
            redactor.register_config(cfg.config)
        target = target_from_config(cfg)
        if args.topic_prefix:
            target.prefix = args.topic_prefix
        if args.topic_template:
            target.template = args.topic_template
        if args.decimal_mode:
            target.decimal_mode = args.decimal_mode
        if args.temporal_mode:
            target.temporal_mode = args.temporal_mode
        if args.unavailable_placeholder:
            target.placeholder = args.unavailable_placeholder
        if args.lob_columns:
            target.lob_skip = args.lob_columns == "skip"

        # read before anything can fail, so the secrets are masked in every message
        password = read_secret(
            redactor, env=args.db_password_env, file=args.db_password_file, what="database password"
        )
        auth = read_secret(redactor, env=args.connect_auth_env, what="Connect credentials")
        kafka_config = _kafka_config(args.kafka_config, redactor)
        database = db
        if database is None:
            if not args.db_dsn or not args.db_user:
                raise ToolError("Give --db-dsn and --db-user for the PDB holding the tables.")
            if password is None:
                raise ToolError("Give --db-password-env or --db-password-file.")
            database = OracleVerifyDatabase(connect(args.db_dsn, args.db_user, password))
        source = topics
        if source is None:
            if not args.bootstrap_servers:
                raise ToolError("Give --bootstrap-servers.")
            source = KafkaTopicSource(args.bootstrap_servers, kafka_config)
        client = api
        if client is None and args.connect_url:
            client = ConnectClient(args.connect_url, auth)

        if args.check_scn is not None:
            check_scn, how = args.check_scn, "given"
        else:
            check_scn = database.scn_seconds_ago(args.safety_margin_seconds)
            how = f"the SCN of {args.safety_margin_seconds} seconds ago"
        position: dict[str, Any] = {"checked": False}
        if not args.no_position_check:
            connector = args.connector or target.name
            if client is None or not connector:
                raise ToolError(
                    "Give --connect-url and --connector so the connector position can be"
                    " confirmed past the check SCN, or --no-position-check."
                )
            position = {
                "checked": True,
                **confirm_position(client, connector, check_scn, args.wait_seconds, sleep),
            }

        settings = Settings(
            check_scn=check_scn,
            tables=[parse_table(t) for t in args.table],
            target=target,
            batch_rows=args.batch_rows,
            show_values=args.show_values,
            ignore_columns={c.upper() for c in args.ignore_column},
            pdb=args.pdb,
        )
        with tempfile.TemporaryDirectory(dir=args.work_dir) as work:
            store = RowStore(str(Path(work) / "rows.sqlite"))
            try:
                results = verify(database, source, store, settings)
            finally:
                store.close()
        inputs = {
            "database": {"dsn": args.db_dsn, "user": args.db_user},
            "bootstrap_servers": args.bootstrap_servers,
            "connector_config": args.input,
            "connector_kind": target.kind,
            "tables": [f"{t.owner}.{t.table}" for t in settings.tables],
            "check_scn": check_scn,
            "check_scn_source": how,
            "position_check": position,
            "decimal_mode": target.decimal_mode,
            "temporal_mode": target.temporal_mode,
            "unavailable_placeholder": target.placeholder,
            "lob_columns": "skip" if target.lob_skip else "compare",
            "ignored_columns": sorted(settings.ignore_columns),
            "batch_rows": settings.batch_rows,
            "show_values": settings.show_values,
        }
        for r in results:
            r.reason = redactor.scrub(r.reason)
        doc = evidence(results, inputs, started)
        output = args.output or "cutover-evidence.json"
        write_text(output, json.dumps(doc, indent=2) + "\n", redactor)
        if args.report:
            write_text(args.report, render_markdown(doc), redactor)
        if args.json:
            emit(json.dumps(doc, indent=2), redactor)
        else:
            emit(render_summary(doc, output), redactor)
        return EXIT_OK if doc["result"] == PASS else EXIT_FAILURE

    def guarded() -> int:
        try:
            return body()
        except ConnectError as e:
            raise ToolError(str(e)) from None
        except OSError as e:
            raise ToolError(f"{e}") from None

    return run(guarded, redactor)


if __name__ == "__main__":
    sys.exit(main())
