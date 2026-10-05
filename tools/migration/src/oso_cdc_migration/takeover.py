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
"""``takeover_scn``: read the stopped old connector's offset, check that the redo from its SCN is
still there, and write ``cdc.start.scn`` (with ``cdc.snapshot.mode=none``) into the new
connector's configuration (PRD-04 s4).

The connector honours ``cdc.start.scn`` only while it has no stored offset, so the new connector
needs a name of its own and must not have run before.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

from . import __version__, configio
from .common import (
    EXIT_FAILURE,
    EXIT_FOLLOW_UPS,
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
from .connect import ConnectApi, ConnectClient, ConnectError, connector_state
from .offsets import OldPosition, parse_offsets
from .oracle import DatabaseIdentity, OracleTakeoverDatabase, connect, dsn_from_config
from .redact import Redactor
from .redo import LogFile, RedoCoverage, check_coverage, ranges
from .translate import ordered

LOG = logging.getLogger(__name__)
PROG = "takeover_scn.py"


class TakeoverDatabase(Protocol):
    def identity(self) -> DatabaseIdentity: ...

    def logs(self, start_scn: int, resetlogs_scn: int) -> list[LogFile]: ...


@dataclass
class TakeoverResult:
    source: str
    old_connector: str | None
    old_state: str | None
    position: OldPosition
    identity: DatabaseIdentity
    coverage: RedoCoverage
    new_connector: str | None
    problems: list[str] = field(default_factory=list)
    follow_ups: list[str] = field(default_factory=list)
    output: str | None = None

    @property
    def start_scn(self) -> int | None:
        return None if self.problems else self.position.start_scn

    @property
    def exit_code(self) -> int:
        if self.problems:
            return EXIT_FAILURE
        return EXIT_FOLLOW_UPS if self.follow_ups else EXIT_OK


def take_over(
    *,
    offsets_document: Any,
    source: str,
    old_connector: str | None,
    old_state: str | None,
    db: TakeoverDatabase,
    new_connector: str | None,
) -> TakeoverResult:
    position = parse_offsets(offsets_document, source)
    identity = db.identity()
    problems: list[str] = []
    follow_ups: list[str] = []
    if position.start_scn > identity.current_scn:
        raise ToolError(
            f"The offset's SCN {position.start_scn} is ahead of the database's current SCN"
            f" {identity.current_scn}. The tool is connected to a different database than the"
            " old connector."
        )
    if position.start_scn < identity.resetlogs_scn:
        raise ToolError(
            f"The offset's SCN {position.start_scn} is older than the database's RESETLOGS SCN"
            f" {identity.resetlogs_scn}: it belongs to an earlier incarnation, so the redo"
            " cannot be mined. Start the new connector with a snapshot instead."
        )
    coverage = check_coverage(
        position.start_scn, db.logs(position.start_scn, identity.resetlogs_scn)
    )
    problems += coverage.problems()
    if identity.log_mode and identity.log_mode.upper() != "ARCHIVELOG":
        problems.append(f"The database runs in {identity.log_mode} mode, not ARCHIVELOG.")
    if len(coverage.threads) > 1:
        follow_ups.append(
            f"The database has {len(coverage.threads)} redo threads (RAC). RAC is not supported"
            " in this release; do not cut over yet."
        )
    if old_state is None:
        follow_ups.append(
            "The offsets were read from a file, so the tool could not confirm that the old"
            " connector was stopped when they were taken. Stop it with"
            " `PUT /connectors/{name}/stop` and read the offsets again if in doubt."
        )
    return TakeoverResult(
        source=position.source,
        old_connector=old_connector,
        old_state=old_state,
        position=position,
        identity=identity,
        coverage=coverage,
        new_connector=new_connector,
        problems=problems,
        follow_ups=follow_ups,
    )


def check_new_connector(api: ConnectApi, name: str, result: TakeoverResult) -> None:
    """``cdc.start.scn`` is ignored once an offset exists, so the new connector must have none."""
    try:
        api.status(name)
    except ConnectError as e:
        if e.status == 404:
            return  # not created yet: it will start without an offset
        raise
    stored = [e for e in api.offsets(name).get("offsets", []) if e.get("offset")]
    if stored:
        result.problems.append(
            f"The new connector `{name}` already has a stored offset, so it would ignore"
            f" cdc.start.scn. Delete the connector and create it again under a new name, or stop"
            f" it and remove the offset with `DELETE /connectors/{name}/offsets`."
        )
    else:
        result.follow_ups.append(
            f"The new connector `{name}` already exists. Update its configuration with the"
            " start SCN before it first runs; once it has written an offset, cdc.start.scn is"
            " ignored."
        )


def with_start(config: configio.ConnectorConfig | None, start_scn: int) -> dict[str, str]:
    out = dict(config.config) if config else {}
    out["cdc.start.scn"] = str(start_scn)
    out["cdc.snapshot.mode"] = "none"
    return ordered(out)


def render_output(target: configio.ConnectorConfig | None, path: str, start_scn: int) -> str:
    config = with_start(target, start_scn)
    if path.endswith(".properties"):
        return configio.format_properties(config)
    if target is None:
        return json.dumps(config, indent=2) + "\n"
    return json.dumps({"name": target.name, "config": config}, indent=2) + "\n"


# Reports -----------------------------------------------------------------------------------------


def to_dict(r: TakeoverResult) -> dict[str, Any]:
    return {
        "tool": PROG,
        "tool_version": __version__,
        "exit_code": r.exit_code,
        "source": r.source,
        "old_connector": r.old_connector,
        "old_connector_state": r.old_state,
        "start_scn": r.start_scn,
        "start_scn_field": r.position.start_field,
        "old_commit_scn": r.position.last_commit_scn,
        "old_commit_scn_raw": r.position.commit_scn_raw,
        "old_offsets": r.position.entries,
        "notes": r.position.notes,
        "database": asdict(r.identity),
        "redo": {"ok": r.coverage.ok, "threads": [asdict(t) for t in r.coverage.threads]},
        "new_connector": r.new_connector,
        "settings": (
            {"cdc.start.scn": str(r.start_scn), "cdc.snapshot.mode": "none"}
            if r.start_scn is not None
            else None
        ),
        "output": r.output,
        "problems": r.problems,
        "follow_ups": r.follow_ups,
        "next_steps": next_steps(r),
    }


def next_steps(r: TakeoverResult) -> list[str]:
    if r.problems:
        return [
            "Resolve the problems above. If the redo is gone, restore the archived logs, or start"
            " the new connector with a snapshot instead of a takeover."
        ]
    name = r.new_connector or "<new-connector>"
    where = f"`{r.output}`" if r.output else "the new connector's configuration"
    overlap = (
        f" up to the old connector's last commit at SCN {r.position.last_commit_scn}"
        if r.position.last_commit_scn
        else ""
    )
    return [
        f"Create `{name}` from {where}, with `cdc.start.scn={r.start_scn}` and"
        " `cdc.snapshot.mode=none`. It starts streaming at that SCN because it has no stored"
        " offset yet.",
        f"Changes committed after SCN {r.start_scn}{overlap} are delivered again (bounded"
        " duplicates during the overlap; consumers that upsert by key are unaffected). There is"
        " no gap.",
        "After the overlap has passed, run `verify_cutover.py` and archive the evidence file.",
    ]


def render_markdown(r: TakeoverResult) -> str:
    redo = "available on every thread" if r.coverage.ok else "missing; see the problems"
    old = f"`{r.old_connector}` ({r.old_state})" if r.old_connector else "offsets read from a file"
    if r.start_scn is None:
        written = "not produced"
    else:
        written = f"written to `{r.output}`" if r.output else "printed only"
    lines = [
        f"# Takeover report: {r.source.capitalize()} to the OSO CDC Connector",
        "",
        "| Item | Value |",
        "|---|---|",
        f"| Tool | `{PROG}` {__version__} |",
        f"| Old connector | {old} |",
        f"| Start SCN | {r.position.start_scn}, from the offset's {r.position.start_field} |",
        f"| Old connector's last commit SCN | {r.position.last_commit_scn or 'not recorded'} |",
        f"| Database | DBID {r.identity.dbid}, RESETLOGS SCN {r.identity.resetlogs_scn},"
        f" current SCN {r.identity.current_scn} |",
        f"| Redo from the start SCN | {redo} |",
        f"| New connector | `{r.new_connector or '(not named)'}` |",
        f"| `cdc.start.scn` | {written} |",
        f"| Exit code | {r.exit_code} |",
        "",
    ]
    if r.problems:
        lines += ["## Problems", ""] + [f"1. {p}" for p in r.problems] + [""]
    if r.follow_ups:
        lines += ["## Follow-ups", ""] + [f"1. {f}" for f in r.follow_ups] + [""]
    if r.position.notes:
        lines += ["## Notes", ""] + [f"- {n}" for n in r.position.notes] + [""]
    lines += [
        "## Redo logs",
        "",
        "| Thread | First sequence needed | Current sequence | Missing |",
        "|---|---|---|---|",
    ]
    for t in r.coverage.threads:
        first = t.first_sequence if t.first_sequence is not None else "none"
        missing = ranges(t.missing) or "none"
        lines.append(f"| {t.thread} | {first} | {t.last_sequence} | {missing} |")
    lines += [
        "",
        "## Old connector's offsets",
        "",
        "```json",
        json.dumps({"offsets": r.position.entries}, indent=2),
        "```",
        "",
        "## Next steps",
        "",
    ]
    lines += [f"1. {s}" for s in next_steps(r)]
    return "\n".join(lines) + "\n"


def render_summary(r: TakeoverResult) -> str:
    commit = f", last commit SCN {r.position.last_commit_scn}" if r.position.last_commit_scn else ""
    lines = [
        f"{r.source.capitalize()} offset: start SCN {r.position.start_scn}{commit}.",
        "Redo: " + ("available from the start SCN." if r.coverage.ok else "MISSING."),
    ]
    lines += [f"Problem: {p}" for p in r.problems]
    lines += [f"Follow-up: {f}" for f in r.follow_ups]
    if r.start_scn is not None:
        lines.append(f"Set cdc.start.scn={r.start_scn} and cdc.snapshot.mode=none.")
    if r.output:
        lines.append(f"Wrote {r.output}.")
    lines += [f"Next: {s}" for s in next_steps(r)]
    return "\n".join(lines) + "\n"


# Command line ----------------------------------------------------------------------------------

DESCRIPTION = (
    "Read the committed offset of a stopped Debezium Oracle or Confluent Oracle CDC Source"
    " connector, check that the archived logs from its SCN still exist, and give the new OSO CDC"
    " Connector cdc.start.scn (with cdc.snapshot.mode=none) so it starts streaming where the old"
    " connector stopped. With --target-config the tool writes the new connector's configuration"
    " with both settings to --output."
)
EPILOG = """\
examples:
  %(prog)s --connect-url http://connect:8083 --connector inventory-connector \\
      --target-config new-connector.json --db-password-env ORACLE_PASSWORD \\
      --output new-connector-takeover.json --report takeover.md
  %(prog)s --input old-offsets.json --db-dsn db:1521/ORCLCDB --db-user c##cdc \\
      --db-password-file /run/secrets/oracle --json"""


def build_parser():
    p = parser(PROG, DESCRIPTION, EPILOG % {"prog": PROG})
    add_common_options(
        p,
        input_help="the old connector's offsets as returned by GET /connectors/{name}/offsets,"
        " instead of reading them through --connect-url",
        output_help="write the new connector's configuration with cdc.start.scn and"
        " cdc.snapshot.mode=none to FILE (just those two settings without --target-config)",
    )
    p.add_argument("--connect-url", metavar="URL", help="REST URL of the Kafka Connect cluster")
    p.add_argument(
        "--connect-auth-env",
        metavar="VAR",
        help="environment variable holding user:password for the Connect REST API",
    )
    p.add_argument("--connector", help="name of the old (stopped) connector")
    p.add_argument(
        "--source",
        choices=("auto", "debezium", "confluent"),
        default="auto",
        help="which connector wrote the offsets (default: detected from the partition)",
    )
    p.add_argument(
        "--target-config",
        metavar="FILE",
        help="the translated configuration of the new connector; supplies its name and the"
        " database address and user, and is the base of --output",
    )
    p.add_argument(
        "--target-connector",
        help="name of the new connector (default: from --target-config); with --connect-url"
        " the tool checks that it has no stored offset",
    )
    p.add_argument("--db-dsn", help="database address, for example host:1521/ORCLCDB")
    p.add_argument("--db-user", help="database user (the connector's user will do)")
    secret = p.add_mutually_exclusive_group()
    secret.add_argument(
        "--db-password-env", metavar="VAR", help="environment variable holding the password"
    )
    secret.add_argument("--db-password-file", metavar="FILE", help="file holding the password")
    return p


def main(
    argv: list[str] | None = None,
    *,
    api: ConnectApi | None = None,
    db: TakeoverDatabase | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    redactor = Redactor()
    configure_logging(redactor, args.verbose)

    def body() -> int:
        # read secrets before anything can fail, so they are masked in every message
        password = read_secret(
            redactor, env=args.db_password_env, file=args.db_password_file, what="database password"
        )
        auth = read_secret(redactor, env=args.connect_auth_env, what="Connect credentials")
        target = configio.load(args.target_config) if args.target_config else None
        if target:
            redactor.register_config(target.config)
        new_name = args.target_connector or (target.name if target else None)
        client = api
        if client is None and args.connect_url:
            client = ConnectClient(args.connect_url, auth)

        old_state = None
        if args.input:
            document = json.loads(Path(args.input).read_text(encoding="utf-8"))
        else:
            if client is None or not args.connector:
                raise ToolError("Give --input, or --connect-url with --connector.")
            old_state = connector_state(client, args.connector)
            if old_state != "STOPPED":
                raise ToolError(
                    f"The old connector `{args.connector}` is {old_state}. Stop it with"
                    f" `PUT /connectors/{args.connector}/stop` so its offset is final; pausing"
                    " is not enough."
                )
            document = client.offsets(args.connector)
        if args.connector and new_name == args.connector:
            raise ToolError(
                "The new connector needs a name of its own: Kafka Connect keeps offsets per"
                " connector name, and cdc.start.scn is ignored once an offset exists."
            )

        database = db
        if database is None:
            dsn = args.db_dsn or (dsn_from_config(target) if target else None)
            user = args.db_user or (target.config.get("cdc.database.user") if target else None)
            if not dsn or not user:
                raise ToolError("Give --db-dsn and --db-user, or a --target-config naming them.")
            if password is None:
                raise ToolError("Give --db-password-env or --db-password-file.")
            database = OracleTakeoverDatabase(connect(dsn, user, password))

        result = take_over(
            offsets_document=document,
            source=args.source,
            old_connector=args.connector,
            old_state=old_state,
            db=database,
            new_connector=new_name,
        )
        if client is not None and new_name:
            check_new_connector(client, new_name, result)
        if result.start_scn is not None and args.output:
            write_text(args.output, render_output(target, args.output, result.start_scn), redactor)
            result.output = args.output
        if args.report:
            write_text(args.report, render_markdown(result), redactor)
        if args.json:
            emit(json.dumps(to_dict(result), indent=2), redactor)
        else:
            emit(render_summary(result), redactor)
        return result.exit_code

    def guarded() -> int:
        try:
            return body()
        except ConnectError as e:
            raise ToolError(str(e)) from None
        except (OSError, ValueError) as e:
            raise ToolError(f"{e}") from None

    return run(guarded, redactor)


if __name__ == "__main__":
    sys.exit(main())
