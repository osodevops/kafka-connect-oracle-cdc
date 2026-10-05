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
"""The command line both translators share (PRD-04 s2)."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field

from . import configio
from .common import (
    EXIT_FAILURE,
    ToolError,
    add_common_options,
    configure_logging,
    emit,
    parser,
    read_secret,
    run,
    write_text,
)
from .connect import ConnectApi, ConnectClient, check_exactly_once
from .redact import Redactor
from .translate import (
    Finaliser,
    RuleSet,
    TranslationResult,
    render_config,
    render_markdown,
    render_summary,
    to_dict,
    translate,
)

LOG = logging.getLogger(__name__)


@dataclass
class TranslatorSpec:
    prog: str
    source_kind: str
    source_classes: tuple[str, ...]
    other_tool: str
    rules: RuleSet
    finalisers: list[Finaliser]
    next_steps: Callable[[str | None, str, str | None], list[str]]
    description: str
    extra_options: Callable | None = None
    prepare: Callable | None = None
    epilog: str = field(
        default=(
            "example:\n"
            "  %(prog)s --input old-connector.json --output new-connector.json \\\n"
            "      --report migration-report.md --connect-url http://connect:8083"
        )
    )


def default_target_name(source_name: str | None) -> str:
    base = source_name or "oracle-cdc"
    return re.sub(r"[^A-Za-z0-9._-]", "-", base) + "-oso"


def build_parser(spec: TranslatorSpec):
    p = parser(spec.prog, spec.description, spec.epilog % {"prog": spec.prog})
    add_common_options(
        p,
        input_help="the source connector configuration: a JSON file (Connect REST create body,"
        " GET /connectors/{name} output or a flat map) or a .properties file",
        output_help="write the translated configuration to FILE (JSON create body, or"
        " .properties when FILE ends in .properties)",
        input_required=True,
    )
    p.add_argument(
        "--name",
        help="name of the new connector (default: the source name with -oso appended); it"
        " must differ from the source connector's name",
    )
    p.add_argument(
        "--format",
        choices=("json", "properties"),
        help="format of --output (default: from the file extension, else json)",
    )
    p.add_argument(
        "--connect-url",
        metavar="URL",
        help="REST URL of the target Kafka Connect cluster, used to check whether exactly-once"
        " source support is enabled (MIG-4)",
    )
    p.add_argument(
        "--connect-auth-env",
        metavar="VAR",
        help="environment variable holding user:password for the Connect REST API",
    )
    p.add_argument(
        "--start-scn",
        type=int,
        metavar="SCN",
        help="the takeover SCN from takeover_scn.py, written as cdc.start.scn; without it the"
        " report asks you to run takeover_scn.py",
    )
    if spec.extra_options:
        spec.extra_options(p)
    return p


def translator_main(
    spec: TranslatorSpec, argv: list[str] | None = None, api: ConnectApi | None = None
) -> int:
    args = build_parser(spec).parse_args(argv)
    redactor = Redactor()
    configure_logging(redactor, args.verbose)

    def body() -> int:
        source = configio.load(args.input)
        redactor.register_config(source.config)
        cls = source.config.get("connector.class", "").strip()
        if cls and cls not in spec.source_classes:
            raise ToolError(
                f"{args.input} configures `{cls}`, not a {spec.source_kind}. Use"
                f" {spec.other_tool} for other source connectors."
            )
        target_name = args.name or default_target_name(source.name)
        if source.name and target_name == source.name:
            raise ToolError(
                "The new connector needs a name of its own: Kafka Connect keeps offsets per"
                " connector name, and the new connector cannot read the old connector's offset."
            )
        auth = read_secret(redactor, env=args.connect_auth_env, what="Connect credentials")
        client = api
        if client is None and args.connect_url:
            client = ConnectClient(args.connect_url, auth)
        check = check_exactly_once(client)
        fmt = args.format or (
            "properties" if args.output and args.output.endswith(".properties") else "json"
        )
        state = spec.prepare(args) if spec.prepare else {}
        state["start_scn"] = args.start_scn
        result = translate(
            source,
            tool=spec.prog,
            source_kind=spec.source_kind,
            rules=spec.rules,
            finalisers=spec.finalisers,
            target_name=target_name,
            redactor=redactor,
            exactly_once=check,
            next_steps=spec.next_steps(source.name, target_name, args.output),
            state=state,
        )
        return finish(result, args, fmt, redactor)

    return run(body, redactor)


def finish(result: TranslationResult, args, fmt: str, redactor: Redactor) -> int:
    wrote = []
    try:
        if args.output:
            write_text(args.output, render_config(result, fmt), redactor)
            wrote.append(f"configuration to {args.output}")
        if args.report:
            write_text(args.report, render_markdown(result), redactor)
            wrote.append(f"report to {args.report}")
    except OSError as e:
        LOG.error("Cannot write %s: %s", e.filename, e.strerror)
        return EXIT_FAILURE
    if args.json:
        emit(json.dumps(to_dict(result), indent=2), redactor)
    else:
        emit(render_summary(result, wrote), redactor)
    return result.exit_code
