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
"""Runs a translator golden case: input, args.json and the expected files under expected/.

Set UPDATE_GOLDEN=1 to rewrite the expected files after a deliberate change, then review the
diff.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from oso_cdc_migration import __version__
from oso_cdc_migration.confluent import SPEC as CONFLUENT
from oso_cdc_migration.connect import ConnectError
from oso_cdc_migration.debezium import SPEC as DEBEZIUM
from oso_cdc_migration.translator_cli import translator_main

from .fakes import FakeConnect, eos_response

GOLDEN = Path(__file__).parent / "golden"
UPDATE = os.environ.get("UPDATE_GOLDEN") == "1"


def cases(kind: str) -> list[Path]:
    return sorted(p for p in (GOLDEN / kind).iterdir() if p.is_dir())


def connect_for(eos: str | None) -> FakeConnect | None:
    if eos is None:
        return None
    api = FakeConnect()
    if eos == "enabled":
        api.validate_response = eos_response([])
    elif eos == "disabled":
        api.validate_response = eos_response(
            ["This worker does not have exactly-once source support enabled."]
        )
    else:
        api.validate_error = ConnectError(
            "PUT /connector-plugins/sh.oso.connect.oracle.OracleCdcSourceConnector/config/validate"
            " returned HTTP 404: Failed to find any class that implements Connector",
            404,
        )
    return api


def run_case(case: Path, tmp: Path, capsys) -> tuple[int, dict[str, str]]:
    args = json.loads((case / "args.json").read_text()) if (case / "args.json").exists() else {}
    source = next(case.glob("input.*"))
    spec = DEBEZIUM if case.parent.name == "debezium" else CONFLUENT
    output = args.get("output", "config.json")
    argv = [
        "--input",
        str(source),
        "--output",
        str(tmp / output),
        "--report",
        str(tmp / "report.md"),
        "--json",
        *args.get("argv", []),
    ]
    cwd = Path.cwd()
    os.chdir(tmp)
    try:
        argv = [a.replace(str(tmp) + os.sep, "") for a in argv]
        code = translator_main(spec, argv, api=connect_for(args.get("eos")))
    finally:
        os.chdir(cwd)
    files = {
        output: (tmp / output).read_text(),
        "report.md": (tmp / "report.md").read_text(),
        "report.json": capsys.readouterr().out,
    }
    return code, {k: v.replace(__version__, "<version>") for k, v in files.items()}


def compare(case: Path, files: dict[str, str]) -> None:
    expected_dir = case / "expected"
    if UPDATE:
        expected_dir.mkdir(exist_ok=True)
        for name, text in files.items():
            (expected_dir / name).write_text(text)
        return
    for name, text in files.items():
        expected = (expected_dir / name).read_text()
        assert text == expected, f"{case.name}/{name} differs; run with UPDATE_GOLDEN=1 to review"
