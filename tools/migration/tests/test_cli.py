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
"""Each script named in PRD-04 runs with --help and exits 1 on a usage error."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = [
    "migrate_from_debezium.py",
    "migrate_from_confluent.py",
    "takeover_scn.py",
    "verify_cutover.py",
]


def run(script: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ROOT / script), *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.parametrize("script", SCRIPTS)
def test_help(script):
    result = run(script, "--help")
    assert result.returncode == 0, result.stderr
    for option in ("--input", "--output", "--report", "--json"):
        assert option in result.stdout
    assert "exit codes:" in result.stdout


@pytest.mark.parametrize("script", SCRIPTS)
def test_usage_error_exits_one(script):
    result = run(script, "--definitely-not-an-option")
    assert result.returncode == 1
    assert "error:" in result.stderr


def test_missing_input_file_is_a_failure(tmp_path):
    result = run("migrate_from_debezium.py", "--input", str(tmp_path / "missing.json"))
    assert result.returncode == 1
    assert "Cannot read" in result.stderr
