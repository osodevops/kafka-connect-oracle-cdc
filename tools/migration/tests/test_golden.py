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
"""Golden-file tests for both translators (PRD-04 s6)."""

from __future__ import annotations

import json

import pytest

from .golden_support import cases, compare, run_case


@pytest.mark.parametrize("case", cases("debezium"), ids=lambda p: p.name)
def test_debezium_golden(case, tmp_path, capsys):
    code, files = run_case(case, tmp_path, capsys)
    compare(case, files)
    assert code == json.loads(files["report.json"])["exit_code"]


@pytest.mark.parametrize("case", cases("confluent"), ids=lambda p: p.name)
def test_confluent_golden(case, tmp_path, capsys):
    code, files = run_case(case, tmp_path, capsys)
    compare(case, files)
    assert code == json.loads(files["report.json"])["exit_code"]
