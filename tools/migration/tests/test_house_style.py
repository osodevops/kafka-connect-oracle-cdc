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
"""Client-facing text follows the house style: no em or en dashes, arrow glyphs, tildes or emoji
in the tools' messages, the README or the migration pages."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT.parents[1] / "website" / "docs" / "migration"
FORBIDDEN = re.compile(
    "["
    + "".join(chr(c) for c in (0x2013, 0x2014))
    + chr(0x2190)
    + "-"
    + chr(0x21FF)
    + "~"
    + chr(0x2600)
    + "-"
    + chr(0x27BF)
    + chr(0x1F300)
    + "-"
    + chr(0x1FAFF)
    + "]"
)


def files() -> list[Path]:
    out = sorted((ROOT / "src").rglob("*.py")) + sorted(ROOT.glob("*.py"))
    out.append(ROOT / "README.md")
    if DOCS.exists():
        out += sorted(DOCS.glob("*.md"))
    return out


@pytest.mark.parametrize("path", files(), ids=lambda p: p.name)
def test_house_style(path):
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        match = FORBIDDEN.search(line)
        assert match is None, f"{path.name}:{number}: {match.group()!r} in {line.strip()!r}"
