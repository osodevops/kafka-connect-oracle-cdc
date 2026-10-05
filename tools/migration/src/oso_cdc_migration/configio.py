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
"""Reading and writing connector configurations: Connect REST JSON or Java properties."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .common import ToolError


@dataclass
class ConnectorConfig:
    name: str | None
    config: dict[str, str]


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list | tuple):
        return ",".join(stringify(v) for v in value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return str(value)


def from_json(document: Any) -> ConnectorConfig:
    """Accepts a create body ({name, config}), GET /connectors/{name} output or a flat map."""
    if not isinstance(document, dict):
        raise ToolError("The configuration must be a JSON object.")
    if isinstance(document.get("config"), dict):
        config = {str(k): stringify(v) for k, v in document["config"].items()}
        name = document.get("name") or config.get("name")
    else:
        config = {str(k): stringify(v) for k, v in document.items()}
        name = config.get("name")
    return ConnectorConfig(name=name or None, config=config)


def load(path: str | Path) -> ConnectorConfig:
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as e:
        raise ToolError(f"Cannot read {p}: {e.strerror}.") from e
    if p.suffix.lower() == ".properties" or not text.lstrip().startswith("{"):
        config = parse_properties(text)
        return ConnectorConfig(name=config.get("name") or None, config=config)
    try:
        return from_json(json.loads(text))
    except json.JSONDecodeError as e:
        raise ToolError(f"{p} is not valid JSON: {e.msg} at line {e.lineno}.") from e


def _logical_lines(text: str):
    pending = ""
    for raw in text.splitlines():
        line = raw.lstrip() if pending else raw
        if not pending and (not line.strip() or line.lstrip()[0] in "#!"):
            continue
        trailing = len(line) - len(line.rstrip("\\"))
        if trailing % 2 == 1:
            pending += line[:-1]
            continue
        yield (pending + line).lstrip()
        pending = ""
    if pending:
        yield pending.lstrip()


def _unescape(s: str) -> str:
    out = []
    i = 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            n = s[i + 1]
            if n == "u" and i + 5 < len(s):
                try:
                    out.append(chr(int(s[i + 2 : i + 6], 16)))
                    i += 6
                    continue
                except ValueError:
                    pass
            out.append({"t": "\t", "n": "\n", "r": "\r", "f": "\f"}.get(n, n))
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def parse_properties(text: str) -> dict[str, str]:
    """Java properties: key=value, key:value or key value; # and ! comments; continuations."""
    out: dict[str, str] = {}
    for line in _logical_lines(text):
        i = 0
        while i < len(line):
            c = line[i]
            if c == "\\":
                i += 2
                continue
            if c in "=: \t\f":
                break
            i += 1
        key = line[:i]
        rest = line[i:].lstrip(" \t\f")
        if rest[:1] in ("=", ":"):
            rest = rest[1:].lstrip(" \t\f")
        out[_unescape(key)] = _unescape(rest)
    return out


def _escape(s: str, key: bool) -> str:
    out = []
    for i, c in enumerate(s):
        if c == "\\":
            out.append("\\\\")
        elif (key and c in "=:#! ") or (not key and i == 0 and c in " #!"):
            out.append("\\" + c)
        elif c == "\n":
            out.append("\\n")
        elif c == "\t":
            out.append("\\t")
        else:
            out.append(c)
    return "".join(out)


def format_properties(config: dict[str, str]) -> str:
    return "".join(f"{_escape(k, True)}={_escape(v, False)}\n" for k, v in config.items())
