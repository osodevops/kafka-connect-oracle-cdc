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
"""Rewriting table filters into ``cdc.tables.include`` and ``cdc.tables.exclude`` patterns.

The connector matches each pattern against the whole of ``PDB.SCHEMA.TABLE`` in a CDB, or
``SCHEMA.TABLE`` otherwise, case-insensitively unless ``cdc.tables.case.sensitive=true``.
"""

from __future__ import annotations

import re

from .translate import Manual

_SEPARATORS = ("\\.", "[.]", ".")
_WILDCARD_SEGMENTS = (
    ".*",
    ".+",
    ".*?",
    ".+?",
    "[^.]*",
    "[^.]+",
    "\\w+",
    "\\w*",
    "[A-Z0-9_]+",
    "[A-Za-z0-9_]+",
    "[A-Z0-9_$#]+",
)


def split_list(value: str, key: str) -> list[str]:
    """A comma-separated list of regular expressions, as Debezium reads it."""
    if "\\," in value:
        raise Manual(
            f"`{key}` contains an escaped comma. The connector splits its pattern lists at every"
            " comma, so rewrite the pattern without a comma (for example `{1,3}` as an"
            " alternation) and set it by hand."
        )
    return [p.strip() for p in value.split(",") if p.strip()]


def strip_anchors(pattern: str) -> str:
    if pattern.startswith("^"):
        pattern = pattern[1:]
    if pattern.endswith("$") and not pattern.endswith("\\$"):
        pattern = pattern[:-1]
    return pattern


def group(pattern: str) -> str:
    return f"(?:{pattern})" if "|" in pattern else pattern


def top_level_alternatives(pattern: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_class = False
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "\\":
            current.append(pattern[i : i + 2])
            i += 2
            continue
        if in_class:
            if c == "]":
                in_class = False
        elif c == "[":
            in_class = True
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "|" and depth == 0:
            parts.append("".join(current))
            current = []
            i += 1
            continue
        current.append(c)
        i += 1
    parts.append("".join(current))
    return parts


def _has_separator(pattern: str) -> bool:
    return any(sep in pattern for sep in _SEPARATORS)


def strip_first_segment(pattern: str, names: list[str], wildcards: bool) -> str | None:
    """Removes a leading database segment, given literally or as a wildcard, with its dot."""
    candidates = [n for n in names if n]
    if wildcards:
        candidates += list(_WILDCARD_SEGMENTS)
    for name in sorted(candidates, key=len, reverse=True):
        for sep in _SEPARATORS:
            head = name + sep
            if pattern.upper().startswith(head.upper()):
                rest = pattern[len(head) :]
                if rest and _has_separator(rest):
                    return rest
    return None


def pdb_prefix(pdb: str | None) -> str:
    return re.escape(pdb) + "\\." if pdb else ""
