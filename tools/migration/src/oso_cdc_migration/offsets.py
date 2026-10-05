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
"""The old connector's committed offset, read through Connect's offsets API (KIP-875), and the
start SCN of the new connector (PRD-04 s4).

The new connector starts at ``cdc.start.scn`` because it has no stored offset of its own.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .common import ToolError

_NUMBER = re.compile(r"^\s*(\d+)\s*$")


@dataclass
class OldPosition:
    source: str  # debezium or confluent
    start_scn: int
    start_field: str
    commit_scns: list[int] = field(default_factory=list)
    commit_scn_raw: str | None = None
    entries: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def last_commit_scn(self) -> int | None:
        return max(self.commit_scns) if self.commit_scns else None


def _scn(value: Any, what: str) -> int:
    if isinstance(value, bool):
        raise ToolError(f"{what} is not an SCN: {value!r}.")
    if isinstance(value, int):
        return value
    m = _NUMBER.match(str(value))
    if not m:
        raise ToolError(f"{what} is not an SCN: {value!r}.")
    return int(m.group(1))


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes", "initial")


def entries_of(document: Any) -> list[dict[str, Any]]:
    """The non-null partition and offset pairs of a GET /connectors/{name}/offsets response."""
    raw = document.get("offsets") if isinstance(document, dict) else document
    if not isinstance(raw, list):
        raise ToolError(
            "The offsets document has no `offsets` list; pass the output of"
            " GET /connectors/{name}/offsets."
        )
    out = []
    for e in raw:
        if not isinstance(e, dict) or not isinstance(e.get("partition"), dict):
            raise ToolError("Every entry of `offsets` needs a `partition` object.")
        if isinstance(e.get("offset"), dict) and e["offset"]:
            out.append(e)
    return out


def detect_source(entries: list[dict[str, Any]]) -> str:
    keys = {k for e in entries for k in e["partition"]}
    if "sidPdb" in keys:
        return "confluent"
    if "server" in keys:
        return "debezium"
    raise ToolError(
        "Cannot tell which connector wrote these offsets (partition keys "
        + ", ".join(sorted(keys))
        + "); pass --source."
    )


def parse_commit_scn(raw: Any) -> list[int]:
    """Debezium 1.x stored a plain number; 2.x and later one ``scn:...`` entry per thread."""
    if raw is None:
        return []
    out = []
    for part in str(raw).split(","):
        head = part.split(":", 1)[0].strip()
        if head.isdigit():
            out.append(int(head))
    return out


def _pending_scns(raw: Any) -> list[int]:
    """Debezium's transactions open at the snapshot start; the lowest number per entry is taken,
    which can only move the start back (more duplicates, never a gap)."""
    out = []
    for part in str(raw).split(","):
        if not part.strip():
            continue
        numeric = [int(x.strip()) for x in part.split(":") if x.strip().isdigit()]
        if not numeric:
            raise ToolError(
                f"The offset's `snapshot_pending_tx` entry `{part.strip()}` carries no SCN the"
                " tool can read. Inspect the offset and choose the start position by hand."
            )
        out.append(min(numeric))
    return out


def parse_debezium(entries: list[dict[str, Any]]) -> OldPosition:
    scns: list[int] = []
    commits: list[int] = []
    raws: list[str] = []
    notes: list[str] = []
    for e in entries:
        off = e["offset"]
        if "scn" not in off or off["scn"] in (None, ""):
            if "lcr_position" in off:
                raise ToolError(
                    "The Debezium offset holds an XStream LCR position and no SCN. Take the"
                    " start SCN from the XStream outbound server by hand."
                )
            raise ToolError(f"The Debezium offset has no `scn`: {sorted(off)}.")
        if _truthy(off.get("snapshot", False)) and not _truthy(
            off.get("snapshot_completed", False)
        ):
            raise ToolError(
                "The Debezium connector stopped inside its initial snapshot, so its offset is"
                " not a streaming position. Let the snapshot finish, or start the new connector"
                " with its own snapshot instead of a takeover."
            )
        scn = _scn(off["scn"], "The Debezium offset's `scn`")
        pending = off.get("snapshot_pending_tx")
        if pending not in (None, ""):
            low = _pending_scns(pending)
            if low and min(low) < scn:
                notes.append(
                    f"Transactions open at the snapshot start before SCN {scn}; the start moved"
                    f" back to {min(low)}."
                )
                scn = min(low)
        scns.append(scn)
        if off.get("commit_scn") not in (None, ""):
            raws.append(str(off["commit_scn"]))
            commits += parse_commit_scn(off["commit_scn"])
    start = min(scns)
    if len(scns) > 1:
        notes.append(f"{len(scns)} offset partitions; the lowest `scn` is used.")
    return OldPosition(
        source="debezium",
        start_scn=start,
        start_field="scn (the resume position, the low watermark of open transactions)",
        commit_scns=commits,
        commit_scn_raw=",".join(raws) or None,
        entries=entries,
        notes=notes,
    )


def parse_confluent(entries: list[dict[str, Any]]) -> OldPosition:
    scns = []
    for e in entries:
        off = e["offset"]
        if "scn" in off and off["scn"] not in (None, ""):
            scns.append(_scn(off["scn"], "The Confluent offset's `scn`"))
    if not scns:
        raise ToolError("No Confluent offset partition carries an `scn`.")
    notes = []
    if len(scns) > 1:
        notes.append(f"{len(scns)} offset partitions carry an SCN; the lowest is used.")
    return OldPosition(
        source="confluent",
        start_scn=min(scns),
        start_field="scn (transactions that began at or after it were still to be delivered)",
        entries=entries,
        notes=notes,
    )


def parse_offsets(document: Any, source: str = "auto") -> OldPosition:
    entries = entries_of(document)
    if not entries:
        raise ToolError(
            "The old connector has no committed offset, so there is no position to take over"
            " from. Start the new connector with a snapshot instead."
        )
    kind = detect_source(entries) if source == "auto" else source
    return parse_debezium(entries) if kind == "debezium" else parse_confluent(entries)
