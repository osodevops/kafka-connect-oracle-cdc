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
"""Applying change records to the topic side of the row store (PRD-04 VER-3).

Records apply in partition order, by key, up to those whose commit SCN is at or below the check
SCN; tombstones remove their key. A field carrying the unavailable placeholder keeps the row's
previous value, as a consumer must (SRC-LOB-1).
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field

from .normalise import Column, NormaliseError, Normaliser
from .records import ChangeRecord, RecordError
from .rowstore import RowStore, canonical


@dataclass
class TableSpec:
    owner: str
    table: str
    topic: str
    columns: list[Column]
    key: list[str]
    all_columns: list[Column] = field(default_factory=list)
    ignored: dict[str, str] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return f"{self.owner}.{self.table}"

    def column(self, name: str) -> Column:
        for c in self.all_columns or self.columns:
            if c.name == name:
                return c
        raise RecordError(f"{self.name} has no column {name}")


@dataclass
class TableStats:
    records: int = 0
    applied: int = 0
    tombstones: int = 0
    after_check_scn: int = 0
    snapshot_after_check_scn: int = 0
    without_commit_scn: int = 0
    other: int = 0
    error_count: int = 0
    errors: list[str] = field(default_factory=list)

    def error(self, where: str, message: str) -> None:
        self.error_count += 1
        if len(self.errors) < 10:
            self.errors.append(f"{where}: {message}")


@dataclass
class _Last:
    scn: int | None
    spec: TableSpec | None
    applied: bool


class Materialiser:
    def __init__(
        self,
        store: RowStore,
        normaliser: Normaliser,
        check_scn: int,
        placeholder: str,
        specs: list[TableSpec],
    ) -> None:
        self.store = store
        self.normaliser = normaliser
        self.check_scn = check_scn
        self.placeholders = {
            placeholder,
            base64.b64encode(placeholder.encode("utf-8")).decode("ascii"),
        }
        self.by_topic: dict[str, list[TableSpec]] = {}
        for s in specs:
            self.by_topic.setdefault(s.topic, []).append(s)
        self.stats = {s.name: TableStats() for s in specs}
        self._last: dict[tuple[str, int], _Last] = {}

    def _spec_for(self, rec: ChangeRecord) -> TableSpec | None:
        specs = self.by_topic.get(rec.topic, [])
        schema, table = rec.schema_table
        for s in specs:
            if s.owner == schema and s.table == table:
                return s
        if len(specs) == 1 and schema is None and table is None:
            return specs[0]
        return None

    def apply(self, rec: ChangeRecord) -> None:
        where = f"{rec.topic}-{rec.partition}@{rec.offset}"
        part = (rec.topic, rec.partition)
        if rec.tombstone:
            self._tombstone(rec, part, where)
            return
        spec = self._spec_for(rec)
        if spec is None:
            self._last[part] = _Last(None, None, False)
            return
        stats = self.stats[spec.name]
        stats.records += 1
        if rec.op not in ("c", "r", "u", "d", "t"):
            stats.other += 1
            return
        scn = rec.commit_scn()
        if scn is None:
            stats.without_commit_scn += 1
            self._last[part] = _Last(None, spec, False)
            return
        if scn > self.check_scn:
            stats.after_check_scn += 1
            if rec.op == "r":
                stats.snapshot_after_check_scn += 1
            self._last[part] = _Last(scn, spec, False)
            return
        try:
            self._apply_change(spec, rec)
        except (NormaliseError, RecordError) as e:
            stats.error(where, str(e))
            self._last[part] = _Last(scn, spec, False)
            return
        stats.applied += 1
        self._last[part] = _Last(scn, spec, True)

    def _tombstone(self, rec: ChangeRecord, part: tuple[str, int], where: str) -> None:
        last = self._last.get(part)
        specs = self.by_topic.get(rec.topic, [])
        spec = specs[0] if len(specs) == 1 else (last.spec if last else None)
        if spec is None:
            return
        stats = self.stats[spec.name]
        stats.tombstones += 1
        scn = rec.commit_scn()
        if scn is None:
            if last is None or last.spec is not spec:
                stats.without_commit_scn += 1
                return
            scn = last.scn
            if scn is None:
                stats.without_commit_scn += 1
                return
        if scn > self.check_scn or not spec.key:
            return
        try:
            self.store.delete("kafka", spec.name, self._key(spec, rec, None))
        except (NormaliseError, RecordError) as e:
            stats.error(where, str(e))

    def _apply_change(self, spec: TableSpec, rec: ChangeRecord) -> None:
        table = spec.name
        if rec.op == "t":
            self.store.clear("kafka", table)
            return
        if not spec.key:
            if rec.op in ("u", "d") and rec.before is not None:
                self.store.count("kafka", table, self._row(spec, rec, rec.before, None), -1)
            if rec.op in ("c", "r", "u") and rec.after is not None:
                self.store.count("kafka", table, self._row(spec, rec, rec.after, None), 1)
            return
        if rec.op == "d":
            self.store.delete("kafka", table, self._key(spec, rec, rec.before))
            return
        if rec.after is None:
            raise RecordError(f"an {rec.op} record without an after image")
        key = self._key(spec, rec, rec.after)
        previous = self.store.get("kafka", table, key)
        self.store.put("kafka", table, key, self._row(spec, rec, rec.after, previous))

    def _row(self, spec: TableSpec, rec: ChangeRecord, image: dict, previous: str | None) -> str:
        before = dict(json.loads(previous)) if previous else {}
        pairs: list[tuple[str, str | None]] = []
        for col in spec.columns:
            raw = image.get(col.name)
            if isinstance(raw, str) and raw in self.placeholders:
                pairs.append((col.name, before.get(col.name)))
                continue
            value = self.normaliser.from_record(raw, col, rec.row_fields.get(col.name))
            pairs.append((col.name, value))
        return canonical(pairs)

    def _key(self, spec: TableSpec, rec: ChangeRecord, image: dict | None) -> str:
        if isinstance(rec.key, dict) and all(c in rec.key for c in spec.key):
            source, fields = rec.key, rec.key_fields
        elif image is not None:
            source, fields = image, rec.row_fields
        else:
            raise RecordError(
                f"the record key does not carry the key columns {', '.join(spec.key)}"
            )
        pairs = [
            (c, self.normaliser.from_record(source.get(c), spec.column(c), fields.get(c)))
            for c in spec.key
        ]
        return canonical(pairs)
