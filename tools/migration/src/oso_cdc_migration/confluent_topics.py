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
"""Confluent's table topics as the old side of a cutover verification (PRD-04 VER-3).

Confluent's Oracle CDC Source connector writes flat rows. Observed with its version 2.15.2 on 12
October 2026: the value holds the table's columns plus ``table``, ``scn``, ``op_type`` (``I``,
``U``, ``D``, ``R`` for snapshot rows, ``T``), ``op_ts``, ``current_ts``, ``row_id`` and
``username``; the key is the primary key value for a one-column key and a struct otherwise. How a
NUMBER is written depends on ``numeric.mapping`` and the converter (a JSON converter writes a
Connect Decimal as base64 unless its ``decimal.format`` is ``NUMERIC``).

Verification from those topics is by key: Confluent's records are applied first and mark each key
they leave present; the new connector's records then overwrite or remove every key they touch.
A key only Confluent delivered counts as present when the database has it (its values are not
compared, since their encoding varies with Confluent's settings) and as only in the topics when
the database does not. Keys the new connector delivered are compared value by value as usual.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from typing import Any

from . import normalise
from .materialise import TableSpec, TableStats
from .normalise import Column, NormaliseError, Normaliser
from .records import ChangeRecord, KafkaRecord, RecordError, _decode, _fields, unwrap
from .rowstore import KEY_ONLY, RowStore, canonical

UPSERT = {"I", "U", "R"}


def _json(raw: bytes | None) -> Any:
    if raw is None:
        return None
    try:
        doc = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as e:
        raise RecordError(f"not JSON: {e}") from None
    if isinstance(doc, dict) and "payload" in doc and "schema" in doc:
        return doc["payload"]
    return doc


def _number(raw: Any, col: Column) -> Decimal:
    if isinstance(raw, bool):
        raise NormaliseError(f"not a number: {raw!r}")
    if isinstance(raw, (int, float)):
        return Decimal(repr(raw)) if isinstance(raw, float) else Decimal(raw)
    text = str(raw)
    try:
        return Decimal(text)
    except InvalidOperation:
        pass
    try:
        unscaled = int.from_bytes(base64.b64decode(text, validate=True), "big", signed=True)
    except (binascii.Error, ValueError):
        raise NormaliseError(f"not a number or a base64 decimal: {text!r}") from None
    return Decimal(unscaled).scaleb(-(col.scale or 0))


def key_value(raw: Any, col: Column, normaliser: Normaliser) -> str | None:
    if raw is None:
        return None
    if col.kind == "number":
        return normaliser.from_database(_number(raw, col), col)
    return normaliser.from_database(raw, col)


def record_key(rec: KafkaRecord, value: Any, spec: TableSpec, normaliser: Normaliser) -> str:
    raw = _json(rec.key)
    if isinstance(raw, dict):
        source = raw
    elif raw is not None and len(spec.key) == 1:
        source = {spec.key[0]: raw}
    elif isinstance(value, dict):
        source = value
    else:
        raise RecordError("the record carries no key")
    missing = [c for c in spec.key if c not in source]
    if missing:
        raise RecordError(f"the record key lacks {', '.join(missing)}")
    return canonical([(c, key_value(source[c], spec.column(c), normaliser)) for c in spec.key])


def apply_topic(
    records: Iterable[KafkaRecord],
    spec: TableSpec,
    store: RowStore,
    normaliser: Normaliser,
    check_scn: int,
    stats: TableStats,
) -> None:
    """Applies one Confluent table topic to the topic side of the row store, by key."""
    if not spec.key:
        raise RecordError(f"{spec.name} has no key; Confluent's topics are verified by key")
    for rec in records:
        where = f"{rec.topic}-{rec.partition}@{rec.offset}"
        stats.confluent_records += 1
        try:
            value = _json(rec.value)
            if value is None:  # a tombstone after a delete (emit.tombstone.on.delete)
                store.delete("kafka", spec.name, record_key(rec, None, spec, normaliser))
                continue
            if not isinstance(value, dict):
                raise RecordError("the value is not a JSON object")
            raw_scn = value.get("scn")
            if raw_scn not in (None, "") and int(raw_scn) > check_scn:
                stats.confluent_after_check_scn += 1
                continue
            op = str(value.get("op_type", "")).upper()
            if op == "T":
                store.clear("kafka", spec.name)
            elif op == "D":
                store.delete("kafka", spec.name, record_key(rec, value, spec, normaliser))
                stats.confluent_deletes += 1
            elif op in UPSERT:
                store.put("kafka", spec.name, record_key(rec, value, spec, normaliser), KEY_ONLY)
            else:
                stats.other += 1
        except (RecordError, NormaliseError, ValueError) as e:
            stats.error(where, str(e))


def resolve_key_only(store: RowStore, table: str) -> int:
    """Gives every key only Confluent delivered the database's row when the database has the key;
    returns how many were resolved. The rest stay and count as only in the topics."""
    cur = store.conn.execute(
        "UPDATE kafka SET row = (SELECT d.row FROM db d WHERE d.tbl = kafka.tbl AND d.k = kafka.k)"
        " WHERE tbl = ? AND row = ? AND EXISTS"
        " (SELECT 1 FROM db d WHERE d.tbl = kafka.tbl AND d.k = kafka.k)",
        (table, KEY_ONLY),
    )
    return cur.rowcount


class FlatNormaliser(Normaliser):
    """Column-specific Confluent numeric mappings and millisecond temporal values."""

    def __init__(self, settings: dict[str, str]) -> None:
        super().__init__()
        self.mapping = settings.get("numeric.mapping", "none")
        self.default_scale = int(settings.get("numeric.default.scale", "127"))
        self.date_mapping = settings.get("date.mapping", "date")

    def number_mapping(self, col: Column) -> str:
        if col.data_type.upper().startswith("FLOAT"):
            return "double"
        precision, scale = col.precision, col.scale
        if self.mapping == "none":
            return "decimal"
        integer = (
            precision is not None
            and precision > 0
            and scale is not None
            and (scale == 0 if self.mapping == "precision_only" else scale <= 0)
        )
        if integer and precision < 19:
            return "integer"
        if self.mapping == "precision_only":
            return "decimal"
        if precision is None or precision < 0 or (scale is not None and scale > 0):
            return "double"
        return {"best_fit_or_double": "double", "best_fit_or_string": "string"}.get(
            self.mapping, "decimal"
        )

    def from_database(self, value: Any, col: Column) -> str | None:
        if value is None:
            return None
        if col.kind == "number":
            decimal = Decimal(repr(value)) if isinstance(value, float) else Decimal(value)
            if self.number_mapping(col) == "double":
                decimal = Decimal(repr(float(decimal)))
            return normalise.number_text(decimal)
        text = super().from_database(value, col)
        if col.kind in ("date", "timestamp", "timestamp_tz"):
            y, mo, d, h, mi, sec, nanos, _ = normalise._parse_local(text)
            if col.kind == "date" and self.date_mapping == "date":
                h = mi = sec = nanos = 0
            else:
                nanos = nanos // 1_000_000 * 1_000_000
            render = normalise.instant_text if col.kind == "timestamp_tz" else normalise.local_text
            return render(y, mo, d, h, mi, sec, nanos)
        return text

    def from_record(self, value: Any, col: Column, schema: dict | None = None) -> str | None:
        if value is None:
            return None
        if col.kind == "number":
            mapping = self.number_mapping(col)
            if mapping == "decimal" and not schema:
                scale = col.scale if col.scale is not None else self.default_scale
                schema = {"name": normalise.DECIMAL, "parameters": {"scale": str(scale)}}
            if (
                mapping in ("string", "integer") or (schema and schema.get("type") == "string")
            ) and isinstance(value, str):
                return normalise.number_text(Decimal(value))
            decimal = self._record_decimal(value, schema)
            if mapping == "double":
                decimal = Decimal(repr(float(decimal)))
            return normalise.number_text(decimal)
        if col.kind in ("date", "timestamp", "timestamp_tz") and not isinstance(value, str):
            n = int(value)
            if col.kind == "date" and self.date_mapping == "date":
                n *= 86_400_000
            parts = normalise._epoch_parts(n, 1000)
            render = normalise.instant_text if col.kind == "timestamp_tz" else normalise.local_text
            return render(*parts)
        return super().from_record(value, col, schema)


class FlatParser:
    """Preserves JSON schemas and resolves custom metadata, scalar and concatenated keys."""

    def __init__(
        self, settings: dict[str, str], specs: list[TableSpec], baseline_scn: int | None = None
    ) -> None:
        self.settings = settings
        self.specs = specs
        self.baseline_scn = baseline_scn
        self.keys: dict[tuple[str, bytes], tuple[dict, dict, dict]] = {}

    def meta(self, value: dict, headers: dict[str, str], setting: str, default: str) -> Any:
        path = self.settings.get(setting, default)
        if path.startswith("header:"):
            raw = headers.get(path[7:])
            if raw is None:
                return None
            try:
                return unwrap(json.loads(raw))[0]
            except ValueError:
                return raw
        current: Any = value
        for part in path.split(".") if path else []:
            current = current.get(part) if isinstance(current, dict) else None
        return current if path else None

    def parse(self, rec: KafkaRecord) -> ChangeRecord:
        try:
            headers = {k: v.decode("utf-8") for k, v in rec.headers if v is not None}
        except UnicodeDecodeError:
            raise RecordError("a flat metadata header is not UTF-8") from None
        key, key_schema = unwrap(_decode(rec.key, "key")) if rec.key is not None else (None, None)
        value, schema = (
            unwrap(_decode(rec.value, "value")) if rec.value is not None else (None, None)
        )
        if value is not None and not isinstance(value, dict):
            raise RecordError("the flat record value is not a JSON object")
        value = value or {}
        table = self.meta(value, headers, "table.name.field", "table")
        candidates = [s for s in self.specs if s.topic == rec.topic]
        spec = next((s for s in candidates if table and str(table).endswith("." + s.name)), None)
        if spec is None and len(candidates) == 1:
            spec = candidates[0]
        source = {"schema": spec.owner, "table": spec.table} if spec else {}
        fields = _fields(schema)
        key_fields = _fields(key_schema)
        identity = (rec.topic, rec.key or b"")
        if not isinstance(key, dict) and spec:
            concatenated = self.settings.get("key.template") == "${primaryKey}"
            if key is None:
                raise RecordError("a flat row with a stable key carries a null key")
            if len(spec.key) == 1 and not concatenated:
                key = {spec.key[0]: key}
                if key_schema:
                    key_fields = {spec.key[0]: key_schema}
            elif rec.value is not None and all(k in value for k in spec.key):
                normaliser = FlatNormaliser(self.settings)
                expected = "_".join(
                    str(normaliser.from_record(value[k], spec.column(k), fields.get(k)))
                    for k in spec.key
                )
                if str(key) != expected:
                    raise RecordError("the concatenated key does not match the row's key columns")
                key = {k: value[k] for k in spec.key}
                key_fields = {k: fields[k] for k in spec.key if k in fields}
                self.keys[identity] = (key, key_fields, source)
            elif identity in self.keys:
                key, key_fields, source = self.keys[identity]
            elif len(spec.key) == 1:
                key = {spec.key[0]: key}
                key_fields = {spec.key[0]: {"type": "string"}}
            else:
                raise RecordError("a concatenated composite key has no preceding row to resolve it")
        source["scn"] = self.meta(value, headers, "scn.field", "scn")
        source["commit_scn"] = self.meta(value, headers, "commit.scn.field", "")
        if self.baseline_scn is not None and not any(k.startswith("cdc.") for k in headers):
            raw_scn = source.get("scn")
            if raw_scn is not None and int(raw_scn) > self.baseline_scn:
                raise RecordError("a Confluent row SCN exceeds the stopped baseline's commit fence")
            # STOPPED plus unchanged offsets proves every historical Confluent record is
            # committed before this fence. OSO rows retain their individual commit headers.
            source["commit_scn"] = self.baseline_scn
        raw_op = self.meta(value, headers, "op.type.field", "op_type")
        operations = {
            self.settings.get("op.type." + name + ".value", code): op
            for name, code, op in (
                ("insert", "I", "c"),
                ("update", "U", "u"),
                ("delete", "D", "d"),
                ("read", "R", "r"),
                ("truncate", "T", "t"),
            )
        }
        op = operations.get(raw_op)
        if rec.value is not None and op is None:
            raise RecordError("the flat record carries no recognised operation")
        before_path = self.settings.get("before.state.field", "")
        before = value.get(before_path) if before_path else None
        return ChangeRecord(
            rec.topic,
            rec.partition,
            rec.offset,
            key,
            key_fields,
            rec.value is None,
            op,
            value if op == "d" else before,
            value if op in ("c", "r", "u") else None,
            source,
            fields,
            headers,
        )
