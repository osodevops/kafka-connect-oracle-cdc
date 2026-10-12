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

from .materialise import TableSpec, TableStats
from .normalise import Column, NormaliseError, Normaliser
from .records import KafkaRecord, RecordError
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
