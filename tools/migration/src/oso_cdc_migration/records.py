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
"""Change records as they sit in a topic: JSON written by Connect's JsonConverter, with or
without the schema envelope, from this connector or from the Debezium Oracle connector."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]] = field(default_factory=list)


class RecordError(Exception):
    pass


@dataclass
class ChangeRecord:
    topic: str
    partition: int
    offset: int
    key: Any
    key_fields: dict[str, dict]
    tombstone: bool
    op: str | None
    before: dict | None
    after: dict | None
    source: dict
    row_fields: dict[str, dict]
    headers: dict[str, str]

    @property
    def schema_table(self) -> tuple[str | None, str | None]:
        return self.source.get("schema"), self.source.get("table")

    def commit_scn(self) -> int | None:
        """``cdc.commit_scn`` header, else ``source.commit_scn``; a snapshot read (``op=r``)
        has no commit and counts at its read SCN (``cdc.scn`` header or ``source.scn``)."""
        for raw in (self.headers.get("cdc.commit_scn"), self.source.get("commit_scn")):
            scn = _scn(raw)
            if scn is not None:
                return scn
        if self.op == "r":
            for raw in (self.headers.get("cdc.scn"), self.source.get("scn")):
                scn = _scn(raw)
                if scn is not None:
                    return scn
        return None


def _scn(raw: Any) -> int | None:
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, Decimal):
        return int(raw)
    text = str(raw).strip()
    if text.isdigit():
        return int(text)
    try:  # a header written by a JSON header converter, with or without a schema
        parsed = unwrap(json.loads(text, parse_float=Decimal))[0]
    except ValueError:
        return None
    return _scn(parsed) if not isinstance(parsed, str) else None


def unwrap(document: Any) -> tuple[Any, dict | None]:
    """A JsonConverter with schemas enabled wraps the payload as {schema, payload}."""
    if isinstance(document, dict) and set(document) == {"schema", "payload"}:
        schema = document["schema"] if isinstance(document["schema"], dict) else None
        return document["payload"], schema
    return document, None


def _fields(schema: dict | None) -> dict[str, dict]:
    if not schema:
        return {}
    return {f["field"]: f for f in schema.get("fields", []) if isinstance(f, dict) and "field" in f}


def _decode(data: bytes, what: str) -> Any:
    try:
        return json.loads(data.decode("utf-8"), parse_float=Decimal)
    except (UnicodeDecodeError, ValueError):
        raise RecordError(
            f"the record {what} is not JSON; verify_cutover reads topics written with the JSON"
            " converter"
        ) from None


def parse(r: KafkaRecord) -> ChangeRecord:
    headers = {}
    for name, raw in r.headers:
        if raw is not None:
            headers[name] = raw.decode("utf-8", errors="replace")
    key, key_schema = unwrap(_decode(r.key, "key")) if r.key is not None else (None, None)
    if r.value is None:
        return ChangeRecord(
            r.topic,
            r.partition,
            r.offset,
            key,
            _fields(key_schema),
            True,
            None,
            None,
            None,
            {},
            {},
            headers,
        )
    value, value_schema = unwrap(_decode(r.value, "value"))
    if not isinstance(value, dict):
        raise RecordError("the record value is not a JSON object")
    envelope = _fields(value_schema)
    row_fields = _fields(envelope.get("after")) or _fields(envelope.get("before"))
    before = value.get("before") if isinstance(value.get("before"), dict) else None
    after = value.get("after") if isinstance(value.get("after"), dict) else None
    source = value.get("source") if isinstance(value.get("source"), dict) else {}
    op = value.get("op") if isinstance(value.get("op"), str) else None
    return ChangeRecord(
        r.topic,
        r.partition,
        r.offset,
        key,
        _fields(key_schema),
        False,
        op,
        before,
        after,
        source,
        row_fields,
        headers,
    )
