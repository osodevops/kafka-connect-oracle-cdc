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
"""Fakes for the Connect REST API, the database and the topics: no live service is needed."""

from __future__ import annotations

import json
from typing import Any

from oso_cdc_migration.connect import ConnectError
from oso_cdc_migration.oracle import DatabaseIdentity, SnapshotTooOld
from oso_cdc_migration.records import KafkaRecord
from oso_cdc_migration.redo import LogFile


class FakeConnect:
    def __init__(self) -> None:
        self.states: dict[str, str] = {}
        self.offset_entries: dict[str, list[dict[str, Any]]] = {}
        self.validate_response: dict[str, Any] | None = None
        self.validate_error: ConnectError | None = None
        self.offset_reads = 0

    def status(self, connector: str) -> dict[str, Any]:
        if connector not in self.states:
            raise ConnectError(f"GET /connectors/{connector}/status returned HTTP 404", 404)
        return {"name": connector, "connector": {"state": self.states[connector]}}

    def offsets(self, connector: str) -> dict[str, Any]:
        self.offset_reads += 1
        return {"offsets": self.offset_entries.get(connector, [])}

    def validate(self, plugin: str, config: dict[str, str]) -> dict[str, Any]:
        if self.validate_error:
            raise self.validate_error
        return self.validate_response or {"configs": []}


def eos_response(errors: list[str]) -> dict[str, Any]:
    return {
        "configs": [
            {"value": {"name": "tasks.max", "errors": []}},
            {"value": {"name": "exactly.once.support", "value": "required", "errors": errors}},
        ]
    }


class FakeTakeoverDb:
    def __init__(
        self,
        logs: list[LogFile],
        dbid: int = 1234567890,
        resetlogs: int = 1,
        current: int = 9_000_000,
        log_mode: str = "ARCHIVELOG",
    ) -> None:
        self._identity = DatabaseIdentity(dbid, resetlogs, current, "ORCLCDB", log_mode)
        self._logs = logs
        self.calls: list[tuple[int, int]] = []

    def identity(self) -> DatabaseIdentity:
        return self._identity

    def logs(self, start_scn: int, resetlogs_scn: int) -> list[LogFile]:
        self.calls.append((start_scn, resetlogs_scn))
        return self._logs


def contiguous_logs(thread: int, first_seq: int, boundaries: list[int]) -> list[LogFile]:
    """Archived logs between consecutive boundaries, then the current online log."""
    out = []
    for i in range(len(boundaries) - 1):
        out.append(LogFile(thread, first_seq + i, boundaries[i], boundaries[i + 1], True))
    last = first_seq + len(boundaries) - 1
    out.append(LogFile(thread, last, boundaries[-1], 281474976710655, True, True))
    return out


class FakeVerifyDb:
    """Tables as they are AS OF the check SCN; the fake ignores the SCN itself."""

    def __init__(self, current_scn: int = 5000, database: str = "ORCLCDB") -> None:
        self.tables: dict[tuple[str, str], dict[str, Any]] = {}
        self._current = current_scn
        self._database = database
        self.reads: list[tuple[str, str, int, list[str]]] = []

    def add(self, owner, table, columns, key, rows, error: Exception | None = None):
        self.tables[(owner, table)] = {
            "columns": columns,
            "key": key,
            "rows": rows,
            "error": error,
        }

    def current_scn(self) -> int:
        return self._current

    def scn_seconds_ago(self, seconds: int) -> int:
        return self._current - seconds * 10

    def database_name(self) -> str:
        return self._database

    def container_name(self) -> str:
        return "ORCLPDB1"

    def columns(self, owner, table):
        t = self.tables.get((owner, table))
        return list(t["columns"]) if t else []

    def key_columns(self, owner, table):
        return list(self.tables[(owner, table)]["key"])

    def rows(self, owner, table, columns, key, scn, batch):
        t = self.tables[(owner, table)]
        self.reads.append((owner, table, scn, list(key)))
        if t["error"] is not None:
            raise t["error"]
        names = [c.name for c in columns]
        rows = [{n: r.get(n) for n in names} for r in t["rows"]]
        for i in range(0, len(rows), batch):
            yield rows[i : i + batch]


class FakeTopics:
    def __init__(self) -> None:
        self.records: dict[str, list[KafkaRecord]] = {}

    def add(self, record: KafkaRecord) -> None:
        self.records.setdefault(record.topic, []).append(record)

    def read(self, topic: str):
        yield from self.records.get(topic, [])


def _json(document: Any) -> bytes:
    return json.dumps(document).encode("utf-8")


class TopicWriter:
    """Writes change records as the connector (headers) or Debezium (source block only) does."""

    def __init__(self, topics: FakeTopics, topic: str, style: str = "oso", envelope=False):
        self.topics = topics
        self.topic = topic
        self.style = style
        self.envelope = envelope
        self.offset = 0
        self.value_schema: dict[str, Any] | None = None
        self.key_schema: dict[str, Any] | None = None

    def _wrap(self, payload: Any, schema: dict[str, Any] | None) -> Any:
        if self.envelope and schema is not None:
            return {"schema": schema, "payload": payload}
        return payload

    def change(
        self,
        op: str,
        key: dict | None,
        before: dict | None,
        after: dict | None,
        commit_scn: int | None,
        *,
        owner: str = "APP",
        table: str = "ORDERS",
        scn: int | None = None,
        partition: int = 0,
    ) -> None:
        source: dict[str, Any] = {"schema": owner, "table": table, "connector": "oracle"}
        headers: list[tuple[str, bytes | None]] = []
        if op == "r":
            source["snapshot"] = "true"
            source["scn"] = str(scn)
            if self.style == "oso":
                headers.append(("cdc.scn", str(scn).encode()))
        else:
            source["scn"] = str(scn or commit_scn)
            if commit_scn is not None:
                source["commit_scn"] = str(commit_scn)
                if self.style == "oso":
                    headers.append(("cdc.commit_scn", str(commit_scn).encode()))
        value = {"before": before, "after": after, "source": source, "op": op}
        self.topics.add(
            KafkaRecord(
                self.topic,
                partition,
                self.offset,
                None if key is None else _json(self._wrap(key, self.key_schema)),
                _json(self._wrap(value, self.value_schema)),
                headers,
            )
        )
        self.offset += 1

    def tombstone(self, key: dict, commit_scn: int | None = None, partition: int = 0) -> None:
        headers = []
        if commit_scn is not None and self.style == "oso":
            headers.append(("cdc.commit_scn", str(commit_scn).encode()))
        self.topics.add(
            KafkaRecord(
                self.topic,
                partition,
                self.offset,
                _json(self._wrap(key, self.key_schema)),
                None,
                headers,
            )
        )
        self.offset += 1


__all__ = [
    "FakeConnect",
    "FakeTakeoverDb",
    "FakeTopics",
    "FakeVerifyDb",
    "SnapshotTooOld",
    "TopicWriter",
    "contiguous_logs",
    "eos_response",
]
