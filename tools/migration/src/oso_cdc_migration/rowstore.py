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
"""Both sides of the comparison in one SQLite file, so tables of any size fit on disk.

Each side holds, per table, a canonical key and the canonical row (the normalised values of the
compared columns as JSON). Tables without a key use the whole row as the key and count copies.
The table hash is order-independent: the sum, modulo 2 to the power 256, of the SHA-256 of every
key and row, once per copy (PRD-04 VER-2, VER-3).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field

SIDES = ("db", "kafka")
_MOD = 1 << 256


def canonical(pairs: list[tuple[str, str | None]]) -> str:
    return json.dumps(pairs, separators=(",", ":"), ensure_ascii=False)


def row_digest(key: str, row: str) -> int:
    return int.from_bytes(hashlib.sha256((key + "\x1e" + row).encode("utf-8")).digest(), "big")


@dataclass
class Difference:
    key: str
    db_row: str | None
    kafka_row: str | None
    db_copies: int
    kafka_copies: int


@dataclass
class Comparison:
    db_rows: int
    kafka_rows: int
    db_hash: str
    kafka_hash: str
    missing_count: int = 0
    extra_count: int = 0
    mismatch_count: int = 0
    samples: list[Difference] = field(default_factory=list)

    @property
    def equal(self) -> bool:
        return (
            self.missing_count == 0
            and self.extra_count == 0
            and self.mismatch_count == 0
            and self.db_hash == self.kafka_hash
            and self.db_rows == self.kafka_rows
        )


class RowStore:
    def __init__(self, path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        for side in SIDES:
            self.conn.execute(f"DROP TABLE IF EXISTS {side}")
            self.conn.execute(
                f"CREATE TABLE {side} (tbl TEXT NOT NULL, k TEXT NOT NULL, row TEXT NOT NULL,"
                " n INTEGER NOT NULL, PRIMARY KEY (tbl, k)) WITHOUT ROWID"
            )

    def close(self) -> None:
        self.conn.close()

    def put(self, side: str, tbl: str, key: str, row: str) -> None:
        self.conn.execute(
            f"INSERT OR REPLACE INTO {side} (tbl, k, row, n) VALUES (?, ?, ?, 1)",
            (tbl, key, row),
        )

    def put_many(self, side: str, tbl: str, rows: list[tuple[str, str]]) -> None:
        self.conn.executemany(
            f"INSERT OR REPLACE INTO {side} (tbl, k, row, n) VALUES (?, ?, ?, 1)",
            [(tbl, k, r) for k, r in rows],
        )

    def get(self, side: str, tbl: str, key: str) -> str | None:
        found = self.conn.execute(
            f"SELECT row FROM {side} WHERE tbl = ? AND k = ?", (tbl, key)
        ).fetchone()
        return found[0] if found else None

    def delete(self, side: str, tbl: str, key: str) -> None:
        self.conn.execute(f"DELETE FROM {side} WHERE tbl = ? AND k = ?", (tbl, key))

    def count(self, side: str, tbl: str, row: str, delta: int) -> None:
        """A copy more or less of a row of a table without a key."""
        self.conn.execute(
            f"INSERT INTO {side} (tbl, k, row, n) VALUES (?, ?, ?, ?) ON CONFLICT (tbl, k)"
            " DO UPDATE SET n = n + excluded.n",
            (tbl, row, row, delta),
        )
        self.conn.execute(f"DELETE FROM {side} WHERE tbl = ? AND k = ? AND n = 0", (tbl, row))

    def count_many(self, side: str, tbl: str, rows: list[str]) -> None:
        for row in rows:
            self.count(side, tbl, row, 1)

    def clear(self, side: str, tbl: str) -> None:
        self.conn.execute(f"DELETE FROM {side} WHERE tbl = ?", (tbl,))

    def _summary(self, side: str, tbl: str) -> tuple[int, str]:
        total = 0
        acc = 0
        for k, row, n in self.conn.execute(f"SELECT k, row, n FROM {side} WHERE tbl = ?", (tbl,)):
            total += n
            acc = (acc + row_digest(k, row) * n) % _MOD
        return total, f"{acc:064x}"

    def compare(self, tbl: str, samples: int = 100) -> Comparison:
        db_rows, db_hash = self._summary("db", tbl)
        kafka_rows, kafka_hash = self._summary("kafka", tbl)
        out = Comparison(db_rows, kafka_rows, db_hash, kafka_hash)
        q = self.conn.execute
        out.missing_count = q(
            "SELECT COUNT(*) FROM db d LEFT JOIN kafka f ON f.tbl = d.tbl AND f.k = d.k"
            " WHERE d.tbl = ? AND f.k IS NULL",
            (tbl,),
        ).fetchone()[0]
        out.extra_count = q(
            "SELECT COUNT(*) FROM kafka f LEFT JOIN db d ON d.tbl = f.tbl AND d.k = f.k"
            " WHERE f.tbl = ? AND d.k IS NULL",
            (tbl,),
        ).fetchone()[0]
        out.mismatch_count = q(
            "SELECT COUNT(*) FROM db d JOIN kafka f ON f.tbl = d.tbl AND f.k = d.k"
            " WHERE d.tbl = ? AND (d.row <> f.row OR d.n <> f.n)",
            (tbl,),
        ).fetchone()[0]
        for sql in (
            "SELECT d.k, d.row, NULL, d.n, 0 FROM db d LEFT JOIN kafka f ON f.tbl = d.tbl"
            " AND f.k = d.k WHERE d.tbl = ? AND f.k IS NULL ORDER BY d.k LIMIT ?",
            "SELECT f.k, NULL, f.row, 0, f.n FROM kafka f LEFT JOIN db d ON d.tbl = f.tbl"
            " AND d.k = f.k WHERE f.tbl = ? AND d.k IS NULL ORDER BY f.k LIMIT ?",
            "SELECT d.k, d.row, f.row, d.n, f.n FROM db d JOIN kafka f ON f.tbl = d.tbl"
            " AND f.k = d.k WHERE d.tbl = ? AND (d.row <> f.row OR d.n <> f.n)"
            " ORDER BY d.k LIMIT ?",
        ):
            room = samples - len(out.samples)
            if room <= 0:
                break
            out.samples += [Difference(*r) for r in q(sql, (tbl, room))]
        return out
