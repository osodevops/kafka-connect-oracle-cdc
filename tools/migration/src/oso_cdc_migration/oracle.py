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
"""Database access through python-oracledb (thin mode, no Oracle client needed).

Only this module imports ``oracledb``, and only when a tool connects, so the translators and
``--help`` work without it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .common import ToolError
from .configio import ConnectorConfig
from .redo import LogFile


@dataclass
class DatabaseIdentity:
    dbid: int
    resetlogs_scn: int
    current_scn: int
    name: str
    log_mode: str


def dsn_from_config(config: ConnectorConfig) -> str | None:
    """The DSN of the database a ``cdc.*`` configuration points at."""
    c = config.config
    url = (c.get("cdc.database.url") or "").strip()
    if url:
        return re.sub(r"^jdbc:oracle:[a-z]+:(?:[^@]*)@", "", url, flags=re.IGNORECASE)
    host = (c.get("cdc.database.host") or "").strip()
    if not host:
        return None
    port = (c.get("cdc.database.port") or "1521").strip()
    service = (c.get("cdc.database.service") or "").strip()
    if service:
        return f"{host}:{port}/{service}"
    sid = (c.get("cdc.database.sid") or "").strip()
    if sid:
        return (
            f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))"
            f"(CONNECT_DATA=(SID={sid})))"
        )
    return None


def connect(dsn: str, user: str, password: str) -> Any:
    try:
        import oracledb
    except ImportError as e:  # pragma: no cover - a dependency of the project
        raise ToolError("python-oracledb is not installed; run `uv sync`.") from e
    oracledb.defaults.fetch_lobs = False
    oracledb.defaults.fetch_decimals = True
    try:
        return oracledb.connect(user=user, password=password, dsn=dsn)
    except oracledb.Error as e:
        raise ToolError(f"Cannot connect to {dsn} as {user}: {e}") from None


def error_code(e: BaseException) -> int | None:
    args = getattr(e, "args", None)
    if args and hasattr(args[0], "code"):
        return int(args[0].code)
    return None


OPEN_LOG_END = 281474976710655


class OracleTakeoverDatabase:
    """The facts takeover_scn.py needs: identity, current SCN and the redo log inventory."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def identity(self) -> DatabaseIdentity:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT dbid, resetlogs_change#, current_scn, name, log_mode FROM v$database"
            )
            dbid, resetlogs, current, name, log_mode = cur.fetchone()
        return DatabaseIdentity(int(dbid), int(resetlogs), int(current), name, log_mode)

    def logs(self, start_scn: int, resetlogs_scn: int) -> list[LogFile]:
        out: list[LogFile] = []
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT thread#, sequence#, first_change#, next_change#, deleted, status"
                " FROM v$archived_log WHERE resetlogs_change# = :r AND next_change# > :s",
                r=resetlogs_scn,
                s=start_scn,
            )
            for thread, seq, first, nxt, deleted, status in cur:
                out.append(
                    LogFile(
                        int(thread),
                        int(seq),
                        int(first),
                        int(nxt),
                        deleted == "NO" and status == "A",
                    )
                )
            # one row per thread for the earliest log recorded at all, so a start SCN older
            # than every recorded log is reported as such
            cur.execute(
                "SELECT thread#, MIN(sequence#), MIN(first_change#) FROM v$archived_log"
                " WHERE resetlogs_change# = :r GROUP BY thread#",
                r=resetlogs_scn,
            )
            for thread, seq, first in cur:
                if int(first) > start_scn:
                    out.append(LogFile(int(thread), int(seq), int(first), int(first), False))
            cur.execute(
                "SELECT thread#, sequence#, first_change#, next_change#, status FROM v$log"
                " WHERE status <> 'UNUSED'"
            )
            for thread, seq, first, nxt, _status in cur:
                end = OPEN_LOG_END if nxt is None else int(nxt)  # the current log is open-ended
                out.append(LogFile(int(thread), int(seq), int(first), end, True, True))
        return out


SNAPSHOT_TOO_OLD = {1555, 8181, 1466}


class SnapshotTooOld(Exception):
    """ORA-01555 and its relatives: the flashback query cannot see the check SCN any more."""


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


_BATCHABLE_KEY_KINDS = {"number", "text", "date", "binary"}


class OracleVerifyDatabase:
    """Table metadata and flashback reads AS OF the check SCN, in key-range batches (VER-2)."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn
        with conn.cursor() as cur:
            cur.execute("ALTER SESSION SET NLS_SORT = BINARY NLS_COMP = BINARY")

    def _one(self, sql: str, **binds: Any) -> Any:
        with self.conn.cursor() as cur:
            cur.execute(sql, binds)
            row = cur.fetchone()
        return row[0] if row else None

    def current_scn(self) -> int:
        return int(self._one("SELECT current_scn FROM v$database"))

    def scn_seconds_ago(self, seconds: int) -> int:
        return int(
            self._one(
                "SELECT TIMESTAMP_TO_SCN(SYSTIMESTAMP - NUMTODSINTERVAL(:s, 'SECOND')) FROM dual",
                s=seconds,
            )
        )

    def database_name(self) -> str:
        return str(self._one("SELECT name FROM v$database"))

    def container_name(self) -> str:
        return str(self._one("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM dual"))

    def columns(self, owner: str, table: str):
        from .normalise import Column

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, data_precision, data_scale FROM all_tab_cols"
                " WHERE owner = :o AND table_name = :t AND hidden_column = 'NO'"
                " AND virtual_column = 'NO' ORDER BY column_id",
                o=owner,
                t=table,
            )
            return [
                Column(name, dtype, None if p is None else int(p), None if s is None else int(s))
                for name, dtype, p, s in cur
            ]

    def key_columns(self, owner: str, table: str) -> list[str]:
        """The primary key, else the first NOT NULL unique index, as the connector chooses."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT cc.column_name FROM all_constraints k JOIN all_cons_columns cc"
                " ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name"
                " WHERE k.owner = :o AND k.table_name = :t AND k.constraint_type = 'P'"
                " AND k.status = 'ENABLED' ORDER BY cc.position",
                o=owner,
                t=table,
            )
            pk = [r[0] for r in cur]
            if pk:
                return pk
            cur.execute(
                "SELECT i.index_name, ic.column_name FROM all_indexes i JOIN all_ind_columns ic"
                " ON ic.index_owner = i.owner AND ic.index_name = i.index_name"
                " WHERE i.table_owner = :o AND i.table_name = :t AND i.uniqueness = 'UNIQUE'"
                " AND i.status = 'VALID' AND NOT EXISTS (SELECT 1 FROM all_ind_columns x"
                " JOIN all_tab_cols tc ON tc.owner = x.table_owner AND tc.table_name ="
                " x.table_name AND tc.column_name = x.column_name WHERE x.index_owner = i.owner"
                " AND x.index_name = i.index_name AND tc.nullable = 'Y')"
                " ORDER BY i.index_name, ic.column_position",
                o=owner,
                t=table,
            )
            first = None
            cols: list[str] = []
            for index, column in cur:
                if first is None:
                    first = index
                if index != first:
                    break
                cols.append(column)
            return cols

    @staticmethod
    def _expression(col) -> str:
        q = _quote(col.name)
        kind = col.kind
        if kind == "date":
            return f"TO_CHAR({q}, 'YYYY-MM-DD\"T\"HH24:MI:SS')"
        if kind == "timestamp":
            return f"TO_CHAR({q}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')"
        if kind == "timestamp_tz":
            inner = (
                f"CAST({q} AS TIMESTAMP WITH TIME ZONE)" if "LOCAL" in col.data_type.upper() else q
            )
            return f"TO_CHAR(SYS_EXTRACT_UTC({inner}), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')"
        if kind in ("interval_ym", "interval_ds"):
            return f"TO_CHAR({q})"
        return q

    def rows(self, owner: str, table: str, columns, key: list[str], scn: int, batch: int):
        """Batches of {column: value}; each batch is one short flashback query."""
        import oracledb

        source = f"{_quote(owner)}.{_quote(table)} AS OF SCN :scn"
        select = ", ".join(self._expression(c) for c in columns)
        kinds = {c.name: c.kind for c in columns}
        if key and all(kinds.get(k) in _BATCHABLE_KEY_KINDS for k in key):
            order = [_quote(k) for k in key]
            extra = [f"{_quote(k)} AS k{i}" for i, k in enumerate(key)]
        elif not key:
            order = ["ROWID"]
            extra = ["ROWIDTOCHAR(ROWID) AS k0"]
        else:
            order = []
            extra = []
        try:
            with self.conn.cursor() as cur:
                cur.arraysize = batch
                cur.prefetchrows = batch + 1
                if not order:  # a key type that cannot be bound back exactly: one query
                    cur.execute(f"SELECT {select} FROM {source}", scn=scn)
                    while rows := cur.fetchmany(batch):
                        yield [dict(zip([c.name for c in columns], r, strict=True)) for r in rows]
                    return
                last: list[Any] | None = None
                while True:
                    binds: dict[str, Any] = {"scn": scn, "n": batch}
                    where = ""
                    if last is not None:
                        terms = []
                        for i in range(len(order)):
                            eq = [f"{order[j]} = :l{j}" for j in range(i)]
                            target = "CHARTOROWID(:l0)" if order[i] == "ROWID" else f":l{i}"
                            terms.append("(" + " AND ".join([*eq, f"{order[i]} > {target}"]) + ")")
                        where = " WHERE " + " OR ".join(terms)
                        binds.update({f"l{i}": v for i, v in enumerate(last)})
                    cur.execute(
                        f"SELECT {select}, {', '.join(extra)} FROM {source}{where}"
                        f" ORDER BY {', '.join(order)} FETCH FIRST :n ROWS ONLY",
                        binds,
                    )
                    rows = cur.fetchall()
                    if not rows:
                        return
                    n = len(columns)
                    yield [dict(zip([c.name for c in columns], r[:n], strict=True)) for r in rows]
                    last = list(rows[-1][n:])
                    if len(rows) < batch:
                        return
        except oracledb.DatabaseError as e:
            if error_code(e) in SNAPSHOT_TOO_OLD:
                raise SnapshotTooOld(str(e)) from None
            raise
