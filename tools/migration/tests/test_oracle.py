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
"""The redo inventory the takeover reads: a log the catalog lists but LogMiner cannot open."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from oso_cdc_migration.common import ToolError
from oso_cdc_migration.oracle import OPEN_LOG_END, OracleTakeoverDatabase
from oso_cdc_migration.redo import check_coverage


class DriverError(Exception):
    """Shaped like python-oracledb's DatabaseError: args[0] carries the ORA code."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(SimpleNamespace(code=code, message=message))


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn
        self.rows: list[tuple] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc) -> None:
        return None

    def __iter__(self):
        return iter(self.rows)

    def execute(self, sql: str, **binds) -> None:
        self.conn.executed.append((sql, binds))
        self.rows = []
        if "ADD_LOGFILE" in sql:
            error = self.conn.add_errors.get(binds["n"])
            if error is not None:
                raise error
            self.conn.added.append((binds["n"], "NEW" if "NEW" in sql else "ADDFILE"))
        elif "END_LOGMNR" in sql:
            self.conn.ended += 1
        elif "FROM v$archived_log WHERE resetlogs_change# = :r AND next_change#" in sql:
            self.rows = list(self.conn.archived)
        elif "GROUP BY thread#" in sql:
            self.rows = [(1, 10, 1000)]
        elif "FROM v$log" in sql:
            self.rows = [(1, 13, 1300, None, "CURRENT")]


class FakeConn:
    def __init__(self, archived: list[tuple], add_errors: dict[str, Exception]) -> None:
        self.archived = archived
        self.add_errors = add_errors
        self.executed: list[tuple[str, dict]] = []
        self.added: list[tuple[str, str]] = []
        self.ended = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


def archived(seq: int, first: int, nxt: int, name: str, deleted="NO", status="A") -> tuple:
    return (1, seq, first, nxt, deleted, status, name)


LOGS = [
    archived(10, 1000, 1100, "/arch/1_10.arc"),
    archived(11, 1100, 1200, "/arch/1_11.arc"),
    archived(12, 1200, 1300, "/arch/1_12.arc"),
]


def test_every_listed_log_is_opened_and_the_session_ended():
    conn = FakeConn(LOGS, {})
    logs = OracleTakeoverDatabase(conn).logs(1050, 1)
    assert [log.available for log in logs if not log.online] == [True, True, True]
    assert conn.added == [
        ("/arch/1_10.arc", "NEW"),
        ("/arch/1_11.arc", "ADDFILE"),
        ("/arch/1_12.arc", "ADDFILE"),
    ]
    assert conn.ended == 1
    assert check_coverage(1050, logs).ok


def test_a_log_moved_aside_is_not_available():
    missing = DriverError(1284, "ORA-01284: file /arch/1_11.arc cannot be opened")
    conn = FakeConn(LOGS, {"/arch/1_11.arc": missing})
    logs = OracleTakeoverDatabase(conn).logs(1050, 1)
    assert [log.available for log in logs if not log.online] == [True, False, True]
    coverage = check_coverage(1050, logs)
    assert not coverage.ok
    assert coverage.threads[0].missing == [11]
    assert "removed outside RMAN" in coverage.problems()[0]
    assert conn.ended == 1


def test_another_copy_of_the_same_log_is_enough():
    copies = [
        archived(10, 1000, 1100, "/arch/1_10.arc"),
        archived(10, 1000, 1100, "/fra/1_10.arc"),
        *LOGS[1:],
    ]
    missing = DriverError(308, "ORA-00308: cannot open archived log '/arch/1_10.arc'")
    conn = FakeConn(copies, {"/arch/1_10.arc": missing})
    logs = OracleTakeoverDatabase(conn).logs(1050, 1)
    assert [log.available for log in logs if not log.online] == [False, True, True, True]
    assert conn.added[0] == ("/fra/1_10.arc", "NEW")
    assert check_coverage(1050, logs).ok


def test_deleted_logs_are_not_opened():
    conn = FakeConn([archived(10, 1000, 1100, "/arch/1_10.arc", deleted="YES")], {})
    logs = OracleTakeoverDatabase(conn).logs(1050, 1)
    assert conn.added == []
    assert conn.ended == 0
    assert not logs[0].available
    assert logs[-1].online and logs[-1].next_scn == OPEN_LOG_END


def test_a_probe_that_cannot_run_stops_the_check():
    denied = DriverError(6550, "ORA-06550: PLS-00201: identifier 'DBMS_LOGMNR' must be declared")
    conn = FakeConn(LOGS, {"/arch/1_10.arc": denied})
    with pytest.raises(ToolError, match="EXECUTE on DBMS_LOGMNR"):
        OracleTakeoverDatabase(conn).logs(1050, 1)


def test_the_session_is_ended_when_a_later_probe_fails():
    denied = DriverError(1031, "ORA-01031: insufficient privileges")
    conn = FakeConn(LOGS, {"/arch/1_12.arc": denied})
    with pytest.raises(ToolError):
        OracleTakeoverDatabase(conn).logs(1050, 1)
    assert conn.ended == 1
