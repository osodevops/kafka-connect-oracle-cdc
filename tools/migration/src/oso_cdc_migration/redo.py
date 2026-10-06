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
"""Is the redo from the start SCN still there? (PRD-04 s4: refuse when an archived log is gone.)

For every redo thread the log that contains the start SCN, and every later log up to the current
online log, must be available: an online log, or an archived log not deleted, with status A, and
that LogMiner can open.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LogFile:
    thread: int
    sequence: int
    first_scn: int
    next_scn: int
    available: bool
    online: bool = False


@dataclass
class ThreadCoverage:
    thread: int
    first_sequence: int | None
    last_sequence: int | None
    missing: list[int]
    problem: str | None


@dataclass
class RedoCoverage:
    start_scn: int
    threads: list[ThreadCoverage]

    @property
    def ok(self) -> bool:
        return bool(self.threads) and all(t.problem is None for t in self.threads)

    def problems(self) -> list[str]:
        if not self.threads:
            return ["The database reported no redo logs."]
        return [t.problem for t in self.threads if t.problem]


def ranges(numbers: list[int], limit: int = 20) -> str:
    """1,2,3,5 as "1 to 3, 5"."""
    parts: list[str] = []
    start = prev = None
    for n in sorted(numbers):
        if start is None:
            start = prev = n
        elif n == prev + 1:
            prev = n
        else:
            parts.append(str(start) if start == prev else f"{start} to {prev}")
            start = prev = n
    if start is not None:
        parts.append(str(start) if start == prev else f"{start} to {prev}")
    if len(parts) > limit:
        return ", ".join(parts[:limit]) + f" and {len(parts) - limit} more"
    return ", ".join(parts)


def check_coverage(start_scn: int, logs: list[LogFile]) -> RedoCoverage:
    threads = sorted({log.thread for log in logs if log.online}) or sorted(
        {log.thread for log in logs}
    )
    out = []
    for thread in threads:
        mine = [log for log in logs if log.thread == thread]
        by_seq: dict[int, LogFile] = {}
        for log in mine:
            known = by_seq.get(log.sequence)
            if known is None or (log.available and not known.available):
                by_seq[log.sequence] = log
        last = max(by_seq)
        containing = [s for s, log in by_seq.items() if log.first_scn <= start_scn < log.next_scn]
        if not containing:
            earliest = min(log.first_scn for log in mine)
            problem = (
                f"Thread {thread}: no redo log containing SCN {start_scn} is recorded; the"
                f" earliest recorded log starts at SCN {earliest}."
                if earliest > start_scn
                else f"Thread {thread}: no redo log containing SCN {start_scn} is recorded."
            )
            out.append(ThreadCoverage(thread, None, last, [], problem))
            continue
        first = min(containing)
        missing = [s for s in range(first, last + 1) if s not in by_seq or not by_seq[s].available]
        problem = None
        if missing:
            problem = (
                f"Thread {thread}: the logs with sequence {ranges(missing)} are no longer"
                f" available (deleted, removed outside RMAN, or never archived to a readable"
                f" destination); the connector needs every log from sequence {first} on."
            )
        out.append(ThreadCoverage(thread, first, last, missing, problem))
    return RedoCoverage(start_scn, out)
