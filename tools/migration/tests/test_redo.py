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
from __future__ import annotations

from oso_cdc_migration.redo import LogFile, check_coverage, ranges

from .fakes import contiguous_logs


def test_all_logs_from_the_start_are_available():
    logs = contiguous_logs(1, 10, [1000, 2000, 3000, 4000])
    c = check_coverage(2500, logs)
    assert c.ok
    assert c.threads[0].first_sequence == 11
    assert c.threads[0].last_sequence == 13


def test_a_deleted_log_after_the_start_is_a_gap():
    logs = contiguous_logs(1, 10, [1000, 2000, 3000, 4000])
    logs[2] = LogFile(1, 12, 3000, 4000, False)
    c = check_coverage(1500, logs)
    assert not c.ok
    assert c.threads[0].missing == [12]
    assert "sequence 12" in c.problems()[0]


def test_a_copy_in_another_destination_counts():
    logs = contiguous_logs(1, 10, [1000, 2000, 3000])
    logs.append(LogFile(1, 11, 2000, 3000, False))
    assert check_coverage(2500, logs).ok


def test_a_start_older_than_every_recorded_log_is_refused():
    logs = contiguous_logs(1, 10, [1000, 2000, 3000])
    c = check_coverage(500, logs)
    assert not c.ok
    assert "earliest recorded log starts at SCN 1000" in c.problems()[0]


def test_the_purged_log_holding_the_start_is_refused():
    logs = [LogFile(1, 9, 500, 1000, False), *contiguous_logs(1, 10, [1000, 2000])]
    c = check_coverage(700, logs)
    assert not c.ok
    assert c.threads[0].missing == [9]


def test_start_in_the_current_online_log():
    c = check_coverage(5000, contiguous_logs(1, 10, [1000, 4000]))
    assert c.ok
    assert c.threads[0].first_sequence == 11


def test_every_thread_is_checked():
    logs = contiguous_logs(1, 10, [1000, 2000]) + contiguous_logs(2, 20, [3000, 4000])
    c = check_coverage(1500, logs)
    assert [t.thread for t in c.threads] == [1, 2]
    assert not c.ok  # thread 2 has nothing at or before 1500


def test_ranges_are_written_as_words():
    assert ranges([1, 2, 3, 5, 7, 8]) == "1 to 3, 5, 7 to 8"
    assert ranges(list(range(0, 100, 2)), limit=2) == "0, 2 and 48 more"
