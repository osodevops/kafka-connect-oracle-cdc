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

import pytest

from oso_cdc_migration.common import ToolError
from oso_cdc_migration.offsets import parse_commit_scn, parse_offsets


def dbz(offset, server="server1"):
    return {"offsets": [{"partition": {"server": server}, "offset": offset}]}


def test_debezium_uses_scn_and_records_commit_scn():
    p = parse_offsets(dbz({"scn": "324567890", "commit_scn": "324567897:1:0a001b00c1020000"}))
    assert p.source == "debezium"
    assert p.start_scn == 324567890
    assert p.last_commit_scn == 324567897
    assert p.commit_scn_raw == "324567897:1:0a001b00c1020000"


def test_debezium_commit_scn_forms():
    assert parse_commit_scn("12345") == [12345]
    assert parse_commit_scn("100:1:aa-bb,120:2:cc") == [100, 120]
    assert parse_commit_scn("324567897: 0x2832343233323:1") == [324567897]
    assert parse_commit_scn(None) == []


def test_debezium_without_a_finished_snapshot_is_refused():
    with pytest.raises(ToolError, match="inside its initial snapshot"):
        parse_offsets(dbz({"scn": "100", "snapshot": "true", "snapshot_completed": "false"}))


def test_debezium_finished_snapshot_flag_is_accepted():
    p = parse_offsets(dbz({"scn": "100", "snapshot": True, "snapshot_completed": True}))
    assert p.start_scn == 100


def test_pending_snapshot_transactions_move_the_start_back():
    p = parse_offsets(dbz({"scn": "500", "snapshot_pending_tx": "0a001b00c1020000:450"}))
    assert p.start_scn == 450
    assert p.notes


def test_unreadable_pending_transactions_are_refused():
    with pytest.raises(ToolError, match="snapshot_pending_tx"):
        parse_offsets(dbz({"scn": "500", "snapshot_pending_tx": "abc:def"}))


def test_xstream_offset_without_scn_is_refused():
    with pytest.raises(ToolError, match="XStream"):
        parse_offsets(dbz({"lcr_position": "0000001f0000000100000001"}))


def test_no_committed_offset_is_refused():
    with pytest.raises(ToolError, match="no committed offset"):
        parse_offsets({"offsets": [{"partition": {"server": "s"}, "offset": None}]})


def test_not_an_scn_is_refused():
    with pytest.raises(ToolError, match="not an SCN"):
        parse_offsets(dbz({"scn": "12a"}))


def test_confluent_takes_the_lowest_scn():
    doc = {
        "offsets": [
            {"partition": {"sidPdb": "ORCLCDB.ORCLPDB1"}, "offset": {"scn": "2169287"}},
            {"partition": {"sidPdb": "ORCLCDB.ORCLPDB2"}, "offset": {"scn": "2169100"}},
        ]
    }
    p = parse_offsets(doc)
    assert p.source == "confluent"
    assert p.start_scn == 2169100
    assert p.last_commit_scn is None


def test_unknown_partition_shape_needs_source():
    doc = {"offsets": [{"partition": {"other": "x"}, "offset": {"scn": "1"}}]}
    with pytest.raises(ToolError, match="--source"):
        parse_offsets(doc)
    assert parse_offsets(doc, "confluent").start_scn == 1
