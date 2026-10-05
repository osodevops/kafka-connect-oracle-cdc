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
"""verify_cutover against fakes: VER-1 to VER-5."""

from __future__ import annotations

import base64
import json
from decimal import Decimal

import pytest

from oso_cdc_migration.common import EXIT_FAILURE, EXIT_OK
from oso_cdc_migration.normalise import Column
from oso_cdc_migration.oracle import SnapshotTooOld
from oso_cdc_migration.verify import check_seal, main

from .fakes import FakeConnect, FakeTopics, FakeVerifyDb, TopicWriter

COLUMNS = [
    Column("ID", "NUMBER", 9, 0),
    Column("STATUS", "VARCHAR2"),
    Column("AMOUNT", "NUMBER", 10, 2),
    Column("CREATED", "DATE"),
    Column("NOTE", "CLOB"),
]
MAR_1 = 1709287200000  # 2024-03-01T10:00 UTC in milliseconds
CHECK = 5000


def oso_config(tmp_path, **extra):
    config = {
        "name": "orders-oso",
        "config": {
            "connector.class": "sh.oso.connect.oracle.OracleCdcSourceConnector",
            "cdc.topic.prefix": "srv",
            "cdc.topic.template": "${prefix}.${schema}.${table}",
            "cdc.decimal.mode": "string",
            "cdc.database.password": "never-printed-pw",
            **extra,
        },
    }
    path = tmp_path / "connector.json"
    path.write_text(json.dumps(config))
    return path


def row(i, status, amount="10.00", note="n"):
    return {"ID": i, "STATUS": status, "AMOUNT": amount, "CREATED": MAR_1, "NOTE": note}


def db_row(i, status, amount="10", note="n"):
    return {
        "ID": Decimal(i),
        "STATUS": status,
        "AMOUNT": Decimal(amount),
        "CREATED": "2024-03-01T10:00:00",
        "NOTE": note,
    }


def standard_topic(topics, style="oso", topic="srv.APP.ORDERS"):
    w = TopicWriter(topics, topic, style)
    w.change("c", {"ID": 1}, None, row(1, "NEW"), 100)
    w.change("c", {"ID": 2}, None, row(2, "NEW"), 110)
    w.change("u", {"ID": 1}, row(1, "NEW"), row(1, "SHIPPED"), 120)
    w.change("d", {"ID": 2}, row(2, "NEW"), None, 130)
    w.tombstone({"ID": 2}, 130)
    w.change("c", {"ID": 3}, None, row(3, "NEW", "12.50"), 140)
    w.change("c", {"ID": 4}, None, row(4, "LATE"), 9000)  # after the check SCN
    return w


def db_with(rows, error=None):
    db = FakeVerifyDb()
    db.add("APP", "ORDERS", COLUMNS, ["ID"], rows, error)
    return db


def run(tmp_path, db, topics, *extra, api=None, sleep=None, config=None):
    out = tmp_path / "evidence.json"
    argv = [
        "--input",
        str(config or oso_config(tmp_path)),
        "--table",
        "APP.ORDERS",
        "--check-scn",
        str(CHECK),
        "--output",
        str(out),
        "--report",
        str(tmp_path / "verification.md"),
        *extra,
    ]
    if api is None and "--connect-url" not in extra:
        argv.append("--no-position-check")
    code = main(argv, db=db, topics=topics, api=api, sleep=sleep or (lambda s: None))
    return code, json.loads(out.read_text()) if out.exists() else None


def test_pass_when_topic_state_equals_the_database(tmp_path, capsys):
    topics = FakeTopics()
    standard_topic(topics)
    code, ev = run(tmp_path, db_with([db_row(1, "SHIPPED"), db_row(3, "NEW", "12.5")]), topics)
    assert code == EXIT_OK
    t = ev["tables"][0]
    assert ev["result"] == "PASS"
    assert t["status"] == "PASS"
    assert t["database_rows"] == t["topic_rows"] == 2
    assert t["database_hash"] == t["topic_hash"]
    assert t["records"]["after_check_scn"] == 1
    assert t["ignored_columns"] == {"NOTE": "LOB columns are left out of the records"}
    assert check_seal(ev)
    assert "never-printed-pw" not in json.dumps(ev)


def test_fail_lists_missing_extra_and_different_keys(tmp_path, capsys):
    topics = FakeTopics()
    standard_topic(topics)
    db = db_with([db_row(1, "PACKED"), db_row(5, "NEW")])
    code, ev = run(tmp_path, db, topics)
    t = ev["tables"][0]
    assert code == EXIT_FAILURE
    assert t["status"] == "FAIL"
    assert (t["only_in_database"], t["only_in_topics"], t["different"]) == (1, 1, 1)
    kinds = sorted(s["difference"] for s in t["samples"])
    assert kinds == ["different values", "in the database only", "in the topics only"]
    different = next(s for s in t["samples"] if s["difference"] == "different values")
    assert different["key"] == [["ID", "1"]]
    assert different["columns"] == ["STATUS"]
    assert "values" not in different
    assert "PACKED" not in json.dumps(ev)


def test_show_values_reveals_the_values(tmp_path, capsys):
    topics = FakeTopics()
    standard_topic(topics)
    _, ev = run(
        tmp_path, db_with([db_row(1, "PACKED"), db_row(3, "NEW", "12.5")]), topics, "--show-values"
    )
    sample = ev["tables"][0]["samples"][0]
    assert sample["values"] == {"STATUS": {"database": "PACKED", "topic": "SHIPPED"}}


def test_debezium_topics_without_headers(tmp_path, capsys):
    cfg = tmp_path / "dbz.json"
    cfg.write_text(
        json.dumps(
            {
                "name": "inventory",
                "config": {
                    "connector.class": "io.debezium.connector.oracle.OracleConnector",
                    "topic.prefix": "srv",
                    "decimal.handling.mode": "string",
                },
            }
        )
    )
    topics = FakeTopics()
    standard_topic(topics, style="debezium")
    code, ev = run(
        tmp_path, db_with([db_row(1, "SHIPPED"), db_row(3, "NEW", "12.5")]), topics, config=cfg
    )
    assert ev["inputs"]["connector_kind"] == "debezium"
    assert ev["inputs"]["unavailable_placeholder"] == "__debezium_unavailable_value"
    assert code == EXIT_OK


def test_schema_envelope_and_base64_decimals(tmp_path, capsys):
    cfg = oso_config(tmp_path, **{"cdc.decimal.mode": "precise"})
    topics = FakeTopics()
    w = TopicWriter(topics, "srv.APP.ORDERS", envelope=True)
    decimal = {"name": "org.apache.kafka.connect.data.Decimal", "parameters": {"scale": "2"}}
    fields = [
        {"field": "ID", "type": "int32"},
        {"field": "STATUS", "type": "string"},
        {"field": "AMOUNT", "type": "bytes", **decimal},
        {"field": "CREATED", "type": "int64", "name": "io.debezium.time.Timestamp"},
    ]
    w.value_schema = {
        "type": "struct",
        "fields": [
            {"field": "before", "type": "struct", "fields": fields},
            {"field": "after", "type": "struct", "fields": fields},
        ],
    }
    w.key_schema = {"type": "struct", "fields": [{"field": "ID", "type": "int32"}]}
    amount = base64.b64encode((1250).to_bytes(2, "big", signed=True)).decode()
    w.change("c", {"ID": 1}, None, {**row(1, "NEW"), "AMOUNT": amount}, 100)
    code, ev = run(tmp_path, db_with([db_row(1, "NEW", "12.5")]), topics, config=cfg)
    assert ev["tables"][0]["status"] == "PASS", ev["tables"][0]["reason"]
    assert code == EXIT_OK


def test_unavailable_lob_keeps_the_previous_value(tmp_path, capsys):
    cfg = oso_config(tmp_path, **{"cdc.lob.mode": "inline"})
    topics = FakeTopics()
    w = TopicWriter(topics, "srv.APP.ORDERS")
    w.change("c", {"ID": 1}, None, row(1, "NEW", note="first"), 100)
    w.change("u", {"ID": 1}, None, row(1, "PAID", note="__cdc_unavailable_value"), 110)
    code, ev = run(tmp_path, db_with([db_row(1, "PAID", note="first")]), topics, config=cfg)
    assert ev["tables"][0]["compared_columns"][-1] == "NOTE"
    assert code == EXIT_OK


def test_tombstone_without_headers_follows_its_delete(tmp_path, capsys):
    topics = FakeTopics()
    w = TopicWriter(topics, "srv.APP.ORDERS", style="debezium")
    w.change("c", {"ID": 1}, None, row(1, "NEW"), 100)
    w.change("d", {"ID": 1}, row(1, "NEW"), None, 9999)  # after the check SCN
    w.tombstone({"ID": 1})
    code, _ = run(tmp_path, db_with([db_row(1, "NEW")]), topics)
    assert code == EXIT_OK


def test_compacted_topic_with_only_a_tombstone(tmp_path, capsys):
    topics = FakeTopics()
    w = TopicWriter(topics, "srv.APP.ORDERS")
    w.tombstone({"ID": 1}, 120)
    w.change("c", {"ID": 2}, None, row(2, "NEW"), 130)
    code, _ = run(tmp_path, db_with([db_row(2, "NEW")]), topics)
    assert code == EXIT_OK


def test_keyless_table_counts_copies(tmp_path, capsys):
    db = FakeVerifyDb()
    cols = [Column("A", "VARCHAR2")]
    db.add("APP", "LOG", cols, [], [{"A": "x"}, {"A": "x"}, {"A": "y"}])
    topics = FakeTopics()
    w = TopicWriter(topics, "srv.APP.LOG")
    for v in ("x", "x", "y", "z"):
        w.change("c", None, None, {"A": v}, 100, table="LOG")
    w.change("d", None, {"A": "z"}, None, 110, table="LOG")
    out = tmp_path / "e.json"
    code = main(
        [
            "--input",
            str(oso_config(tmp_path)),
            "--table",
            "APP.LOG",
            "--check-scn",
            "500",
            "--no-position-check",
            "--output",
            str(out),
        ],
        db=db,
        topics=topics,
    )
    assert code == EXIT_OK
    assert json.loads(out.read_text())["tables"][0]["key_columns"] == []


def test_truncate_clears_the_table(tmp_path, capsys):
    topics = FakeTopics()
    w = TopicWriter(topics, "srv.APP.ORDERS", style="debezium")
    w.change("c", {"ID": 1}, None, row(1, "NEW"), 100)
    w.change("t", None, None, None, 110)
    code, _ = run(tmp_path, db_with([]), topics)
    assert code == EXIT_OK


def test_snapshot_rows_read_after_the_check_scn_are_inconclusive(tmp_path, capsys):
    topics = FakeTopics()
    w = TopicWriter(topics, "srv.APP.ORDERS")
    w.change("r", {"ID": 1}, None, row(1, "NEW"), None, scn=CHECK + 10)
    code, ev = run(tmp_path, db_with([db_row(1, "NEW")]), topics)
    assert code == EXIT_FAILURE
    assert ev["tables"][0]["status"] == "INCONCLUSIVE"
    assert "after the check SCN" in ev["tables"][0]["reason"]


def test_snapshot_rows_before_the_check_scn_count(tmp_path, capsys):
    topics = FakeTopics()
    TopicWriter(topics, "srv.APP.ORDERS").change("r", {"ID": 1}, None, row(1, "NEW"), None, scn=90)
    code, _ = run(tmp_path, db_with([db_row(1, "NEW")]), topics)
    assert code == EXIT_OK


def test_records_without_a_commit_scn_are_inconclusive(tmp_path, capsys):
    topics = FakeTopics()
    TopicWriter(topics, "srv.APP.ORDERS", style="debezium").change(
        "c", {"ID": 1}, None, row(1, "NEW"), None
    )
    code, ev = run(tmp_path, db_with([db_row(1, "NEW")]), topics)
    assert ev["tables"][0]["status"] == "INCONCLUSIVE"
    assert code == EXIT_FAILURE


def test_snapshot_too_old_is_inconclusive(tmp_path, capsys):
    topics = FakeTopics()
    standard_topic(topics)
    db = db_with([], SnapshotTooOld("ORA-01555: snapshot too old"))
    code, ev = run(tmp_path, db, topics)
    assert ev["tables"][0]["status"] == "INCONCLUSIVE"
    assert "UNDO_RETENTION" in ev["tables"][0]["reason"]
    assert code == EXIT_FAILURE


def test_unreadable_record_is_inconclusive(tmp_path, capsys):
    from oso_cdc_migration.records import KafkaRecord

    topics = FakeTopics()
    topics.add(KafkaRecord("srv.APP.ORDERS", 0, 0, b"{}", b"not json", []))
    code, ev = run(tmp_path, db_with([]), topics)
    assert ev["tables"][0]["status"] == "INCONCLUSIVE"
    assert code == EXIT_FAILURE


def test_batches_cover_every_row(tmp_path, capsys):
    rows = [db_row(i, "NEW") for i in range(1, 8)]
    topics = FakeTopics()
    w = TopicWriter(topics, "srv.APP.ORDERS")
    for i in range(1, 8):
        w.change("c", {"ID": i}, None, row(i, "NEW", "10"), 100 + i)
    code, ev = run(tmp_path, db_with(rows), topics, "--batch-rows", "3")
    assert ev["tables"][0]["database_rows"] == 7
    assert code == EXIT_OK


def test_position_must_pass_the_check_scn(tmp_path, capsys):
    topics = FakeTopics()
    standard_topic(topics)
    api = FakeConnect()
    reads = iter([[CHECK - 100], [CHECK - 1], [CHECK + 1]])

    def offsets(connector):
        return {
            "offsets": [
                {"partition": {"server": "srv"}, "offset": {"resume_scn": s}} for s in next(reads)
            ]
        }

    api.offsets = offsets
    slept = []
    code, ev = run(
        tmp_path,
        db_with([db_row(1, "SHIPPED"), db_row(3, "NEW", "12.5")]),
        topics,
        "--connect-url",
        "http://connect",
        api=api,
        sleep=slept.append,
    )
    assert code == EXIT_OK
    assert ev["inputs"]["position_check"] == {
        "checked": True,
        "connector": "orders-oso",
        "position_scn": CHECK + 1,
        "passed": True,
    }
    assert len(slept) == 2


def test_position_that_never_passes_fails_without_evidence(tmp_path, capsys):
    api = FakeConnect()
    api.offset_entries["orders-oso"] = [{"partition": {}, "offset": {"scn": str(CHECK)}}]
    code, ev = run(
        tmp_path,
        db_with([]),
        FakeTopics(),
        "--connect-url",
        "http://c",
        "--wait-seconds",
        "10",
        api=api,
    )
    assert code == EXIT_FAILURE
    assert ev is None
    assert "has not passed the check SCN" in capsys.readouterr().err


def test_default_check_scn_is_the_safety_margin(tmp_path, capsys):
    topics = FakeTopics()
    out = tmp_path / "e.json"
    db = db_with([])
    code = main(
        [
            "--input",
            str(oso_config(tmp_path)),
            "--table",
            "APP.ORDERS",
            "--no-position-check",
            "--safety-margin-seconds",
            "60",
            "--output",
            str(out),
        ],
        db=db,
        topics=topics,
    )
    ev = json.loads(out.read_text())
    assert ev["inputs"]["check_scn"] == db.current_scn() - 600
    assert code == EXIT_OK


def test_check_evidence_detects_tampering(tmp_path, capsys):
    topics = FakeTopics()
    standard_topic(topics)
    run(tmp_path, db_with([db_row(1, "SHIPPED"), db_row(3, "NEW", "12.5")]), topics)
    path = tmp_path / "evidence.json"
    assert main(["--check-evidence", str(path)]) == EXIT_OK
    ev = json.loads(path.read_text())
    ev["tables"][0]["status"] = "FAIL"
    path.write_text(json.dumps(ev))
    assert main(["--check-evidence", str(path)]) == EXIT_FAILURE


def test_two_tables_in_one_topic(tmp_path, capsys):
    db = db_with([db_row(1, "NEW")])
    db.add("APP", "OTHER", [Column("ID", "NUMBER", 9, 0)], ["ID"], [{"ID": Decimal(7)}])
    topics = FakeTopics()
    w = TopicWriter(topics, "shared")
    w.change("c", {"ID": 1}, None, row(1, "NEW"), 100)
    w.change("c", {"ID": 7}, None, {"ID": 7}, 101, table="OTHER")
    out = tmp_path / "e.json"
    code = main(
        [
            "--table",
            "APP.ORDERS=shared",
            "--table",
            "APP.OTHER=shared",
            "--check-scn",
            "500",
            "--no-position-check",
            "--decimal-mode",
            "string",
            "--output",
            str(out),
        ],
        db=db,
        topics=topics,
    )
    assert code == EXIT_OK, out.read_text()


def test_confluent_configuration_is_refused(tmp_path, capsys):
    cfg = tmp_path / "c.json"
    cfg.write_text(json.dumps({"connector.class": "io.confluent.connect.oracle.cdc.X"}))
    code, _ = run(tmp_path, db_with([]), FakeTopics(), config=cfg)
    assert code == EXIT_FAILURE


@pytest.mark.parametrize("flag", ["--table", "--check-scn"])
def test_usage_errors_exit_one(flag):
    with pytest.raises(SystemExit) as e:
        main([flag])
    assert e.value.code == EXIT_FAILURE
