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
"""Translator behaviour beyond the golden files."""

from __future__ import annotations

import json

import pytest

from oso_cdc_migration.common import EXIT_FAILURE, EXIT_FOLLOW_UPS, EXIT_OK
from oso_cdc_migration.configio import format_properties, parse_properties
from oso_cdc_migration.confluent import SPEC as CONFLUENT
from oso_cdc_migration.debezium import SPEC as DEBEZIUM
from oso_cdc_migration.translator_cli import translator_main

from .fakes import FakeConnect, eos_response


def run(spec, tmp_path, config, *extra, api=None, capsys=None):
    src = tmp_path / "in.json"
    src.write_text(json.dumps(config))
    out = tmp_path / "out.json"
    code = translator_main(
        spec, ["--input", str(src), "--output", str(out), "--json", *extra], api=api
    )
    report = json.loads(capsys.readouterr().out) if capsys else None
    return code, json.loads(out.read_text()) if out.exists() else None, report


BASE = {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "database.hostname": "db",
    "database.port": "1521",
    "database.user": "c##u",
    "database.password": "${file:/s.properties:p}",
    "database.dbname": "ORCLCDB",
    "database.pdb.name": "PDB1",
    "topic.prefix": "srv",
    "table.include.list": "APP.T1",
}


def by_key(report):
    return {p["key"]: p for p in report["properties"]}


def test_schema_and_table_lists_combine_into_one_pattern_set(tmp_path, capsys):
    cfg = {
        **BASE,
        "table.include.list": "^APP\\.T1$,APP\\.T2|APP\\.T3",
        "schema.include.list": "APP",
        "schema.exclude.list": "TMP.*",
    }
    _, out, _ = run(DEBEZIUM, tmp_path, {"name": "a", "config": cfg}, capsys=capsys)
    c = out["config"]
    assert c["cdc.tables.include"].split(",") == [
        "PDB1\\.(?=(?:APP)\\.)APP\\.T1",
        "PDB1\\.(?=(?:APP)\\.)(?:APP\\.T2|APP\\.T3)",
    ]
    assert c["cdc.tables.exclude"] == "PDB1\\.TMP.*\\..+"


def test_escaped_comma_in_a_table_list_is_manual(tmp_path, capsys):
    cfg = {**BASE, "table.include.list": "APP\\.T{1\\,2}"}
    code, out, report = run(DEBEZIUM, tmp_path, cfg, capsys=capsys)
    assert by_key(report)["table.include.list"]["classification"] == "manual"
    assert "cdc.tables.include" not in out["config"]
    assert code == EXIT_FOLLOW_UPS


def test_regex_in_message_key_columns_is_manual(tmp_path, capsys):
    cfg = {**BASE, "message.key.columns": "APP.T.*:ID"}
    _, out, report = run(DEBEZIUM, tmp_path, cfg, capsys=capsys)
    assert by_key(report)["message.key.columns"]["classification"] == "manual"
    assert "cdc.key.columns" not in out["config"]


def test_secret_driver_property_is_manual_and_not_copied(tmp_path, capsys):
    cfg = {**BASE, "driver.oracle.net.wallet_password": "wallet-secret-1"}
    _, out, report = run(DEBEZIUM, tmp_path, cfg, capsys=capsys)
    assert by_key(report)["driver.oracle.net.wallet_password"]["classification"] == "manual"
    assert "wallet-secret-1" not in json.dumps(out) + json.dumps(report)


def test_exactly_once_enabled_sets_both_properties(tmp_path, capsys):
    api = FakeConnect()
    api.validate_response = eos_response([])
    _, out, report = run(
        DEBEZIUM, tmp_path, BASE, "--connect-url", "http://c", api=api, capsys=capsys
    )
    assert out["config"]["exactly.once.support"] == "required"
    assert out["config"]["transaction.boundary"] == "connector"
    assert report["exactly_once"]["status"] == "enabled"


def test_exactly_once_disabled_leaves_at_least_once_with_a_follow_up(tmp_path, capsys):
    api = FakeConnect()
    api.validate_response = eos_response(["This worker does not have exactly-once enabled."])
    cfg = {**BASE, "exactly.once.support": "required"}
    code, out, report = run(
        DEBEZIUM, tmp_path, cfg, "--connect-url", "http://c", api=api, capsys=capsys
    )
    assert "exactly.once.support" not in out["config"]
    assert any("does not have exactly-once" in f["text"] for f in report["follow_ups"])
    assert code == EXIT_FOLLOW_UPS


def test_conflicting_targets_keep_the_first_and_report(tmp_path, capsys):
    cfg = {
        **BASE,
        "archive.destination.name": "LOG_ARCHIVE_DEST_1",
        "log.mining.archive.destination.name": "LOG_ARCHIVE_DEST_2",
    }
    _, out, report = run(DEBEZIUM, tmp_path, cfg, capsys=capsys)
    assert out["config"]["cdc.archive.destination"] == "LOG_ARCHIVE_DEST_1"
    assert any("would be set differently" in f["text"] for f in report["follow_ups"])


def test_wrong_connector_class_is_refused(tmp_path, capsys):
    cfg = {**BASE, "connector.class": "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector"}
    code, out, _ = run(DEBEZIUM, tmp_path, cfg)
    assert code == EXIT_FAILURE
    assert out is None


def test_new_name_must_differ_from_the_old_one(tmp_path):
    code, out, _ = run(DEBEZIUM, tmp_path, {"name": "same", "config": BASE}, "--name", "same")
    assert code == EXIT_FAILURE
    assert out is None


def test_usage_errors_exit_with_one_not_two(tmp_path):
    with pytest.raises(SystemExit) as e:
        translator_main(DEBEZIUM, ["--no-such-option"])
    assert e.value.code == EXIT_FAILURE


def test_clean_translation_exits_zero(tmp_path, capsys):
    api = FakeConnect()
    api.validate_response = eos_response([])
    cfg = {**BASE, "include.schema.changes": "false"}
    code, _, report = run(
        DEBEZIUM,
        tmp_path,
        cfg,
        "--connect-url",
        "http://c",
        "--start-scn",
        "100",
        api=api,
        capsys=capsys,
    )
    assert report["follow_ups"] == []
    assert code == EXIT_OK


def test_properties_round_trip(tmp_path):
    text = "a.b=x\\:y\nkey\\ with\\ space = v\n! comment\nlong=one \\\n    two\n"
    parsed = parse_properties(text)
    assert parsed == {"a.b": "x:y", "key with space": "v", "long": "one two"}
    assert parse_properties(format_properties(parsed)) == parsed


def test_properties_input_and_output(tmp_path, capsys):
    src = tmp_path / "in.properties"
    src.write_text(format_properties({"name": "p", **BASE}))
    out = tmp_path / "out.properties"
    code = translator_main(DEBEZIUM, ["--input", str(src), "--output", str(out)])
    assert code == EXIT_FOLLOW_UPS
    config = parse_properties(out.read_text())
    assert config["cdc.topic.prefix"] == "srv"
    assert config["cdc.snapshot.mode"] == "none"
    assert "cdc.start.scn" not in config


def test_confluent_tables_without_a_pdb_lose_the_sid(tmp_path, capsys):
    cfg = {
        "connector.class": "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector",
        "oracle.sid": "ORCL",
        "table.inclusion.regex": "^ORCL\\.APP\\.(A|B)$",
        "table.exclusion.regex": ".*[.]APP[.]B",
    }
    _, out, _ = run(CONFLUENT, tmp_path, {"name": "c", "config": cfg}, capsys=capsys)
    assert out["config"]["cdc.tables.include"] == "APP\\.(A|B)"
    assert out["config"]["cdc.tables.exclude"] == "APP[.]B"
    assert out["config"]["cdc.tables.case.sensitive"] == "true"


def test_confluent_regex_that_cannot_lose_its_database_is_manual(tmp_path, capsys):
    cfg = {
        "connector.class": "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector",
        "oracle.sid": "ORCL",
        "table.inclusion.regex": "OTHER[.]APP[.]A",
    }
    _, out, report = run(CONFLUENT, tmp_path, cfg, capsys=capsys)
    assert by_key(report)["table.inclusion.regex"]["classification"] == "manual"
    assert "cdc.tables.include" not in out["config"]


def test_confluent_record_format_follow_up_comes_first(tmp_path, capsys):
    cfg = {"connector.class": "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector"}
    code, _, report = run(CONFLUENT, tmp_path, cfg, capsys=capsys)
    assert report["follow_ups"][0]["text"].startswith("This release publishes the Debezium")
    assert code == EXIT_FOLLOW_UPS


def test_confluent_connector_name_in_template_becomes_literal(tmp_path, capsys):
    cfg = {
        "connector.class": "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector",
        "table.topic.name.template": "${connectorName}.${tableName}",
    }
    _, out, _ = run(CONFLUENT, tmp_path, {"name": "cx", "config": cfg}, capsys=capsys)
    assert out["config"]["cdc.topic.template"] == "cx.${table}"


def test_summary_mode_prints_counts(tmp_path, capsys):
    src = tmp_path / "in.json"
    src.write_text(json.dumps(BASE))
    code = translator_main(DEBEZIUM, ["--input", str(src)])
    text = capsys.readouterr().out
    assert "Debezium Oracle connector:" in text
    assert code in (EXIT_OK, EXIT_FOLLOW_UPS)


def test_start_scn_is_written_for_the_takeover(tmp_path, capsys):
    _, out, report = run(DEBEZIUM, tmp_path, BASE, "--start-scn", "324567890", capsys=capsys)
    assert out["config"]["cdc.start.scn"] == "324567890"
    assert out["config"]["cdc.snapshot.mode"] == "none"
    assert not any("cdc.start.scn" in f["text"] for f in report["follow_ups"])


def test_without_a_start_scn_the_report_asks_for_the_takeover(tmp_path, capsys):
    code, out, report = run(DEBEZIUM, tmp_path, BASE, capsys=capsys)
    assert "cdc.start.scn" not in out["config"]
    assert any("takeover_scn.py" in f["text"] for f in report["follow_ups"])
    assert code == EXIT_FOLLOW_UPS
