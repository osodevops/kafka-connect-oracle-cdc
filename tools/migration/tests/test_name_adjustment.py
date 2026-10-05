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
"""Debezium schema and field name adjustment to ``cdc.*.name.adjustment.mode`` (ADR-0020)."""

from __future__ import annotations

import pytest

from oso_cdc_migration.debezium import SPEC as DEBEZIUM

from .test_translators import BASE, by_key, run

SCHEMA = "cdc.schema.name.adjustment.mode"
FIELD = "cdc.field.name.adjustment.mode"

LEGACY = {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "database.hostname": "db",
    "database.port": "1521",
    "database.user": "dbz",
    "database.password": "${file:/s.properties:p}",
    "database.dbname": "ORCL",
    "database.server.name": "legacy",
    "table.include.list": "APP\\.T1",
    "database.history.kafka.topic": "legacy.history",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
}
AVRO = {
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
}


@pytest.mark.parametrize("mode", ["none", "avro", "avro_unicode", "AVRO"])
def test_adjustment_modes_map_to_the_same_values(tmp_path, capsys, mode):
    cfg = {**BASE, "schema.name.adjustment.mode": mode, "field.name.adjustment.mode": mode}
    _, out, report = run(DEBEZIUM, tmp_path, cfg, capsys=capsys)
    assert out["config"][SCHEMA] == mode.lower()
    assert out["config"][FIELD] == mode.lower()
    for key in ("schema.name.adjustment.mode", "field.name.adjustment.mode"):
        assert by_key(report)[key]["classification"] == "mapped"


def test_an_unknown_adjustment_mode_is_manual(tmp_path, capsys):
    cfg = {**BASE, "field.name.adjustment.mode": "avro-unicode"}
    _, out, report = run(DEBEZIUM, tmp_path, cfg, capsys=capsys)
    assert by_key(report)["field.name.adjustment.mode"]["classification"] == "manual"
    assert FIELD not in out["config"]


@pytest.mark.parametrize(("flag", "mode"), [("true", "avro"), ("false", "none")])
def test_sanitize_field_names_becomes_a_field_adjustment_mode(tmp_path, capsys, flag, mode):
    cfg = {**BASE, "sanitize.field.names": flag}
    _, out, report = run(DEBEZIUM, tmp_path, cfg, capsys=capsys)
    assert out["config"][FIELD] == mode
    assert by_key(report)["sanitize.field.names"]["classification"] == "mapped-with-change"


def test_a_2x_configuration_keeps_the_defaults_without_pins(tmp_path, capsys):
    _, out, report = run(DEBEZIUM, tmp_path, {**BASE, **AVRO}, capsys=capsys)
    assert SCHEMA not in out["config"]
    assert FIELD not in out["config"]
    assert not [s for s in report["settings"] if s["key"] in (SCHEMA, FIELD)]


def test_a_1x_configuration_keeps_its_avro_schema_names(tmp_path, capsys):
    _, out, report = run(DEBEZIUM, tmp_path, LEGACY, capsys=capsys)
    assert out["config"][SCHEMA] == "avro"
    assert FIELD not in out["config"], "1.x sanitised fields only with an Avro converter"
    pinned = {s["key"]: s for s in report["settings"]}
    assert "Debezium 1.x" in pinned[SCHEMA]["reason"]


def test_a_1x_configuration_with_an_avro_converter_keeps_its_field_names(tmp_path, capsys):
    _, out, report = run(DEBEZIUM, tmp_path, {**LEGACY, **AVRO}, capsys=capsys)
    assert out["config"][SCHEMA] == "avro"
    assert out["config"][FIELD] == "avro"
    pinned = {s["key"]: s for s in report["settings"]}
    assert "sanitize.field.names" in pinned[FIELD]["reason"]


def test_explicit_1x_settings_win_over_the_pins(tmp_path, capsys):
    cfg = {
        **LEGACY,
        **AVRO,
        "schema.name.adjustment.mode": "none",
        "sanitize.field.names": "false",
    }
    _, out, report = run(DEBEZIUM, tmp_path, cfg, capsys=capsys)
    assert out["config"][SCHEMA] == "none"
    assert out["config"][FIELD] == "none"
    assert not [s for s in report["settings"] if s["key"] in (SCHEMA, FIELD)]
