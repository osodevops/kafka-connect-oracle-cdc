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
"""PRD-04 MIG-7 and acceptance: canary secrets never appear in any output, report or log line."""

from __future__ import annotations

import json
import logging

import pytest

from oso_cdc_migration.confluent import SPEC as CONFLUENT
from oso_cdc_migration.debezium import SPEC as DEBEZIUM
from oso_cdc_migration.normalise import Column
from oso_cdc_migration.takeover import main as takeover
from oso_cdc_migration.translator_cli import translator_main
from oso_cdc_migration.verify import main as verify

from .fakes import FakeConnect, FakeTakeoverDb, FakeTopics, FakeVerifyDb, contiguous_logs

CANARIES = [
    "CANARY-db-7f3a9c",
    "CANARY-url-55aa",
    "CANARY-ts-1122",
    "CANARY-sr-3344",
    "CANARY-jaas-9988",
    "CANARY-token-5566",
    "CANARY-lic-0011",
    "CANARY-ldap-2233",
    "CANARY-env-4455",
    "CANARY-connect-7788",
    "CANARY-kafka-6677",
]


def assert_clean(tmp_path, capsys, caplog, inputs):
    out, err = capsys.readouterr()
    texts = {"stdout": out, "stderr": err, "log": caplog.text}
    for path in tmp_path.rglob("*"):
        if path.is_file() and path not in inputs:
            texts[path.name] = path.read_text()
    assert len(texts) > 3, "the tool wrote nothing to check"
    for where, text in texts.items():
        for canary in CANARIES:
            assert canary not in text, f"{canary} leaked into {where}"


@pytest.fixture(autouse=True)
def debug_logging(caplog):
    caplog.set_level(logging.DEBUG)


@pytest.mark.parametrize("summary", [True, False])
def test_debezium_translator_masks_every_secret(tmp_path, capsys, caplog, summary):
    src = tmp_path / "in.json"
    src.write_text(
        json.dumps(
            {
                "name": "dbz",
                "config": {
                    "connector.class": "io.debezium.connector.oracle.OracleConnector",
                    "database.hostname": "db",
                    "database.user": "c##u",
                    "database.password": "CANARY-db-7f3a9c",
                    "database.url": "jdbc:oracle:thin:scott/CANARY-url-55aa@db:1521/ORCLCDB",
                    "database.dbname": "ORCLCDB",
                    "topic.prefix": "p",
                    "table.include.list": "APP.T",
                    "driver.javax.net.ssl.trustStorePassword": "CANARY-ts-1122",
                    "value.converter.basic.auth.user.info": "user:CANARY-sr-3344",
                    "schema.history.internal.producer.sasl.jaas.config": (
                        "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        ' username="u" password="CANARY-jaas-9988";'
                    ),
                    "vendor.api.token": "CANARY-token-5566",
                    "note": "the password is CANARY-db-7f3a9c",
                },
            }
        )
    )
    argv = [
        "--input",
        str(src),
        "--output",
        str(tmp_path / "o.json"),
        "--report",
        str(tmp_path / "r.md"),
        "-v",
    ]
    if not summary:
        argv.append("--json")
    translator_main(DEBEZIUM, argv)
    translator_main(DEBEZIUM, [*argv[:2], "--output", str(tmp_path / "o.properties"), "-v"])
    assert_clean(tmp_path, capsys, caplog, {src})


def test_confluent_translator_masks_every_secret(tmp_path, capsys, caplog):
    src = tmp_path / "in.properties"
    src.write_text(
        "name=c\n"
        "connector.class=io.confluent.connect.oracle.cdc.OracleCdcSourceConnector\n"
        "oracle.password=CANARY-db-7f3a9c\n"
        "oracle.ssl.truststore.password=CANARY-ts-1122\n"
        "confluent.license=CANARY-lic-0011\n"
        "ldap.security.credentials=CANARY-ldap-2233\n"
        'confluent.topic.sasl.jaas.config=x password="CANARY-jaas-9988";\n'
    )
    translator_main(
        CONFLUENT,
        [
            "--input",
            str(src),
            "--output",
            str(tmp_path / "o.json"),
            "--report",
            str(tmp_path / "r.md"),
            "--json",
            "-v",
        ],
    )
    assert_clean(tmp_path, capsys, caplog, {src})


class LeakyDb(FakeTakeoverDb):
    def identity(self):
        raise RuntimeError("ORA-01017: invalid password CANARY-env-4455")


def test_takeover_masks_passwords_and_connect_credentials(tmp_path, capsys, caplog, monkeypatch):
    monkeypatch.setenv("ORACLE_PW", "CANARY-env-4455")
    monkeypatch.setenv("CONNECT_AUTH", "admin:CANARY-connect-7788")
    target = tmp_path / "target.json"
    target.write_text(
        json.dumps(
            {
                "name": "new",
                "config": {
                    "cdc.topic.prefix": "p",
                    "cdc.database.host": "db",
                    "cdc.database.service": "ORCLCDB",
                    "cdc.database.user": "c##u",
                    "cdc.database.password": "CANARY-db-7f3a9c",
                },
            }
        )
    )
    api = FakeConnect()
    api.states["old"] = "STOPPED"
    api.offset_entries["old"] = [{"partition": {"server": "p"}, "offset": {"scn": "2500"}}]
    common = [
        "--connect-url",
        "http://connect",
        "--connect-auth-env",
        "CONNECT_AUTH",
        "--connector",
        "old",
        "--target-config",
        str(target),
        "--db-password-env",
        "ORACLE_PW",
        "--report",
        str(tmp_path / "t.md"),
        "-v",
    ]
    good = FakeTakeoverDb(contiguous_logs(1, 1, [1000, 2000, 3000]))
    assert takeover([*common, "--output", str(tmp_path / "takeover.json")], api=api, db=good) == 0
    assert takeover(common, api=api, db=LeakyDb([])) == 1
    assert_clean(tmp_path, capsys, caplog, {target})


class LeakyVerifyDb(FakeVerifyDb):
    def rows(self, *args, **kwargs):
        raise RuntimeError("connection lost for password CANARY-env-4455")
        yield


def test_verifier_masks_passwords_in_evidence_and_logs(tmp_path, capsys, caplog, monkeypatch):
    monkeypatch.setenv("ORACLE_PW", "CANARY-env-4455")
    kafka = tmp_path / "kafka.properties"
    kafka.write_text("security.protocol=SASL_SSL\nsasl.password=CANARY-kafka-6677\n")
    db = LeakyVerifyDb()
    db.add("APP", "T", [Column("ID", "NUMBER", 9, 0)], ["ID"], [])
    out = tmp_path / "e.json"
    code = verify(
        [
            "--table",
            "APP.T=t",
            "--check-scn",
            "10",
            "--no-position-check",
            "--db-dsn",
            "db/x",
            "--db-user",
            "u",
            "--db-password-env",
            "ORACLE_PW",
            "--kafka-config",
            str(kafka),
            "--bootstrap-servers",
            "k:9092",
            "--output",
            str(out),
            "--report",
            str(tmp_path / "v.md"),
            "--json",
            "-v",
        ],
        db=db,
        topics=FakeTopics(),
    )
    assert code == 1
    assert "reading the table failed" in out.read_text()
    assert_clean(tmp_path, capsys, caplog, {kafka})
