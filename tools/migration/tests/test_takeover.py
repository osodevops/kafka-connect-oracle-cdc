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

import json

from oso_cdc_migration.common import EXIT_FAILURE, EXIT_FOLLOW_UPS, EXIT_OK
from oso_cdc_migration.configio import parse_properties
from oso_cdc_migration.redo import LogFile
from oso_cdc_migration.takeover import main

from .fakes import FakeConnect, FakeTakeoverDb, contiguous_logs

OLD = "inventory-connector"
NEW = "inventory-connector-oso"
DEBEZIUM_OFFSETS = [
    {"partition": {"server": "server1"}, "offset": {"scn": "2500", "commit_scn": "2600:1:ab"}}
]


def connect_with_old(state="STOPPED") -> FakeConnect:
    api = FakeConnect()
    api.states[OLD] = state
    api.offset_entries[OLD] = DEBEZIUM_OFFSETS
    return api


def db_ok() -> FakeTakeoverDb:
    return FakeTakeoverDb(contiguous_logs(1, 10, [1000, 2000, 3000, 4000]), current=5000)


def target(tmp_path, name=NEW, suffix=".json"):
    config = {
        "cdc.topic.prefix": "server1",
        "cdc.database.host": "db",
        "cdc.database.service": "ORCLCDB",
        "cdc.database.user": "c##cdc",
        "cdc.snapshot.mode": "initial",
    }
    path = tmp_path / f"new{suffix}"
    if suffix == ".properties":
        path.write_text("".join(f"{k}={v}\n" for k, v in {"name": name, **config}.items()))
    else:
        path.write_text(json.dumps({"name": name, "config": config}))
    return path


def args(tmp_path, *extra):
    return [
        "--connect-url",
        "http://connect",
        "--connector",
        OLD,
        "--target-config",
        str(target(tmp_path)),
        "--json",
        *extra,
    ]


def test_writes_the_start_scn_into_the_new_configuration(tmp_path, capsys):
    out = tmp_path / "takeover.json"
    code = main(args(tmp_path, "--output", str(out)), api=connect_with_old(), db=db_ok())
    result = json.loads(capsys.readouterr().out)
    assert code == EXIT_OK
    assert result["start_scn"] == 2500
    assert result["old_commit_scn"] == 2600
    assert result["settings"] == {"cdc.start.scn": "2500", "cdc.snapshot.mode": "none"}
    written = json.loads(out.read_text())
    assert written["name"] == NEW
    assert written["config"]["cdc.start.scn"] == "2500"
    assert written["config"]["cdc.snapshot.mode"] == "none"
    assert written["config"]["cdc.topic.prefix"] == "server1"


def test_properties_target_stays_properties(tmp_path, capsys):
    out = tmp_path / "takeover.properties"
    code = main(
        [
            "--input",
            str(_offsets_file(tmp_path)),
            "--target-config",
            str(target(tmp_path, suffix=".properties")),
            "--output",
            str(out),
        ],
        db=db_ok(),
    )
    assert code == EXIT_FOLLOW_UPS
    assert parse_properties(out.read_text())["cdc.start.scn"] == "2500"


def test_without_a_target_only_the_two_settings_are_written(tmp_path, capsys):
    out = tmp_path / "start.json"
    main(["--input", str(_offsets_file(tmp_path)), "--output", str(out)], db=db_ok())
    assert json.loads(out.read_text()) == {
        "cdc.snapshot.mode": "none",
        "cdc.start.scn": "2500",
    }


def _offsets_file(tmp_path):
    src = tmp_path / "offsets.json"
    src.write_text(json.dumps({"offsets": DEBEZIUM_OFFSETS}))
    return src


def test_refuses_a_connector_that_is_only_paused(tmp_path, capsys):
    code = main(args(tmp_path), api=connect_with_old("PAUSED"), db=db_ok())
    assert code == EXIT_FAILURE
    assert "pausing is not enough" in capsys.readouterr().err


def test_refuses_when_the_archived_log_is_gone(tmp_path, capsys):
    logs = contiguous_logs(1, 10, [1000, 2000, 3000, 4000])
    logs[1] = LogFile(1, 11, 2000, 3000, False)  # holds SCN 2500, deleted
    out = tmp_path / "takeover.json"
    code = main(
        args(tmp_path, "--output", str(out)), api=connect_with_old(), db=FakeTakeoverDb(logs)
    )
    result = json.loads(capsys.readouterr().out)
    assert code == EXIT_FAILURE
    assert result["start_scn"] is None
    assert result["settings"] is None
    assert result["problems"]
    assert not out.exists()


def test_refuses_an_offset_ahead_of_the_database(tmp_path, capsys):
    db = FakeTakeoverDb(contiguous_logs(1, 10, [1000, 2000]), current=2000)
    assert main(args(tmp_path), api=connect_with_old(), db=db) == EXIT_FAILURE
    assert "different database" in capsys.readouterr().err


def test_refuses_an_offset_from_an_earlier_incarnation(tmp_path, capsys):
    db = FakeTakeoverDb(contiguous_logs(1, 10, [3000, 4000]), resetlogs=3000, current=5000)
    assert main(args(tmp_path), api=connect_with_old(), db=db) == EXIT_FAILURE
    assert "earlier incarnation" in capsys.readouterr().err


def test_refuses_the_same_name_for_old_and_new(tmp_path, capsys):
    argv = [*args(tmp_path), "--target-connector", OLD]
    assert main(argv, api=connect_with_old(), db=db_ok()) == EXIT_FAILURE


def test_a_new_connector_with_an_offset_would_ignore_the_start_scn(tmp_path, capsys):
    api = connect_with_old()
    api.states[NEW] = "RUNNING"
    api.offset_entries[NEW] = [{"partition": {"server": "server1"}, "offset": {"v": 1}}]
    code = main(args(tmp_path), api=api, db=db_ok())
    result = json.loads(capsys.readouterr().out)
    assert code == EXIT_FAILURE
    assert any("would ignore" in p for p in result["problems"])


def test_an_existing_new_connector_without_an_offset_is_a_follow_up(tmp_path, capsys):
    api = connect_with_old()
    api.states[NEW] = "STOPPED"
    assert main(args(tmp_path), api=api, db=db_ok()) == EXIT_FOLLOW_UPS


def test_offsets_from_a_file_cannot_prove_the_stop(tmp_path, capsys):
    code = main(["--input", str(_offsets_file(tmp_path)), "--json"], db=db_ok())
    result = json.loads(capsys.readouterr().out)
    assert code == EXIT_FOLLOW_UPS
    assert any("could not confirm" in f for f in result["follow_ups"])


def test_confluent_offsets_and_a_report(tmp_path, capsys):
    src = tmp_path / "offsets.json"
    src.write_text(
        json.dumps(
            {
                "offsets": [
                    {
                        "partition": {"sidPdb": "ORCLCDB.ORCLPDB1"},
                        "offset": {"scn": "2169", "tablePlacement": ""},
                    }
                ]
            }
        )
    )
    report = tmp_path / "takeover.md"
    code = main(
        [
            "--input",
            str(src),
            "--target-config",
            str(target(tmp_path, "c-oso")),
            "--report",
            str(report),
        ],
        db=db_ok(),
    )
    assert code == EXIT_FOLLOW_UPS
    text = report.read_text()
    assert "| Start SCN | 2169" in text
    assert "cdc.start.scn=2169" in text


def test_rac_is_a_follow_up(tmp_path, capsys):
    logs = contiguous_logs(1, 10, [1000, 3000]) + contiguous_logs(2, 20, [1000, 3000])
    code = main(args(tmp_path), api=connect_with_old(), db=FakeTakeoverDb(logs))
    assert code == EXIT_FOLLOW_UPS
    assert any("RAC" in f for f in json.loads(capsys.readouterr().out)["follow_ups"])


def test_noarchivelog_is_a_problem(tmp_path, capsys):
    db = FakeTakeoverDb(contiguous_logs(1, 10, [1000, 4000]), log_mode="NOARCHIVELOG")
    assert main(args(tmp_path), api=connect_with_old(), db=db) == EXIT_FAILURE
