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
"""PRD-04 s7: every Debezium property of PRD-01 Appendix A and every Confluent property of
research/confluent_oracle_connectors_detail.md section 1.4 has a classification and a golden test.
"""

from __future__ import annotations

import json

import pytest

from oso_cdc_migration import translate as t
from oso_cdc_migration.confluent import RULES as CONFLUENT_RULES
from oso_cdc_migration.debezium import RULES as DEBEZIUM_RULES

from .golden_support import GOLDEN

# PRD-01 Appendix A, with each family expanded to the properties Debezium documents
APPENDIX_A = [
    "database.hostname",
    "database.port",
    "database.user",
    "database.password",
    "database.url",
    "database.dbname",
    "database.pdb.name",
    "topic.prefix",
    "table.include.list",
    "table.exclude.list",
    "schema.include.list",
    "schema.exclude.list",
    "column.exclude.list",
    "log.mining.username.exclude.list",
    "log.mining.username.include.list",
    "snapshot.mode",
    "snapshot.max.threads",
    "log.mining.strategy",
    "log.mining.batch.size.min",
    "log.mining.batch.size.max",
    "log.mining.batch.size.default",
    "log.mining.sleep.time.min.ms",
    "log.mining.sleep.time.max.ms",
    "log.mining.sleep.time.default.ms",
    "log.mining.sleep.time.increment.ms",
    "log.mining.query.filter.mode",
    "log.mining.buffer.type",
    "log.mining.buffer.drop.on.stop",
    "log.mining.transaction.retention.ms",
    "log.mining.archive.log.only.mode",
    "log.mining.archive.destination.name",
    "archive.destination.name",
    "lob.enabled",
    "unavailable.value.placeholder",
    "decimal.handling.mode",
    "time.precision.mode",
    "binary.handling.mode",
    "tombstones.on.delete",
    "heartbeat.interval.ms",
    "heartbeat.action.query",
    "schema.history.internal.kafka.topic",
    "schema.history.internal.kafka.bootstrap.servers",
    "signal.data.collection",
    "rac.nodes",
    "database.connection.adapter",
    "event.processing.failure.handling.mode",
]

# research/confluent_oracle_connectors_detail.md section 1.4, families expanded
SECTION_1_4 = [
    "oracle.server",
    "oracle.port",
    "oracle.username",
    "oracle.password",
    "oracle.sid",
    "oracle.pdb.name",
    "oracle.service.name",
    "key.converter",
    "value.converter",
    "table.inclusion.regex",
    "table.exclusion.regex",
    "table.topic.name.template",
    "redo.log.topic.name",
    "redo.log.consumer.bootstrap.servers",
    "redo.log.consumer.fetch.min.bytes",
    "redo.log.consumer.max.partition.fetch.bytes",
    "redo.log.consumer.fetch.max.bytes",
    "redo.log.consumer.max.poll.records",
    "redo.log.consumer.request.timeout.ms",
    "redo.log.consumer.receive.buffer.bytes",
    "redo.log.consumer.send.buffer.bytes",
    "start.from",
    "key.template",
    "max.batch.size",
    "query.timeout.ms",
    "max.retry.time.ms",
    "redo.log.poll.interval.ms",
    "snapshot.row.fetch.size",
    "redo.log.row.fetch.size",
    "poll.linger.ms",
    "snapshot.threads.per.task",
    "heartbeat.interval.ms",
    "heartbeat.topic.name",
    "use.transaction.begin.for.mining.session",
    "log.mining.transaction.age.threshold.ms",
    "log.mining.transaction.threshold.breached.action",
    "redo.log.corruption.topic",
    "behavior.on.dictionary.mismatch",
    "behavior.on.unparsable.statement",
    "oracle.dictionary.mode",
    "log.mining.archive.destination.name",
    "record.buffer.mode",
    "max.batch.timeout.ms",
    "max.buffer.size",
    "lob.topic.name.template",
    "enable.large.lob.object.support",
    "log.sensitive.data",
    "numeric.mapping",
    "numeric.default.scale",
    "oracle.date.mapping",
    "emit.tombstone.on.delete",
    "oracle.fan.events.enable",
    "table.task.reconfig.checking.interval.ms",
    "table.rps.logging.interval.ms",
    "log.mining.end.scn.deviation.ms",
    "output.before.state.field",
    "output.table.name.field",
    "output.scn.field",
    "output.commit.scn.field",
    "output.op.type.field",
    "output.op.ts.field",
    "output.current.ts.field",
    "output.row.id.field",
    "output.username.field",
    "output.redo.field",
    "output.undo.field",
    "output.op.type.read.value",
    "output.op.type.insert.value",
    "output.op.type.update.value",
    "output.op.type.delete.value",
    "output.op.type.truncate.value",
    "redo.log.startup.polling.limit.ms",
    "snapshot.by.table.partitions",
    "oracle.validation.result.fetch.size",
    "redo.log.row.poll.fields.include",
    "redo.log.row.poll.fields.exclude",
    "redo.log.row.poll.username.include",
    "redo.log.row.poll.username.exclude",
    "db.timezone",
    "db.timezone.date",
    "oracle.supplemental.log.level",
    "ldap.url",
    "ldap.security.principal",
    "ldap.security.credentials",
    "oracle.ssl.truststore.file",
    "oracle.ssl.truststore.password",
    "oracle.kerberos.cache.file",
    "retry.error.codes",
    "enable.metrics.collection",
    "confluent.license",
    "confluent.topic.bootstrap.servers",
    "confluent.topic.replication.factor",
    "connection.pool.initial.size",
    "connection.pool.min.size",
    "connection.pool.max.size",
    "redo.log.initial.delay.interval.ms",
]


def _classified(kind: str, case: str) -> dict[str, str]:
    report = json.loads((GOLDEN / kind / case / "expected" / "report.json").read_text())
    return {p["key"]: p["classification"] for p in report["properties"]}


@pytest.mark.parametrize("key", APPENDIX_A)
def test_every_appendix_a_property_has_a_rule_and_a_golden(key):
    assert DEBEZIUM_RULES.find(key) is not t.unknown
    assert key in _classified("debezium", "appendix-a")


@pytest.mark.parametrize("key", SECTION_1_4)
def test_every_confluent_property_has_a_rule_and_a_golden(key):
    assert CONFLUENT_RULES.find(key) is not t.unknown
    assert key in _classified("confluent", "reference-1-4")


def test_every_source_property_is_reported_with_a_known_classification():
    for kind in ("debezium", "confluent"):
        for case in (GOLDEN / kind).iterdir():
            report = json.loads((case / "expected" / "report.json").read_text())
            source = json.loads((case / "expected" / "report.json").read_text())
            classes = {p["classification"] for p in report["properties"]}
            assert classes <= {"mapped", "mapped-with-change", "dropped", "manual"}
            for p in source["properties"]:
                if p["classification"] in ("dropped", "manual"):
                    assert p["note"], f"{case.name}: {p['key']} has no reason or instruction"
