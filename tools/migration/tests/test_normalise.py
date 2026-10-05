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
"""Canonical forms, matching bench/.../check/Normaliser.java (ADR-0012)."""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from oso_cdc_migration.normalise import (
    Column,
    NormaliseError,
    Normaliser,
    interval_micros,
    iso_interval_micros,
    java_double_text,
    java_float_text,
    kind_of,
    number_text,
)

N = Normaliser()
BIG = "12345678901234567890123456789012345678"
TINY = "0." + "0" * 37 + "1"


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (1.0, "1.0"),
        (100.0, "100.0"),
        (1e7, "1.0E7"),
        (9999999.0, "9999999.0"),
        (0.001, "0.001"),
        (0.0001, "1.0E-4"),
        (123.456, "123.456"),
        (-1.5e-10, "-1.5E-10"),
        (0.0, "0.0"),
        (-0.0, "-0.0"),
        (float("nan"), "NaN"),
        (float("-inf"), "-Infinity"),
    ],
)
def test_double_text_matches_java(value, text):
    assert java_double_text(value) == text


@pytest.mark.parametrize(
    ("value", "text"),
    [(1.1, "1.1"), (0.1, "0.1"), (3.4028235e38, "3.4028235E38"), (16777216.0, "1.6777216E7")],
)
def test_float_text_matches_java(value, text):
    assert java_float_text(value) == text


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (Decimal("100.00"), "100"),
        (Decimal("-0.000"), "0"),
        (Decimal("1.2300"), "1.23"),
        (Decimal("1E+3"), "1000"),
        (Decimal(BIG), BIG),
        (Decimal(TINY), TINY),
    ],
)
def test_numbers_lose_trailing_zeros(value, text):
    assert number_text(value) == text


def test_kinds():
    assert kind_of("NUMBER") == "number"
    assert kind_of("TIMESTAMP(6) WITH LOCAL TIME ZONE") == "timestamp_tz"
    assert kind_of("TIMESTAMP(9)") == "timestamp"
    assert kind_of("INTERVAL DAY(2) TO SECOND(6)") == "interval_ds"
    assert kind_of("INTERVAL YEAR(2) TO MONTH") == "interval_ym"
    assert kind_of("RAW") == "binary"
    assert kind_of("JSON") == "unsupported"


NUM = Column("N", "NUMBER", 10, 2)


def test_record_numbers_in_every_encoding():
    assert N.from_record(Decimal("12.50"), NUM) == "12.5"
    assert N.from_record(42, Column("I", "NUMBER", 9, 0)) == "42"
    scaled = base64.b64encode((1250).to_bytes(2, "big", signed=True)).decode()
    schema = {"name": "org.apache.kafka.connect.data.Decimal", "parameters": {"scale": "2"}}
    assert N.from_record(scaled, NUM, schema) == "12.5"
    negative = base64.b64encode((-5).to_bytes(1, "big", signed=True)).decode()
    vsd = {"scale": 1, "value": negative}
    assert N.from_record(vsd, Column("V", "NUMBER")) == "-0.5"


def test_base64_decimal_without_schema_is_refused_in_precise_mode():
    with pytest.raises(NormaliseError, match="base64"):
        N.from_record("BOI=", NUM)


def test_string_and_double_modes():
    assert Normaliser("string").from_record("12.50", NUM) == "12.5"
    double = Normaliser("double")
    assert double.from_record(Decimal("0.1"), NUM) == "0.1"
    assert double.from_database(Decimal("0.1000000000000000000001"), NUM) == "0.1"
    assert double.from_record(Decimal("1.0E20"), NUM) == double.from_database(
        Decimal("100000000000000000000"), NUM
    )


def test_database_and_record_agree_on_temporals():
    date = Column("D", "DATE")
    assert N.from_database("2024-03-01T10:00:00", date) == "2024-03-01T10:00"
    assert N.from_record(1709287200000, date) == "2024-03-01T10:00"

    ts6 = Column("T", "TIMESTAMP(6)", None, 6)
    assert N.from_database("2024-03-01T10:00:05.123456000", ts6) == "2024-03-01T10:00:05.123456"
    assert N.from_record(1709287205123456, ts6) == "2024-03-01T10:00:05.123456"
    micro = {"name": "io.debezium.time.MicroTimestamp"}
    assert N.from_record(1709287205123456, Column("T", "TIMESTAMP"), micro).endswith(".123456")

    ts9 = Column("T", "TIMESTAMP(9)", None, 9)
    assert N.from_database("2024-03-01T10:00:05.123456789", ts9) == "2024-03-01T10:00:05.123456789"
    assert N.from_record(1709287205123456789, ts9) == "2024-03-01T10:00:05.123456789"

    ts3 = Column("T", "TIMESTAMP(3)", None, 3)
    assert N.from_record(1709287205120, ts3) == "2024-03-01T10:00:05.120"
    assert N.from_database(datetime(2024, 3, 1, 10, 0, 5, 120000), ts3) == "2024-03-01T10:00:05.120"

    before_epoch = N.from_record(-1, Column("D", "DATE"))
    assert before_epoch == "1969-12-31T23:59:59.999"


def test_zoned_timestamps_become_utc_instants():
    tz = Column("Z", "TIMESTAMP(6) WITH TIME ZONE")
    assert N.from_record("2024-03-01T12:00:00.5+02:00", tz) == "2024-03-01T10:00:00.500Z"
    assert N.from_record("2024-03-01T10:00:00Z", tz) == "2024-03-01T10:00:00Z"
    assert N.from_database("2024-03-01T10:00:00.500000000", tz) == "2024-03-01T10:00:00.500Z"
    aware = datetime(2024, 3, 1, 10, 0, tzinfo=UTC)
    assert N.from_database(aware, tz) == "2024-03-01T10:00:00Z"
    with pytest.raises(NormaliseError):
        N.from_record("2024-03-01T10:00:00", tz)


def test_iso_string_mode_reads_text():
    iso = Normaliser(temporal_mode="iso_string")
    assert iso.from_record("2024-03-01T10:00", Column("D", "DATE")) == "2024-03-01T10:00"
    ds = Column("I", "INTERVAL DAY(2) TO SECOND(6)")
    assert iso.from_record("PT122H3M2.123456S", ds) == N.from_database("+05 02:03:02.123456", ds)
    ym = Column("Y", "INTERVAL YEAR(2) TO MONTH")
    assert iso.from_record("P1Y2M", ym) == N.from_database("+01-02", ym)


def test_intervals():
    assert interval_micros("-05 04:03:02.123456", False) == "-446582123456"
    assert interval_micros("+01-02", True) == "36817200000000"
    assert iso_interval_micros("PT-0.5S") == "-500000"
    assert iso_interval_micros("P0D") == "0"
    ds = Column("I", "INTERVAL DAY(2) TO SECOND(6)")
    assert N.from_record(Decimal("4.46582123456E11"), ds) == "446582123456"


def test_binary_and_floats():
    raw = Column("R", "RAW")
    assert N.from_database(b"\x01\xab", raw) == "01ab"
    assert N.from_record(base64.b64encode(b"\x01\xab").decode(), raw) == "01ab"
    bf = Column("F", "BINARY_FLOAT")
    assert N.from_record(Decimal("1.1"), bf) == N.from_database(1.100000023841858, bf) == "1.1"
    assert N.from_record(Decimal("2.5"), Column("D", "BINARY_DOUBLE")) == "2.5"


def test_nulls_and_text():
    assert N.from_database(None, NUM) is None
    assert N.from_record(None, NUM) is None
    assert N.from_record("abc ", Column("C", "CHAR")) == "abc "
    with pytest.raises(NormaliseError):
        N.from_database("x", Column("J", "JSON"))
