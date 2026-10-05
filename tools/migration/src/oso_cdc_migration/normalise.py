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
"""The canonical text of a column value, from the database and from a change record.

This is the Python counterpart of ``bench/.../check/Normaliser.java`` (ADR-0012): numbers lose
trailing zeros, dates and timestamps become ``LocalDateTime`` text, zoned timestamps ``Instant``
text in UTC, intervals microseconds, binaries lower-case hex, BINARY_FLOAT and BINARY_DOUBLE the
text of Java's ``Float.toString`` and ``Double.toString``. Both sides are normalised by this one
module, per ``cdc.decimal.mode`` and ``cdc.temporal.mode`` (PRD-04 VER-3). Interval day to second
values are converted exactly rather than through a double.
"""

from __future__ import annotations

import base64
import binascii
import math
import re
import struct
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Context, Decimal, InvalidOperation
from typing import Any

MICROS_PER_MONTH = 365.25 / 12 * 24 * 60 * 60 * 1_000_000.0
_EPOCH = datetime(1970, 1, 1)
_WIDE = Context(prec=200)

DECIMAL = "org.apache.kafka.connect.data.Decimal"
TIMESTAMP_MS = "io.debezium.time.Timestamp"
TIMESTAMP_US = "io.debezium.time.MicroTimestamp"
TIMESTAMP_NS = "io.debezium.time.NanoTimestamp"


class NormaliseError(Exception):
    """A value that cannot be brought to canonical form."""


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    precision: int | None = None
    scale: int | None = None

    @property
    def kind(self) -> str:
        return kind_of(self.data_type)

    @property
    def is_lob(self) -> bool:
        return self.data_type.upper() in ("CLOB", "NCLOB", "BLOB", "XMLTYPE")

    @property
    def fraction_digits(self) -> int:
        m = re.search(r"TIMESTAMP\((\d)\)", self.data_type.upper())
        if m:
            return int(m.group(1))
        return 6 if self.scale is None or self.scale < 0 else self.scale


def kind_of(data_type: str) -> str:
    t = data_type.upper().strip()
    base = re.sub(r"\(.*?\)", "", t).strip()
    if base in ("NUMBER", "FLOAT", "INTEGER", "INT", "SMALLINT", "DECIMAL", "NUMERIC", "REAL"):
        return "number"
    if base == "BINARY_FLOAT":
        return "binary_float"
    if base == "BINARY_DOUBLE":
        return "binary_double"
    if base == "DATE":
        return "date"
    if base.startswith("TIMESTAMP"):
        return "timestamp_tz" if "TIME ZONE" in base else "timestamp"
    if base.startswith("INTERVAL YEAR"):
        return "interval_ym"
    if base.startswith("INTERVAL DAY"):
        return "interval_ds"
    if base in ("RAW", "LONG RAW", "BLOB"):
        return "binary"
    if base in (
        "VARCHAR2",
        "VARCHAR",
        "NVARCHAR2",
        "CHAR",
        "NCHAR",
        "CLOB",
        "NCLOB",
        "LONG",
        "ROWID",
        "UROWID",
    ):
        return "text"
    if base == "XMLTYPE":
        return "xml"
    return "unsupported"


# Java text forms --------------------------------------------------------------------------------


def java_round(x: float) -> int:
    """``Math.round(double)``: half up."""
    return math.floor(x + 0.5)


def _java_digits(text: str) -> str:
    d = Decimal(text)
    sign = "-" if d.is_signed() else ""
    t = abs(d).normalize(_WIDE).as_tuple()
    digits = "".join(map(str, t.digits)) or "0"
    exp10 = t.exponent + len(t.digits) - 1
    if -3 <= exp10 < 7:
        point = exp10 + 1
        if point <= 0:
            whole, frac = "0", "0" * (-point) + digits
        elif point >= len(digits):
            whole, frac = digits + "0" * (point - len(digits)), "0"
        else:
            whole, frac = digits[:point], digits[point:]
        return f"{sign}{whole}.{frac}"
    return f"{sign}{digits[0]}.{digits[1:] or '0'}E{exp10}"


def _special(x: float) -> str | None:
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    if x == 0:
        return "-0.0" if math.copysign(1.0, x) < 0 else "0.0"
    return None


def java_double_text(x: float) -> str:
    """``Double.toString``: the shortest digits that identify the double."""
    return _special(x) or _java_digits(repr(x))


def to_float32(x: float) -> float:
    return struct.unpack("<f", struct.pack("<f", x))[0]


def java_float_text(x: float) -> str:
    """``Float.toString``: the shortest digits that identify the float."""
    f = to_float32(x)
    special = _special(f)
    if special:
        return special
    for p in range(1, 10):
        s = f"{f:.{p}g}"
        try:
            if to_float32(float(s)) == f:
                return _java_digits(s)
        except OverflowError:  # rounding up past the largest float
            continue
    return _java_digits(repr(f))  # pragma: no cover - nine digits always identify a float


def _date_text(y: int, mo: int, d: int) -> str:
    if y > 9999:
        return f"+{y}-{mo:02d}-{d:02d}"
    if y < 0:
        return f"-{-y:04d}-{mo:02d}-{d:02d}"
    return f"{y:04d}-{mo:02d}-{d:02d}"


def _fraction(nanos: int) -> str:
    if nanos == 0:
        return ""
    if nanos % 1_000_000 == 0:
        return f".{nanos // 1_000_000:03d}"
    if nanos % 1000 == 0:
        return f".{nanos // 1000:06d}"
    return f".{nanos:09d}"


def local_text(y: int, mo: int, d: int, h: int, mi: int, s: int, nanos: int) -> str:
    """``LocalDateTime.toString``: seconds and fraction only when not zero."""
    time = f"{h:02d}:{mi:02d}"
    if s or nanos:
        time += f":{s:02d}" + _fraction(nanos)
    return f"{_date_text(y, mo, d)}T{time}"


def instant_text(y: int, mo: int, d: int, h: int, mi: int, s: int, nanos: int) -> str:
    """``Instant.toString``: seconds always, the fraction in groups of three digits."""
    return f"{_date_text(y, mo, d)}T{h:02d}:{mi:02d}:{s:02d}{_fraction(nanos)}Z"


_LOCAL = re.compile(
    r"^\s*([+-]?\d{4,})-(\d{1,2})-(\d{1,2})[T ](\d{1,2}):(\d{2})(?::(\d{2})(?:[.,](\d{1,9}))?)?"
)
_ZONE = re.compile(r"(Z|[+-]\d{2}:?\d{2}(?::?\d{2})?)(\[[^\]]*\])?\s*$")


def _parse_local(text: str) -> tuple[int, int, int, int, int, int, int, str]:
    m = _LOCAL.match(text)
    if not m:
        raise NormaliseError(f"not a date and time: {text!r}")
    y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
    s = int(m.group(6) or 0)
    frac = (m.group(7) or "").ljust(9, "0")
    return y, mo, d, h, mi, s, int(frac or 0), text[m.end() :]


def _to_utc(parts: tuple[int, ...], offset_seconds: int) -> tuple[int, ...]:
    y, mo, d, h, mi, s, nanos = parts
    dt = datetime(y, mo, d, h, mi, s) - timedelta(seconds=offset_seconds)
    return dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, nanos


def _offset_seconds(zone: str) -> int:
    if zone in ("Z", ""):
        return 0
    sign = -1 if zone[0] == "-" else 1
    digits = zone[1:].replace(":", "")
    h, mi = int(digits[0:2]), int(digits[2:4])
    s = int(digits[4:6]) if len(digits) >= 6 else 0
    return sign * (h * 3600 + mi * 60 + s)


def _epoch_parts(n: int, unit: int) -> tuple[int, ...]:
    seconds, fraction = divmod(n, unit)
    dt = _EPOCH + timedelta(seconds=seconds)
    nanos = fraction * (1_000_000_000 // unit)
    return dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, nanos


def interval_micros(text: str, year_month: bool) -> str:
    """``+12-03`` or ``+05 04:03:02.123456`` (Oracle TO_CHAR) to microseconds."""
    s = text.strip()
    sign = -1 if s.startswith("-") else 1
    s = s.lstrip("+-")
    if year_month:
        years, months = s.split("-")
        total = int(years) * 12 + int(months)
        return str(java_round(sign * total * MICROS_PER_MONTH))
    days, clock = s.split(" ")
    h, mi, sec = clock.split(":")
    nanos = (int(days) * 86400 + int(h) * 3600 + int(mi) * 60) * 1_000_000_000 + int(
        Decimal(sec) * 1_000_000_000
    )
    return str(sign * (nanos // 1000))


_PERIOD = re.compile(r"^P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)W)?(?:(-?\d+)D)?$")
_DURATION = re.compile(r"^PT(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d+(?:\.\d+)?)S)?$")


def iso_interval_micros(text: str) -> str:
    """``Period.toString`` or ``Duration.toString`` (iso_string mode) to microseconds."""
    s = text.strip()
    m = _DURATION.match(s)
    if m:
        h, mi, sec = m.group(1), m.group(2), m.group(3)
        nanos = (int(h or 0) * 3600 + int(mi or 0) * 60) * 1_000_000_000
        nanos += int(Decimal(sec or "0") * 1_000_000_000)
        q = abs(nanos) // 1000
        return str(q if nanos >= 0 else -q)
    m = _PERIOD.match(s)
    if m:
        y, mo, w, d = (int(g or 0) for g in m.groups())
        months = y * 12 + mo
        return str(java_round(months * MICROS_PER_MONTH + (d + 7 * w) * 86_400_000_000.0))
    raise NormaliseError(f"not an ISO 8601 period or duration: {text!r}")


def number_text(d: Decimal) -> str:
    if d.is_zero():
        return "0"
    return format(d.normalize(_WIDE), "f")


def _decode_base64(text: str) -> bytes:
    try:
        return base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError):
        raise NormaliseError(f"not base64: {text!r}") from None


def _unscaled(raw: bytes | str, scale: int) -> Decimal:
    data = raw if isinstance(raw, bytes | bytearray) else _decode_base64(raw)
    return Decimal(int.from_bytes(data, "big", signed=True)).scaleb(-scale, _WIDE)


def _schema_name(schema: dict | None) -> str | None:
    return schema.get("name") if isinstance(schema, dict) else None


def _schema_scale(schema: dict | None) -> int | None:
    if _schema_name(schema) == DECIMAL:
        params = schema.get("parameters") or {}
        if "scale" in params:
            return int(params["scale"])
    return None


class Normaliser:
    def __init__(self, decimal_mode: str = "precise", temporal_mode: str = "adaptive") -> None:
        self.decimal_mode = decimal_mode.lower()
        self.temporal_mode = temporal_mode.lower()

    # numbers

    def _number(self, d: Decimal) -> str:
        if self.decimal_mode == "double":
            d = Decimal(repr(float(d)))
        return number_text(d)

    def _record_decimal(self, v: Any, schema: dict | None) -> Decimal:
        if isinstance(v, bool):
            raise NormaliseError(f"not a number: {v!r}")
        if isinstance(v, Decimal):
            return v
        if isinstance(v, int):
            return Decimal(v)
        if isinstance(v, float):
            return Decimal(repr(v))
        if isinstance(v, dict) and "scale" in v and "value" in v:  # VariableScaleDecimal
            return _unscaled(v["value"], int(v["scale"]))
        scale = _schema_scale(schema)
        if isinstance(v, bytes | bytearray):
            if scale is None:
                raise NormaliseError("a decimal as bytes without a scale")
            return _unscaled(bytes(v), scale)
        if isinstance(v, str):
            if scale is not None:
                return _unscaled(v, scale)
            if self.decimal_mode == "precise":
                raise NormaliseError(
                    "a decimal encoded as base64 without its schema; read topics written with"
                    " schemas.enable=true or decimal.format=NUMERIC"
                )
            try:
                return Decimal(v.strip())
            except InvalidOperation:
                raise NormaliseError(f"not a number: {v!r}") from None
        raise NormaliseError(f"not a number: {v!r}")

    # database side

    def from_database(self, value: Any, col: Column) -> str | None:
        if value is None:
            return None
        kind = col.kind
        if kind == "number":
            if isinstance(value, float):
                value = Decimal(repr(value))
            return self._number(Decimal(value))
        if kind == "binary_float":
            return java_float_text(float(value))
        if kind == "binary_double":
            return java_double_text(float(value))
        if kind in ("date", "timestamp"):
            if isinstance(value, datetime):
                parts = (value.year, value.month, value.day, value.hour, value.minute)
                rest = (value.second, value.microsecond * 1000)
                return local_text(*parts, *rest)
            y, mo, d, h, mi, s, nanos, _ = _parse_local(str(value))
            return local_text(y, mo, d, h, mi, s, 0 if kind == "date" else nanos)
        if kind == "timestamp_tz":
            if isinstance(value, datetime):
                if value.tzinfo is not None:
                    value = value.astimezone(UTC).replace(tzinfo=None)
                return instant_text(
                    value.year,
                    value.month,
                    value.day,
                    value.hour,
                    value.minute,
                    value.second,
                    value.microsecond * 1000,
                )
            y, mo, d, h, mi, s, nanos, _ = _parse_local(str(value))  # already UTC
            return instant_text(y, mo, d, h, mi, s, nanos)
        if kind in ("interval_ym", "interval_ds"):
            return interval_micros(str(value), kind == "interval_ym")
        if kind == "binary":
            return bytes(value).hex()
        if kind in ("text", "xml"):
            return str(value)
        raise NormaliseError(f"column {col.name} has type {col.data_type}, which is not compared")

    # record side

    def from_record(self, value: Any, col: Column, schema: dict | None = None) -> str | None:
        if value is None:
            return None
        kind = col.kind
        if kind == "binary_float":
            return java_float_text(float(value))
        if kind == "binary_double":
            return java_double_text(float(value))
        if kind == "number":
            return self._number(self._record_decimal(value, schema))
        iso = isinstance(value, str)
        if kind in ("date", "timestamp"):
            if iso:
                y, mo, d, h, mi, s, nanos, _ = _parse_local(value)
                return local_text(y, mo, d, h, mi, s, nanos)
            n = int(value)
            name = _schema_name(schema)
            if kind == "date" or name == TIMESTAMP_MS:
                unit = 1_000
            elif name == TIMESTAMP_US:
                unit = 1_000_000
            elif name == TIMESTAMP_NS:
                unit = 1_000_000_000
            else:
                p = col.fraction_digits
                unit = 1_000 if p <= 3 else 1_000_000 if p <= 6 else 1_000_000_000
            return local_text(*_epoch_parts(n, unit))
        if kind == "timestamp_tz":
            parts = _parse_local(str(value))
            utc = _to_utc(parts[:7], _offset_seconds(_zone(parts[7], value)))
            return instant_text(*utc)
        if kind in ("interval_ym", "interval_ds"):
            if iso:
                return iso_interval_micros(value)
            return str(java_round(float(value)))
        if kind == "binary":
            if isinstance(value, bytes | bytearray):
                return bytes(value).hex()
            return _decode_base64(str(value)).hex()
        if kind in ("text", "xml"):
            return value if isinstance(value, str) else str(value)
        raise NormaliseError(f"column {col.name} has type {col.data_type}, which is not compared")


def _zone(rest: str, original: Any) -> str:
    m = _ZONE.search(rest)
    if not m:
        raise NormaliseError(f"a zoned timestamp without an offset: {original!r}")
    return m.group(1)
