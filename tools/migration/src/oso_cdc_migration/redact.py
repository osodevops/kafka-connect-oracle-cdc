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
"""Secrets are never written to outputs, reports or logs (PRD-04 MIG-7).

Config provider references such as ``${file:/etc/connect/secrets.properties:password}`` are not
secrets and are kept as they are. Literal secrets are masked by key, and every text a tool writes
also passes through :meth:`Redactor.scrub`, which replaces any registered secret value wherever it
appears, so a secret that leaks into a message (an exception text, a URL) is still masked.
"""

from __future__ import annotations

import logging
import re

MASK = "********"

# Keys whose values are secrets. Matched anywhere in the key, case-insensitively.
_SECRET_KEY = re.compile(
    r"password|passwd|secret|credential|jaas\.config|user\.info|license|licence|api\.key"
    r"|apikey|token|private\.key",
    re.IGNORECASE,
)
_PROVIDER_REFERENCE = re.compile(r"^\$\{[A-Za-z0-9_.\-]+:[^{}]*\}$")
# jdbc:oracle:thin:scott/tiger@host:1521/service carries a password in the URL
_URL_CREDENTIALS = re.compile(r"(jdbc:oracle:[a-z]+:)([^/@:\s\"]+)/([^@\s\"]+)@", re.IGNORECASE)

# Shorter values are masked where they appear as configuration values, but are not scrubbed from
# free text, where they would mask unrelated characters.
MIN_SCRUB_LENGTH = 4


def is_secret_key(key: str) -> bool:
    return bool(_SECRET_KEY.search(key))


def is_provider_reference(value: str) -> bool:
    return bool(_PROVIDER_REFERENCE.match(value.strip()))


def url_password(value: str) -> str | None:
    """The password embedded in an Oracle JDBC URL, if there is one."""
    m = _URL_CREDENTIALS.search(value)
    return m.group(3) if m else None


def mask_url(value: str) -> str:
    return _URL_CREDENTIALS.sub(lambda m: f"{m.group(1)}{m.group(2)}/{MASK}@", value)


class Redactor:
    """Remembers secret values and removes them from any text."""

    def __init__(self) -> None:
        self._secrets: set[str] = set()

    def add(self, value: str | None) -> None:
        if value is None:
            return
        value = str(value)
        if not value or is_provider_reference(value):
            return
        self._secrets.add(value)

    def register_config(self, config: dict[str, str]) -> None:
        for key, value in config.items():
            if is_secret_key(key):
                self.add(value)
            password = url_password(value)
            if password:
                self.add(password)

    def is_secret(self, value: str) -> bool:
        return value in self._secrets

    def scrub(self, text: str) -> str:
        for secret in sorted(self._secrets, key=len, reverse=True):
            if len(secret) >= MIN_SCRUB_LENGTH:
                text = text.replace(secret, MASK)
        return mask_url(text)

    def display(self, key: str, value: str) -> str:
        """A configuration value as it may appear in a report."""
        if is_provider_reference(value):
            return value
        if is_secret_key(key) or value in self._secrets:
            return MASK
        return self.scrub(value)


class RedactingFilter(logging.Filter):
    """Formats each log record and scrubs it, including any exception text."""

    def __init__(self, redactor: Redactor) -> None:
        super().__init__()
        self._redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self._redactor.scrub(record.getMessage())
        record.args = None
        if record.exc_info:
            text = logging.Formatter().formatException(record.exc_info)
            record.exc_text = self._redactor.scrub(text)
            record.exc_info = None
        return True
