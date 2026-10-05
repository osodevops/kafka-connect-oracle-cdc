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
"""A small Kafka Connect REST client: status, offsets (KIP-875) and config validation."""

from __future__ import annotations

import base64
import contextlib
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol

from . import OSO_CONNECTOR_CLASS
from .translate import EosCheck


class ConnectError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class ConnectApi(Protocol):
    def status(self, connector: str) -> dict[str, Any]: ...

    def offsets(self, connector: str) -> dict[str, Any]: ...

    def validate(self, plugin: str, config: dict[str, str]) -> dict[str, Any]: ...


class ConnectClient:
    """Talks to one Connect worker. ``auth`` is ``user:password`` for HTTP basic auth."""

    def __init__(self, base_url: str, auth: str | None = None, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._auth = (
            "Basic " + base64.b64encode(auth.encode("utf-8")).decode("ascii") if auth else None
        )

    def _request(self, method: str, path: str, body: Any = None) -> Any:
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.base_url + path, data=data, method=method)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if self._auth:
            req.add_header("Authorization", self._auth)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            detail = ""
            with contextlib.suppress(ValueError, AttributeError, OSError):
                detail = json.loads(e.read().decode("utf-8")).get("message", "")
            raise ConnectError(
                f"{method} {path} returned HTTP {e.code}" + (f": {detail}" if detail else ""),
                e.code,
            ) from None
        except (urllib.error.URLError, OSError) as e:
            reason = getattr(e, "reason", e)
            raise ConnectError(f"{method} {path} failed: {reason}") from None
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError:
            raise ConnectError(f"{method} {path} did not return JSON") from None

    @staticmethod
    def _name(connector: str) -> str:
        return urllib.parse.quote(connector, safe="")

    def status(self, connector: str) -> dict[str, Any]:
        return self._request("GET", f"/connectors/{self._name(connector)}/status")

    def offsets(self, connector: str) -> dict[str, Any]:
        return self._request("GET", f"/connectors/{self._name(connector)}/offsets")

    def validate(self, plugin: str, config: dict[str, str]) -> dict[str, Any]:
        path = f"/connector-plugins/{urllib.parse.quote(plugin, safe='')}/config/validate"
        return self._request("PUT", path, config)


def connector_state(api: ConnectApi, connector: str) -> str:
    status = api.status(connector)
    return str(status.get("connector", {}).get("state", "UNKNOWN")).upper()


def check_exactly_once(api: ConnectApi | None) -> EosCheck:
    """MIG-4: a distributed worker without exactly-once source support rejects
    ``exactly.once.support=required`` in config validation, so validation tells us."""
    if api is None:
        return EosCheck("not checked", "no --connect-url given")
    probe = {
        "connector.class": OSO_CONNECTOR_CLASS,
        "name": "oso-cdc-exactly-once-probe",
        "tasks.max": "1",
        "exactly.once.support": "required",
        "transaction.boundary": "connector",
    }
    try:
        response = api.validate(OSO_CONNECTOR_CLASS, probe)
    except ConnectError as e:
        return EosCheck("unknown", f"validation request failed: {e}")
    for entry in response.get("configs", []):
        value = entry.get("value", {})
        if value.get("name") == "exactly.once.support":
            errors = value.get("errors") or []
            if errors:
                return EosCheck("disabled", "; ".join(str(x) for x in errors))
            return EosCheck("enabled", "the worker accepted exactly.once.support=required")
    return EosCheck(
        "unknown",
        "the validation response has no exactly.once.support entry (a standalone worker, or"
        " Kafka Connect before 3.3)",
    )
