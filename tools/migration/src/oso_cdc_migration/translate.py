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
"""The shared translator framework (PRD-04 MIG-1 to MIG-7).

Every source property is classified as ``mapped``, ``mapped-with-change``, ``dropped`` (with a
reason) or ``manual`` (with an instruction). Unknown properties are ``manual``. Rules see the whole
source configuration through a :class:`Context`, set target properties through it, and the report
lists every source property with what became of it, so nothing is silently dropped.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from . import OSO_CONNECTOR_CLASS, __version__
from .common import EXIT_FOLLOW_UPS, EXIT_OK
from .configio import ConnectorConfig, format_properties
from .redact import MASK, Redactor, is_provider_reference, is_secret_key, mask_url, url_password


class Classification(StrEnum):
    MAPPED = "mapped"
    MAPPED_WITH_CHANGE = "mapped-with-change"
    DROPPED = "dropped"
    MANUAL = "manual"


@dataclass
class PropertyResult:
    key: str
    value: str
    classification: Classification
    targets: dict[str, str]
    note: str


@dataclass
class FollowUp:
    text: str
    properties: list[str] = field(default_factory=list)


@dataclass
class Setting:
    key: str
    value: str
    reason: str


@dataclass
class EosCheck:
    """Whether the target Connect cluster runs exactly-once source support (MIG-4)."""

    status: str  # enabled, disabled, unknown or not checked
    detail: str


@dataclass
class TranslationResult:
    tool: str
    source_kind: str
    source_name: str | None
    target_name: str
    properties: list[PropertyResult]
    settings: list[Setting]
    follow_ups: list[FollowUp]
    config: dict[str, str]
    exactly_once: EosCheck
    next_steps: list[str]
    tool_version: str = __version__

    @property
    def exit_code(self) -> int:
        return EXIT_FOLLOW_UPS if self.follow_ups else EXIT_OK

    def counts(self) -> dict[str, int]:
        out = {c.value: 0 for c in Classification}
        for p in self.properties:
            out[p.classification.value] += 1
        return out


class Manual(Exception):
    """Raised by a rule that finds the value cannot be translated."""

    def __init__(self, instruction: str) -> None:
        super().__init__(instruction)
        self.instruction = instruction


Outcome = tuple[Classification, str]
Rule = Callable[["Context", str, str], Outcome]


class Context:
    """The source configuration, the target being built and what the report must say."""

    def __init__(
        self,
        source: dict[str, str],
        redactor: Redactor,
        *,
        source_name: str | None,
        target_name: str,
    ) -> None:
        self.source = source
        self.redactor = redactor
        self.source_name = source_name
        self.target_name = target_name
        self.out: dict[str, str] = {}
        self.follow_ups: list[FollowUp] = []
        self.settings: list[Setting] = []
        self.results: dict[str, PropertyResult] = {}
        self.state: dict[str, Any] = {}
        self._origin: dict[str, str] = {}
        self._current: dict[str, str] | None = None
        self._flags: set[str] = set()

    def get(self, key: str, default: str | None = None) -> str | None:
        value = self.source.get(key)
        if value is None or value.strip() == "":
            return default
        return value.strip()

    def has(self, key: str) -> bool:
        return self.get(key) is not None

    def once(self, flag: str) -> bool:
        if flag in self._flags:
            return False
        self._flags.add(flag)
        return True

    def display(self, key: str, value: str) -> str:
        return self.redactor.display(key, value)

    def set(self, key: str, value: str, origin: str) -> None:
        value = str(value)
        if key in self.out:
            if self.out[key] == value:
                if self._current is not None:
                    self._current[key] = self.out[key]
            else:
                first = self._origin[key]
                self.follow_up(
                    f"`{key}` would be set differently by `{first}` and `{origin}`; the value"
                    f" from `{first}` was kept. Decide which one applies.",
                    origin,
                )
            return
        if is_secret_key(key) and value and not is_provider_reference(value):
            self.redactor.add(value)
            value = MASK
            self.follow_up(
                f"Set `{key}` to a config provider reference, for example"
                f" `${{file:/etc/kafka-connect/secrets.properties:{key}}}`. The source held a"
                " literal secret, which is never copied.",
                origin,
            )
        password = url_password(value)
        if password:
            self.redactor.add(password)
            value = mask_url(value)
            self.follow_up(
                f"Remove the password from `{key}` and set `cdc.database.password` instead,"
                " preferably through a config provider. The URL was copied with the password"
                " masked.",
                origin,
            )
        self.out[key] = value
        self._origin[key] = origin
        if self._current is not None:
            self._current[key] = value

    def setting(self, key: str, value: str, reason: str) -> bool:
        """A target property the translator adds; a value from a source property wins."""
        if key in self.out:
            return False
        self.set(key, value, "(added by the translator)")
        self.settings.append(Setting(key, self.display(key, self.out[key]), reason))
        return True

    def attribute(self, source_key: str, target_key: str) -> None:
        """Shows a target a finaliser built from several source properties on each of them."""
        result = self.results.get(source_key)
        if result is not None and target_key in self.out:
            result.targets[target_key] = self.display(target_key, self.out[target_key])

    def follow_up(self, text: str, prop: str | None = None, *, first: bool = False) -> None:
        for f in self.follow_ups:
            if f.text == text:
                if prop and prop not in f.properties:
                    f.properties.append(prop)
                return
        entry = FollowUp(text, [prop] if prop else [])
        if first:
            self.follow_ups.insert(0, entry)
        else:
            self.follow_ups.append(entry)


# Rule builders -----------------------------------------------------------------------------------


def to(target: str, note: str = "") -> Rule:
    def rule(ctx: Context, key: str, value: str) -> Outcome:
        ctx.set(target, value, key)
        return Classification.MAPPED, note

    return rule


def changed(target: str, convert: Callable[[str], str], note: str) -> Rule:
    def rule(ctx: Context, key: str, value: str) -> Outcome:
        ctx.set(target, convert(value.strip()), key)
        return Classification.MAPPED_WITH_CHANGE, note

    return rule


def dropped(reason: str) -> Rule:
    def rule(ctx: Context, key: str, value: str) -> Outcome:
        return Classification.DROPPED, reason

    return rule


def manual(instruction: str) -> Rule:
    def rule(ctx: Context, key: str, value: str) -> Outcome:
        return Classification.MANUAL, instruction

    return rule


def unknown(ctx: Context, key: str, value: str) -> Outcome:
    return (
        Classification.MANUAL,
        "The translator does not know this property. Check what it did in the source connector"
        " and set the corresponding `cdc.*` property by hand, or leave it out.",
    )


FRAMEWORK_PREFIXES = (
    "key.converter",
    "value.converter",
    "header.converter",
    "transforms",
    "predicates",
    "errors.",
    "topic.creation.",
    "producer.override.",
    "consumer.override.",
    "admin.override.",
    "config.action.reload",
    "offsets.storage.topic",
)


def framework(ctx: Context, key: str, value: str) -> Outcome:
    """Kafka Connect properties belong to the worker's framework and are copied unchanged."""
    ctx.set(key, value, key)
    if "io.debezium" in value:
        ctx.follow_up(
            "Transforms, predicates or converters name `io.debezium` classes. Keep the Debezium"
            " plugin (or its transform and converter jars) on the worker's plugin path, or"
            " replace them; the record envelope they expect is unchanged.",
            key,
        )
    if key == "errors.tolerance" and value.strip().lower() == "all":
        ctx.follow_up(
            "`errors.tolerance=all` lets Connect skip records it cannot convert. For change data"
            " capture `none` is recommended, so a conversion problem stops the task instead of"
            " losing a change.",
            key,
        )
    return Classification.MAPPED, "Kafka Connect framework property, copied unchanged."


def exactly_once_rule(ctx: Context, key: str, value: str) -> Outcome:
    return (
        Classification.MAPPED_WITH_CHANGE,
        "Set from the exactly-once check of the target Connect cluster, not copied (MIG-4).",
    )


@dataclass
class RuleSet:
    exact: dict[str, Rule]
    prefixes: dict[str, Rule]
    # rules that depend on other properties, such as a custom converter's own settings
    dynamic: Callable[[dict[str, str], str], Rule | None] | None = None
    # properties whose empty value means something; any other empty property is dropped
    keep_empty: frozenset[str] = frozenset()

    def find(self, key: str, source: dict[str, str] | None = None) -> Rule:
        if key in self.exact:
            return self.exact[key]
        best = None
        for prefix in self.prefixes:
            if key.startswith(prefix) and (best is None or len(prefix) > len(best)):
                best = prefix
        if best is not None:
            return self.prefixes[best]
        if self.dynamic is not None:
            rule = self.dynamic(source or {}, key)
            if rule is not None:
                return rule
        if key in ("exactly.once.support", "transaction.boundary") or key.startswith(
            "transaction.boundary."
        ):
            return exactly_once_rule
        if any(key.startswith(p) for p in FRAMEWORK_PREFIXES):
            return framework
        return unknown


Finaliser = Callable[[Context], None]


def translate(
    source: ConnectorConfig,
    *,
    tool: str,
    source_kind: str,
    rules: RuleSet,
    finalisers: list[Finaliser],
    target_name: str,
    redactor: Redactor,
    exactly_once: EosCheck,
    next_steps: list[str],
    state: dict[str, Any] | None = None,
) -> TranslationResult:
    redactor.register_config(source.config)
    ctx = Context(source.config, redactor, source_name=source.name, target_name=target_name)
    ctx.state.update(state or {})
    ctx.state["exactly_once"] = exactly_once
    for key in sorted(source.config):
        value = source.config[key]
        ctx._current = {}
        try:
            if not value.strip() and key not in rules.keep_empty:
                classification, note = (
                    Classification.DROPPED,
                    "Empty in the source, so there is nothing to carry over.",
                )
            else:
                classification, note = rules.find(key, source.config)(ctx, key, value)
        except Manual as m:
            classification, note = Classification.MANUAL, m.instruction
        targets = {k: ctx.display(k, v) for k, v in ctx._current.items()}
        ctx._current = None
        if classification is Classification.MANUAL:
            ctx.follow_up(note, key)
        ctx.results[key] = PropertyResult(
            key, ctx.display(key, value), classification, targets, note
        )
    for finalise in finalisers:
        finalise(ctx)
    return TranslationResult(
        tool=tool,
        source_kind=source_kind,
        source_name=source.name,
        target_name=target_name,
        properties=[ctx.results[k] for k in sorted(ctx.results)],
        settings=ctx.settings,
        follow_ups=ctx.follow_ups,
        config=ordered(ctx.out),
        exactly_once=exactly_once,
        next_steps=next_steps,
    )


# Finalisers shared by both translators -----------------------------------------------------------


def finalise_identity(ctx: Context) -> None:
    ctx.setting("name", ctx.target_name, "The new connector's name.")
    ctx.setting(
        "connector.class", OSO_CONNECTOR_CLASS, "The OSO CDC Connector for Oracle Database."
    )
    ctx.setting("tasks.max", "1", "The connector always runs one task.")


def finalise_start(ctx: Context) -> None:
    """The takeover: start at the old connector's SCN without a snapshot (PRD-04 s4)."""
    ctx.setting(
        "cdc.snapshot.mode",
        "none",
        "A takeover streams from the old connector's position, without a snapshot.",
    )
    start = ctx.state.get("start_scn")
    if start is not None:
        ctx.setting(
            "cdc.start.scn",
            str(start),
            "The old connector's position (--start-scn); honoured while the new connector has"
            " no stored offset.",
        )
        return
    ctx.follow_up(
        "Set `cdc.start.scn` before the new connector first runs: stop the old connector, then"
        " run `takeover_scn.py`, which reads its offset, checks the archived logs and writes"
        " `cdc.start.scn` into this configuration. Without it the new connector starts at the"
        " current SCN and changes made since the old connector stopped are lost."
    )


def finalise_exactly_once(ctx: Context) -> None:
    check: EosCheck = ctx.state["exactly_once"]
    if check.status == "enabled":
        reason = "The target Connect cluster has exactly-once source support enabled (MIG-4)."
        ctx.setting("exactly.once.support", "required", reason)
        ctx.setting("transaction.boundary", "connector", reason)
        return
    if check.status == "disabled":
        text = (
            "The target Connect cluster does not have exactly-once source support enabled"
            f" ({check.detail}), so the connector delivers at least once. To deliver exactly"
            " once, set `exactly.once.source.support=enabled` on every worker, then add"
            " `exactly.once.support=required` and `transaction.boundary=connector`."
        )
    else:
        how = "was not checked" if check.status == "not checked" else "could not be checked"
        text = (
            f"Exactly-once delivery was not configured because the target Connect cluster {how}"
            f" ({check.detail}). Run the translator again with `--connect-url`, or"
            " add `exactly.once.support=required` and `transaction.boundary=connector` yourself"
            " once the workers run with `exactly.once.source.support=enabled`."
        )
    ctx.follow_up(text)


def finalise_required(ctx: Context) -> None:
    for key in ("cdc.database.user", "cdc.database.password", "cdc.topic.prefix"):
        if key not in ctx.out:
            ctx.follow_up(f"Set `{key}`; the connector requires it and the source had no value.")
    if "cdc.tables.include" not in ctx.out:
        ctx.follow_up(
            "Set `cdc.tables.include` to the tables to capture, as regular expressions over"
            " `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB); the connector requires it."
        )
    if "cdc.database.url" not in ctx.out:
        if "cdc.database.host" not in ctx.out:
            ctx.follow_up("Set `cdc.database.host` or `cdc.database.url`.")
        elif "cdc.database.service" not in ctx.out and "cdc.database.sid" not in ctx.out:
            ctx.follow_up("Set `cdc.database.service` (or `cdc.database.sid`).")


# Output -----------------------------------------------------------------------------------------

_FIRST = ("name", "connector.class", "tasks.max")


def ordered(config: dict[str, str]) -> dict[str, str]:
    out = {k: config[k] for k in _FIRST if k in config}
    for k in sorted(k for k in config if k.startswith("cdc.")):
        out[k] = config[k]
    for k in sorted(k for k in config if k not in out):
        out[k] = config[k]
    return out


def render_config(result: TranslationResult, fmt: str) -> str:
    if fmt == "properties":
        return format_properties(result.config)
    return json.dumps({"name": result.target_name, "config": result.config}, indent=2) + "\n"


def to_dict(result: TranslationResult) -> dict[str, Any]:
    return {
        "tool": result.tool,
        "tool_version": result.tool_version,
        "source_kind": result.source_kind,
        "source_connector": result.source_name,
        "new_connector": result.target_name,
        "exit_code": result.exit_code,
        "summary": result.counts(),
        "exactly_once": asdict(result.exactly_once),
        "follow_ups": [asdict(f) for f in result.follow_ups],
        "settings": [asdict(s) for s in result.settings],
        "properties": [
            {
                "key": p.key,
                "value": p.value,
                "classification": p.classification.value,
                "targets": p.targets,
                "note": p.note,
            }
            for p in result.properties
        ],
        "config": result.config,
        "next_steps": result.next_steps,
    }


def cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def code(text: str) -> str:
    if text == "":
        return "(empty)"
    return "`" + cell(text).replace("`", "'") + "`"


_EXIT_TEXT = {0: "0, success", 2: "2, success with manual follow-ups"}


def render_markdown(result: TranslationResult) -> str:
    c = result.counts()
    eos = result.exactly_once
    lines = [
        f"# Migration report: {result.source_kind} to the OSO CDC Connector",
        "",
        "| Item | Value |",
        "|---|---|",
        f"| Tool | {code(result.tool)} {result.tool_version} |",
        f"| Source connector | {code(result.source_name or '(unnamed)')} |",
        f"| New connector | {code(result.target_name)} |",
        f"| Exactly-once check | {eos.status}: {cell(eos.detail)} |",
        f"| Exit code | {_EXIT_TEXT[result.exit_code]} |",
        "",
        f"{len(result.properties)} source properties: {c['mapped']} mapped,"
        f" {c['mapped-with-change']} mapped with a change, {c['dropped']} dropped and"
        f" {c['manual']} needing manual action. {len(result.follow_ups)} follow-ups.",
        "",
        "## Follow-ups",
        "",
    ]
    if result.follow_ups:
        for i, f in enumerate(result.follow_ups, 1):
            props = " (" + ", ".join(code(p) for p in f.properties) + ")" if f.properties else ""
            lines.append(f"{i}. {f.text}{props}")
    else:
        lines.append("None.")
    lines += [
        "",
        "## Settings added by the translator",
        "",
        "| Property | Value | Reason |",
        "|---|---|---|",
    ]
    for s in result.settings:
        lines.append(f"| {code(s.key)} | {code(s.value)} | {cell(s.reason)} |")
    lines += [
        "",
        "## Source properties",
        "",
        "| Source property | Value | Classification | Target | Note |",
        "|---|---|---|---|---|",
    ]
    for p in result.properties:
        targets = "<br/>".join(f"{code(k)} = {code(v)}" for k, v in p.targets.items()) or " "
        lines.append(
            f"| {code(p.key)} | {code(p.value)} | {p.classification.value} | {targets}"
            f" | {cell(p.note)} |"
        )
    lines += ["", "## Next steps", ""]
    lines += [f"{i}. {s}" for i, s in enumerate(result.next_steps, 1)]
    return "\n".join(lines) + "\n"


def render_summary(result: TranslationResult, wrote: list[str]) -> str:
    c = result.counts()
    lines = [
        f"{result.source_kind}: {len(result.properties)} properties, {c['mapped']} mapped,"
        f" {c['mapped-with-change']} mapped with a change, {c['dropped']} dropped,"
        f" {c['manual']} manual.",
        f"Exactly-once check: {result.exactly_once.status} ({result.exactly_once.detail}).",
    ]
    if result.follow_ups:
        lines.append(f"{len(result.follow_ups)} follow-ups:")
        for i, f in enumerate(result.follow_ups, 1):
            props = " [" + ", ".join(f.properties) + "]" if f.properties else ""
            lines.append(f"  {i}. {f.text}{props}")
    else:
        lines.append("No follow-ups.")
    lines += [f"Wrote {w}" for w in wrote]
    return "\n".join(lines) + "\n"
