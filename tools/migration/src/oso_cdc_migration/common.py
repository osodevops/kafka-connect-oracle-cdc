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
"""What every tool shares: exit codes, the common options, logging and file output (PRD-04 s2)."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import textwrap
from pathlib import Path
from typing import NoReturn

from .redact import RedactingFilter, Redactor

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_FOLLOW_UPS = 2

EXIT_CODES_HELP = """\
exit codes:
  0  success
  1  failure (including usage errors)
  2  success with manual follow-ups
"""

LOG = logging.getLogger("oso_cdc_migration")


class ToolError(Exception):
    """A failure the tool explains to the operator; exits with code 1."""


class ToolArgumentParser(argparse.ArgumentParser):
    """argparse exits with 2 on usage errors, which here would mean success with follow-ups."""

    def error(self, message: str) -> NoReturn:
        self.print_usage(sys.stderr)
        sys.stderr.write(f"{self.prog}: error: {message}\n")
        raise SystemExit(EXIT_FAILURE)


def parser(prog: str, description: str, epilog: str = "") -> ToolArgumentParser:
    return ToolArgumentParser(
        prog=prog,
        description=textwrap.fill(description, 78),
        epilog=(epilog + "\n\n" if epilog else "") + EXIT_CODES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )


def add_common_options(
    p: argparse.ArgumentParser,
    *,
    input_help: str,
    output_help: str,
    input_required: bool = False,
) -> None:
    p.add_argument("--input", metavar="FILE", required=input_required, help=input_help)
    p.add_argument("--output", metavar="FILE", help=output_help)
    p.add_argument("--report", metavar="FILE", help="write the Markdown report to FILE")
    p.add_argument(
        "--json",
        action="store_true",
        help="print the machine-readable result as JSON on standard output instead of a summary",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="log progress at debug level")


def configure_logging(redactor: Redactor, verbose: bool) -> None:
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, "_oso_cdc_migration", False):
            root.removeHandler(h)
    handler = logging.StreamHandler(sys.stderr)
    handler._oso_cdc_migration = True  # type: ignore[attr-defined]
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    root.addHandler(handler)
    # every handler, including any the embedding process attached, sees scrubbed records
    for h in root.handlers:
        for f in [f for f in h.filters if isinstance(f, RedactingFilter)]:
            h.removeFilter(f)
        h.addFilter(RedactingFilter(redactor))
    root.setLevel(logging.DEBUG if verbose else logging.INFO)


def write_text(path: str | Path, text: str, redactor: Redactor) -> None:
    Path(path).write_text(redactor.scrub(text), encoding="utf-8")


def emit(text: str, redactor: Redactor) -> None:
    sys.stdout.write(redactor.scrub(text))
    if not text.endswith("\n"):
        sys.stdout.write("\n")
    sys.stdout.flush()


def read_secret(
    redactor: Redactor, *, env: str | None = None, file: str | None = None, what: str
) -> str | None:
    """A secret from an environment variable or a file; never from the command line."""
    value: str | None = None
    if env:
        value = os.environ.get(env)
        if value is None:
            raise ToolError(f"The environment variable {env} named for the {what} is not set.")
    elif file:
        try:
            value = Path(file).read_text(encoding="utf-8").rstrip("\r\n")
        except OSError as e:
            raise ToolError(f"Cannot read the {what} file {file}: {e.strerror}.") from e
    redactor.add(value)
    return value


def run(main_body, redactor: Redactor) -> int:
    """Runs a tool body, turning a ToolError into exit code 1 with a scrubbed message."""
    try:
        return main_body()
    except ToolError as e:
        LOG.error("%s", e)
        return EXIT_FAILURE
    except KeyboardInterrupt:
        LOG.error("Interrupted.")
        return EXIT_FAILURE
    except Exception as e:
        LOG.error("Unexpected error: %s", e, exc_info=LOG.isEnabledFor(logging.DEBUG))
        return EXIT_FAILURE
