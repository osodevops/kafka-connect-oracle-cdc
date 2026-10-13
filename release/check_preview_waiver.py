#!/usr/bin/env python3
"""Apply the maintainer's recorded 0.1.6 exception only to the already-tested software inputs."""

import json
import os
import subprocess
import sys
from pathlib import Path

from check_t2_evidence import inputs_digest, require


def main():
    version, tag = sys.argv[1:]
    approved = False
    if version == "0.1.6" and tag == "v0.1.6":
        record = json.loads(Path("release/overrides/0.1.6.json").read_text())
        require(
            record["version"] == version and record["tag"] == tag,
            "wrong waiver version",
        )
        require(
            record["approvedBy"] == "Sion Smith", "missing maintainer authorisation"
        )
        require(
            record["t2Status"] == "waived, not completed",
            "do not claim waived tests passed",
        )
        digest = record["sourceInputsSha256"]
        require(
            inputs_digest(Path("."), record["sourceCommit"]) == digest,
            "wrong baseline inputs",
        )
        require(
            inputs_digest(Path("."), "HEAD") == digest,
            "software changed after the approved baseline",
        )
        run = json.loads(
            subprocess.check_output(
                [
                    "gh",
                    "run",
                    "view",
                    str(record["ciRun"]),
                    "--repo",
                    os.environ["GITHUB_REPOSITORY"],
                    "--json",
                    "status,conclusion,headSha,jobs",
                ],
                text=True,
            )
        )
        require(
            run["status"] == "completed"
            and run["conclusion"] == "success"
            and run["headSha"] == record["sourceCommit"],
            "baseline CI did not pass",
        )
        required = {
            "build (Java 17)",
            "build (Java 21)",
            "T1 engine (Oracle Database Free)",
            "T1 connector (Oracle Database Free, Kafka, Connect worker)",
            "Maven Central dry run (no secrets)",
            "secret scan",
        }
        passed = {j["name"] for j in run["jobs"] if j["conclusion"] == "success"}
        require(required <= passed, "baseline CI is missing required successful checks")
        approved = True
        summary = (
            "0.1.6 preview: Sion Smith authorised publication without waiting for outstanding T2 "
            "qualification. T2 is waived, not completed. Existing T0, T1 and Central bundle checks "
            f"are reused from green CI run {record['ciRun']} for identical software inputs. "
            "Version checks, signatures, provenance, checksums and publication approval remain.\n"
        )
        print(summary)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
                output.write(summary)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write("approved=" + str(approved).lower() + "\n")


if __name__ == "__main__":
    main()
