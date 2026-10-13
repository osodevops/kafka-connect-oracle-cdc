#!/usr/bin/env python3
"""Collect and verify local T2 qualification without running long tests on hosted runners.

The receipt contains test names, results and correctness counts, never logs, credentials or row
values. Raw reports stay on the workstation. Evidence is accepted only for identical capture,
test, dependency and database-image inputs; release-only POM version changes are normalised.
"""

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import re
import subprocess
import xml.etree.ElementTree as ET

IMAGES = ("23.26.3-slim-faststart", "23.9-faststart")
MODULES = (
    "oracle-cdc-core",
    "kafka-connect-oracle-cdc",
    "oracle-cdc-doctor",
    "bench",
    "e2e-tests",
)
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
NIGHTLY = "e2e-tests/src/test/java/sh/oso/connect/oracle/e2e/nightly"
FORMAT = "oso-cdc-local-t2-v1"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args])


def build_input(path):
    return (
        path in ("pom.xml", "mvnw", "mvnw.cmd")
        or path.startswith((".mvn/", "docker/test-oracle/", "tools/migration/"))
        or any(path.startswith(module + "/") for module in MODULES)
    )


def normalise_pom(data):
    root = ET.fromstring(data)
    # Only our project and parent versions move for a release. Dependency/plugin versions count.
    for path in ("m:version", "m:parent/m:version"):
        node = root.find(path, NS)
        if node is not None:
            node.text = "OSO-PROJECT-VERSION"
    return ET.canonicalize(
        ET.tostring(root, encoding="unicode"), strip_text=True
    ).encode()


def inputs_digest(repo, revision):
    entries = git(repo, "ls-tree", "-r", "-z", revision).decode().split("\0")
    digest = hashlib.sha256()
    for entry in entries:
        if not entry:
            continue
        metadata, path = entry.split("\t", 1)
        if not build_input(path):
            continue
        data = git(repo, "show", revision + ":" + path)
        if path == "pom.xml" or path.endswith("/pom.xml"):
            data = normalise_pom(data)
        mode = metadata.split(" ", 1)[0]
        digest.update(
            path.encode()
            + b"\0"
            + mode.encode()
            + b"\0"
            + hashlib.sha256(data).digest()
        )
    return digest.hexdigest()


def expected_tests(repo, revision):
    paths = (
        git(repo, "ls-tree", "-r", "--name-only", revision, NIGHTLY)
        .decode()
        .splitlines()
    )
    tests = set()
    for path in paths:
        if not path.endswith("NightlyIT.java"):
            continue
        source = git(repo, "show", revision + ":" + path).decode()
        methods = re.findall(r"@Test\s+(?:public\s+)?void\s+(\w+)\s*\(", source)
        require(
            methods and "@Disabled" not in source,
            "unsupported or disabled suite: " + path,
        )
        classname = "sh.oso.connect.oracle.e2e.nightly." + pathlib.Path(path).stem
        tests.update((classname, method) for method in methods)
    require(tests, "no T2 tests found")
    return tests


def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "timestamp must include UTC offset")
    return parsed


def public_evidence(path, image):
    raw = path.read_bytes()
    document = json.loads(raw)
    require(
        document["verdict"] == "PASS" and not document["failures"],
        "failed evidence: " + path.name,
    )
    require(
        document["versions"]["oracleImageTag"] == image,
        "wrong database image: " + path.name,
    )
    # Deliberate allowlist: logs, fault details, SQL, hostnames and row values cannot be exported.
    result = {
        "suite": document["suite"],
        "case": document.get("case"),
        "startedAt": document["startedAt"],
        "finishedAt": document["finishedAt"],
        "verdict": "PASS",
        "rawSha256": hashlib.sha256(raw).hexdigest(),
    }
    if document["suite"] == "RepeatedWorkerKillsNightlyIT":
        result["kills"] = document["parameters"]["kills"]
        result["killsDone"] = document["counts"]["killsDone"]
        result["caughtUp"] = document["counts"]["caughtUp"]
        result["duplicates"] = document["counts"]["transactionsWithDuplicateEvents"]
        oracle = document["checks"]["oracle"]
        result["oracleVerdict"] = oracle["verdict"]
        result["transactionsSeen"] = oracle["transactions"]["seen"]
        result["transactionsCommitted"] = oracle["transactions"]["ledger"]
    elif document["suite"] == "LargeTransactionNightlyIT":
        result["rows"] = document["parameters"]["rows"]
        result["heapBudgetBytes"] = document["parameters"]["heapBudgetBytes"]
        result["distinctRows"] = document["counts"]["distinctRows"]
        result["duplicates"] = document["counts"]["duplicates"]
        result["taskState"] = document["counts"]["taskState"]
    return result


def collect(repo, results, tested_commit):
    require(
        re.fullmatch(r"[0-9a-f]{40}", tested_commit), "tested commit must be a full SHA"
    )
    expected = expected_tests(repo, tested_commit)
    images = []
    for image in IMAGES:
        directory = results / ("local-t2-" + image)
        require(
            (directory / "exit-code").read_text().strip() == "0",
            "Maven failed on " + image,
        )
        reports = directory / "failsafe-reports"
        summary = ET.parse(reports / "failsafe-summary.xml").getroot()
        require(
            summary.findtext("completed") == str(len(expected)),
            "incomplete T2 run on " + image,
        )
        for field in ("failures", "errors", "skipped"):
            require(summary.findtext(field) == "0", field + " in T2 run on " + image)
        passed = set()
        for report in reports.glob("TEST-*.xml"):
            tree = ET.parse(report).getroot()
            for test in tree.findall("testcase"):
                require(
                    not any(
                        test.find(k) is not None
                        for k in ("failure", "error", "skipped")
                    ),
                    "failed or skipped test in " + report.name,
                )
                identity = (test.attrib["classname"], test.attrib["name"])
                require(
                    identity not in passed, "duplicate test report: " + str(identity)
                )
                passed.add(identity)
        require(passed == expected, "missing or unexpected T2 tests on " + image)
        evidence = [
            public_evidence(p, image) for p in sorted(directory.glob("*-evidence.json"))
        ]
        images.append(
            {
                "image": image,
                "mavenExitCode": 0,
                "tests": [
                    {"class": c, "name": n, "verdict": "PASS"}
                    for c, n in sorted(passed)
                ],
                "evidence": evidence,
                "rawSummarySha256": hashlib.sha256(
                    (reports / "failsafe-summary.xml").read_bytes()
                ).hexdigest(),
            }
        )
    receipt = {
        "format": FORMAT,
        "testedCommit": tested_commit,
        "testedInputsSha256": inputs_digest(repo, tested_commit),
        "images": images,
    }
    verify(repo, receipt, tested_commit)
    return receipt


def verify(repo, receipt, revision="HEAD", max_age_days=7, now=None):
    require(receipt["format"] == FORMAT, "unknown local T2 evidence format")
    tested = receipt["testedCommit"]
    require(re.fullmatch(r"[0-9a-f]{40}", tested), "invalid tested commit")
    require(
        subprocess.run(
            ["git", "-C", str(repo), "merge-base", "--is-ancestor", tested, revision],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0,
        "tested commit is not an ancestor of the release",
    )
    require(
        inputs_digest(repo, tested) == receipt["testedInputsSha256"],
        "wrong tested source digest",
    )
    require(
        inputs_digest(repo, revision) == receipt["testedInputsSha256"],
        "capture, test or build inputs changed after local qualification",
    )
    expected = expected_tests(repo, revision)
    require(len(receipt["images"]) == len(IMAGES), "incomplete database image matrix")
    require(
        {i["image"] for i in receipt["images"]} == set(IMAGES),
        "wrong database image matrix",
    )
    now = now or dt.datetime.now(dt.timezone.utc)
    for image in receipt["images"]:
        require(image["mavenExitCode"] == 0, "Maven did not pass")
        tests = image["tests"]
        require(len(tests) == len(expected), "missing or duplicate T2 results")
        require(
            {(t["class"], t["name"]) for t in tests} == expected,
            "wrong T2 test coverage",
        )
        require(
            all(t["verdict"] == "PASS" for t in tests),
            "a T2 test failed or was skipped",
        )
        evidence = image["evidence"]
        suites = {t[0].rsplit(".", 1)[1] for t in expected} - {"SoakHarnessNightlyIT"}
        require(
            {e["suite"] for e in evidence} == suites,
            "missing or unexpected T2 evidence",
        )
        require(
            len({(e["suite"], e["case"]) for e in evidence}) == len(evidence),
            "duplicate evidence",
        )
        for document in evidence:
            require(document["verdict"] == "PASS", "a T2 evidence verdict failed")
            require(
                re.fullmatch(r"[0-9a-f]{64}", document["rawSha256"]),
                "invalid raw evidence digest",
            )
            start, end = (
                timestamp(document["startedAt"]),
                timestamp(document["finishedAt"]),
            )
            require(
                start <= end <= now + dt.timedelta(minutes=5),
                "invalid evidence timestamps",
            )
            require(
                now - start <= dt.timedelta(days=max_age_days), "T2 evidence is stale"
            )
        worker = [e for e in evidence if e["suite"] == "RepeatedWorkerKillsNightlyIT"]
        require(len(worker) == 1, "missing worker-kill result")
        worker = worker[0]
        require(
            type(worker["kills"]) is int and worker["kills"] >= 1000,
            "fewer than 1000 worker kills",
        )
        require(worker["killsDone"] == worker["kills"], "worker kills did not complete")
        require(
            worker["caughtUp"] is True and worker["oracleVerdict"] == "PASS",
            "worker did not catch up",
        )
        require(worker["duplicates"] == 0, "duplicate events after worker kills")
        require(
            worker["transactionsSeen"] == worker["transactionsCommitted"]
            and worker["transactionsCommitted"] > 0,
            "missing committed transactions",
        )
        large = [e for e in evidence if e["suite"] == "LargeTransactionNightlyIT"]
        require(len(large) == 1, "missing large-transaction result")
        large = large[0]
        require(
            type(large["rows"]) is int and large["rows"] >= 5_000_000,
            "fewer than five million rows",
        )
        require(
            large["distinctRows"] == large["rows"] + 1 and large["duplicates"] == 0,
            "large transaction lost or duplicated rows",
        )
        require(
            large["heapBudgetBytes"] == 16 * 1024 * 1024
            and large["taskState"] == "RUNNING",
            "large transaction did not survive within the heap budget",
        )
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=pathlib.Path, default=pathlib.Path("."))
    commands = parser.add_subparsers(dest="command", required=True)
    pack = commands.add_parser("collect")
    pack.add_argument("--results", type=pathlib.Path, required=True)
    pack.add_argument("--tested-commit", required=True)
    pack.add_argument("--output", type=pathlib.Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("receipt", type=pathlib.Path)
    args = parser.parse_args()
    try:
        if args.command == "collect":
            receipt = collect(args.repo, args.results, args.tested_commit)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(receipt, indent=2) + "\n")
            print("Local T2 evidence collected: " + str(args.output))
        else:
            verify(args.repo, json.loads(args.receipt.read_text()))
            print(
                "Local T2 evidence passed: full matrix, unchanged inputs, 1000 kills, five million rows"
            )
    except (
        ValueError,
        KeyError,
        TypeError,
        OSError,
        ET.ParseError,
        subprocess.CalledProcessError,
    ) as error:
        parser.exit(1, "Local T2 evidence FAILED: " + str(error) + "\n")


if __name__ == "__main__":
    main()
