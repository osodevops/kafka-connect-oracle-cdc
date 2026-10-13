"""Release safety: incomplete, stale, changed or lossy local qualification cannot publish."""

import copy
import datetime as dt
import importlib.util
import json
import pathlib
import subprocess
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "t2", pathlib.Path(__file__).parents[1] / "check_t2_evidence.py"
)
t2 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(t2)


class LocalT2EvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = pathlib.Path(self.temp.name) / "repo"
        self.repo.mkdir()
        self.run_git("init", "-q")
        self.run_git("config", "user.name", "Qualification test")
        self.run_git("config", "user.email", "qualification-test@example.invalid")
        self.write(
            "pom.xml",
            '<project xmlns="http://maven.apache.org/POM/4.0.0">'
            "<version>0.1.6-SNAPSHOT</version><dependencies><dependency>"
            "<version>3.9.2</version></dependency></dependencies></project>",
        )
        self.suites = {
            "RepeatedWorkerKillsNightlyIT": ("everyCommittedTransactionArrivesOnce",),
            "LargeTransactionNightlyIT": ("everyRowArrivesOnceWithinHeapBudget",),
            "NetworkFaultsNightlyIT": ("firstFault", "secondFault"),
            "SoakHarnessNightlyIT": ("checksEachSegment",),
        }
        for name, methods in self.suites.items():
            self.write(
                t2.NIGHTLY + "/" + name + ".java",
                "\n".join("@Test void " + method + "() {}" for method in methods),
            )
        self.commit()
        self.sha = self.run_git("rev-parse", "HEAD").strip()
        self.now = dt.datetime.now(dt.timezone.utc)
        self.receipt = self.make_receipt()

    def run_git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.repo), *args]).decode()

    def write(self, name, data):
        file = self.repo / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(data)

    def commit(self):
        self.run_git("add", ".")
        self.run_git("commit", "-qm", "test: synthetic qualification inputs")

    def document(self, suite):
        result = {
            "suite": suite,
            "case": None,
            "verdict": "PASS",
            "rawSha256": "a" * 64,
            "startedAt": (self.now - dt.timedelta(hours=1)).isoformat(),
            "finishedAt": (self.now - dt.timedelta(minutes=1)).isoformat(),
        }
        if suite == "RepeatedWorkerKillsNightlyIT":
            result.update(
                kills=1000,
                killsDone=1000,
                caughtUp=True,
                duplicates=0,
                oracleVerdict="PASS",
                transactionsSeen=123,
                transactionsCommitted=123,
            )
        elif suite == "LargeTransactionNightlyIT":
            result.update(
                rows=5_000_000,
                distinctRows=5_000_001,
                duplicates=0,
                heapBudgetBytes=16 * 1024 * 1024,
                taskState="RUNNING",
            )
        return result

    def make_receipt(self):
        tests = [
            {
                "class": "sh.oso.connect.oracle.e2e.nightly." + suite,
                "name": method,
                "verdict": "PASS",
            }
            for suite, methods in self.suites.items()
            for method in methods
        ]
        return {
            "format": t2.FORMAT,
            "testedCommit": self.sha,
            "testedInputsSha256": t2.inputs_digest(self.repo, self.sha),
            "images": [
                {
                    "image": image,
                    "mavenExitCode": 0,
                    "tests": copy.deepcopy(tests),
                    "evidence": [
                        self.document(s)
                        for s in self.suites
                        if s != "SoakHarnessNightlyIT"
                    ],
                }
                for image in t2.IMAGES
            ],
        }

    def check(self, receipt=None):
        return t2.verify(self.repo, receipt or self.receipt, now=self.now)

    def test_full_matrix_and_original_invariants_pass(self):
        self.check()

    def test_reduced_or_lossy_runs_fail(self):
        for suite, field, value in (
            ("RepeatedWorkerKillsNightlyIT", "kills", 25),
            ("RepeatedWorkerKillsNightlyIT", "killsDone", 999),
            ("RepeatedWorkerKillsNightlyIT", "caughtUp", False),
            ("RepeatedWorkerKillsNightlyIT", "duplicates", 1),
            ("RepeatedWorkerKillsNightlyIT", "oracleVerdict", "INCONCLUSIVE"),
            ("RepeatedWorkerKillsNightlyIT", "transactionsSeen", 122),
            ("LargeTransactionNightlyIT", "rows", 1_000_000),
            ("LargeTransactionNightlyIT", "distinctRows", 5_000_000),
            ("LargeTransactionNightlyIT", "duplicates", 1),
            ("LargeTransactionNightlyIT", "heapBudgetBytes", 32 * 1024 * 1024),
            ("LargeTransactionNightlyIT", "taskState", "FAILED"),
        ):
            with self.subTest(suite=suite, field=field):
                bad = copy.deepcopy(self.receipt)
                next(e for e in bad["images"][0]["evidence"] if e["suite"] == suite)[
                    field
                ] = value
                with self.assertRaises(ValueError):
                    self.check(bad)

    def test_incomplete_matrix_and_duplicate_image_fail(self):
        bad = copy.deepcopy(self.receipt)
        bad["images"].pop()
        with self.assertRaises(ValueError):
            self.check(bad)
        bad = copy.deepcopy(self.receipt)
        bad["images"][1]["image"] = bad["images"][0]["image"]
        with self.assertRaises(ValueError):
            self.check(bad)

    def test_missing_failed_skipped_and_duplicate_test_results_fail(self):
        for kind in ("missing", "FAIL", "SKIP", "duplicate", "maven"):
            with self.subTest(kind=kind):
                bad = copy.deepcopy(self.receipt)
                image = bad["images"][0]
                if kind == "missing":
                    image["tests"].pop()
                elif kind == "duplicate":
                    image["tests"][-1] = image["tests"][0]
                elif kind == "maven":
                    image["mavenExitCode"] = 1
                else:
                    image["tests"][0]["verdict"] = kind
                with self.assertRaises(ValueError):
                    self.check(bad)

    def test_missing_failed_and_duplicate_evidence_fail(self):
        for kind in ("missing", "failed", "duplicate"):
            with self.subTest(kind=kind):
                bad = copy.deepcopy(self.receipt)
                evidence = bad["images"][0]["evidence"]
                if kind == "missing":
                    evidence.pop()
                elif kind == "failed":
                    evidence[0]["verdict"] = "FAIL"
                else:
                    evidence.append(evidence[0])
                with self.assertRaises(ValueError):
                    self.check(bad)

    def test_stale_future_and_reversed_evidence_timestamps_fail(self):
        for field, offset in (
            ("startedAt", dt.timedelta(days=-8)),
            ("finishedAt", dt.timedelta(hours=1)),
            ("finishedAt", dt.timedelta(hours=-2)),
        ):
            with self.subTest(field=field, offset=offset):
                bad = copy.deepcopy(self.receipt)
                bad["images"][0]["evidence"][0][field] = (self.now + offset).isoformat()
                with self.assertRaises(ValueError):
                    self.check(bad)

    def test_wrong_digest_and_non_ancestor_commit_fail(self):
        bad = copy.deepcopy(self.receipt)
        bad["testedInputsSha256"] = "0" * 64
        with self.assertRaises(ValueError):
            self.check(bad)
        bad["testedCommit"] = "f" * 40
        with self.assertRaises(ValueError):
            self.check(bad)

    def test_doc_and_release_version_changes_do_not_require_repeating_tests(self):
        self.write("docs/guide.md", "Documentation update")
        pom = self.repo / "pom.xml"
        pom.write_text(pom.read_text().replace("0.1.6-SNAPSHOT", "0.1.6"))
        self.commit()
        self.check()

    def test_dependency_changes_require_fresh_qualification(self):
        pom = self.repo / "pom.xml"
        pom.write_text(pom.read_text().replace("3.9.2", "4.0.0"))
        self.commit()
        with self.assertRaisesRegex(ValueError, "inputs changed"):
            self.check()

    def test_source_and_test_changes_require_fresh_qualification(self):
        self.write(
            t2.NIGHTLY + "/NetworkFaultsNightlyIT.java",
            "@Test void anotherInvariant() {}",
        )
        self.commit()
        with self.assertRaisesRegex(ValueError, "inputs changed"):
            self.check()

    def raw_results(self):
        results = pathlib.Path(self.temp.name) / "results"
        for image in self.receipt["images"]:
            directory = results / ("local-t2-" + image["image"])
            reports = directory / "failsafe-reports"
            reports.mkdir(parents=True)
            (directory / "exit-code").write_text("0")
            (reports / "failsafe-summary.xml").write_text(
                "<failsafe-summary><completed>5</completed><failures>0</failures>"
                "<errors>0</errors><skipped>0</skipped></failsafe-summary>"
            )
            cases = "".join(
                '<testcase classname="' + t["class"] + '" name="' + t["name"] + '"/>'
                for t in image["tests"]
            )
            (reports / "TEST-synthetic.xml").write_text(
                "<testsuite>" + cases + "</testsuite>"
            )
            for item in image["evidence"]:
                raw = {
                    "suite": item["suite"],
                    "verdict": "PASS",
                    "failures": [],
                    "startedAt": item["startedAt"],
                    "finishedAt": item["finishedAt"],
                    "versions": {"oracleImageTag": image["image"]},
                    "privateLog": "MUST-NOT-BE-EXPORTED",
                }
                if item["suite"] == "RepeatedWorkerKillsNightlyIT":
                    raw.update(
                        parameters={"kills": 1000},
                        counts={
                            "killsDone": 1000,
                            "caughtUp": True,
                            "transactionsWithDuplicateEvents": 0,
                        },
                        checks={
                            "oracle": {
                                "verdict": "PASS",
                                "transactions": {"seen": 123, "ledger": 123},
                            }
                        },
                    )
                elif item["suite"] == "LargeTransactionNightlyIT":
                    raw.update(
                        parameters={
                            "rows": 5_000_000,
                            "heapBudgetBytes": 16 * 1024 * 1024,
                        },
                        counts={
                            "distinctRows": 5_000_001,
                            "duplicates": 0,
                            "taskState": "RUNNING",
                        },
                    )
                (directory / (item["suite"] + "-evidence.json")).write_text(
                    json.dumps(raw)
                )
        return results

    def test_collector_exports_only_public_fields_and_preserves_raw_hashes(self):
        receipt = t2.collect(self.repo, self.raw_results(), self.sha)
        self.assertNotIn("MUST-NOT-BE-EXPORTED", json.dumps(receipt))
        self.check(receipt)

    def test_collector_rejects_failed_maven_and_skipped_reports(self):
        results = self.raw_results()
        directory = results / ("local-t2-" + t2.IMAGES[0])
        (directory / "exit-code").write_text("1")
        with self.assertRaisesRegex(ValueError, "Maven failed"):
            t2.collect(self.repo, results, self.sha)
        (directory / "exit-code").write_text("0")
        summary = directory / "failsafe-reports/failsafe-summary.xml"
        summary.write_text(
            summary.read_text().replace("<skipped>0</skipped>", "<skipped>1</skipped>")
        )
        with self.assertRaisesRegex(ValueError, "skipped"):
            t2.collect(self.repo, results, self.sha)


if __name__ == "__main__":
    unittest.main()
