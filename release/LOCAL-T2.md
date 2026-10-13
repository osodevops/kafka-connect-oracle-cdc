# Local release qualification

Long T2 fault suites run on local Docker. The GitHub nightly schedule is removed and its optional
manual workflow is restricted to a dedicated self-hosted Linux lab runner. GitHub-hosted runners
do not run the T2 matrix. Ordinary CI and signing/publication workflows remain separate.

Use a clean isolated checkout. Do not edit it or run another Maven build in it until qualification
finishes, and run only one Docker suite at a time. The script installs the current modules, then
runs the complete nightly tier sequentially on both supported database images with 1,000 worker
kills and a five million row transaction. It stops on failure and preserves the raw reports in a
private directory outside the public checkout. It never starts Confluent's proprietary connector.

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
git worktree add --detach ../cdc-local-t2 HEAD
cd ../cdc-local-t2
bash release/run-local-t2.sh "$HOME/.cache/oso-cdc-lab/local-t2"
```

Use a new results directory for each attempt. Keep the workstation awake during the test without
overriding thermal protection. No cloud lab or persistent GitHub runner registration is needed.

After the run, review its result and collect a public receipt in the main checkout. The receipt
contains only suite/test names, verdicts, correctness counts, timestamps, image tags and hashes.
Raw logs, connection details, SQL and row values stay private. If the sources were tested before
this helper was added, use its `collect` command with the private results directory and the full
tested commit SHA; the directory layout is that written by `run-local-t2.sh`.

```bash
python3 release/check_t2_evidence.py collect --results "$HOME/.cache/oso-cdc-lab/local-t2" \
  --tested-commit "$(cat "$HOME/.cache/oso-cdc-lab/local-t2/local-t2-tested-commit")" \
  --output docs/qualification/local-t2/latest.json
python3 release/check_t2_evidence.py verify docs/qualification/local-t2/latest.json
gitleaks dir . --config .gitleaks.toml --redact
```

Commit the reviewed receipt with a signed commit. The release workflow accepts it only when every
nightly test passed on both images, no test was skipped, the evidence is no more than seven days
old, all 1,000 kills completed with catch-up and no missing or duplicate transactions, and the
five million rows plus the following commit were delivered once under the original heap budget.
The tested commit must be an ancestor of the release, and capture sources, tests, dependency and
image definitions must be identical. Only the project's release version in POMs is normalised;
dependency and plugin versions remain part of the source digest. Documentation updates do not
require repeating hours of qualification.

No receipt is created for a failed or partial run. A stale, incomplete or mismatched receipt fails
the release gate. Existing test assertions and the release environment approval remain in place.
Without a local receipt, the workflow still requires a recent successful lab nightly and evidence.
