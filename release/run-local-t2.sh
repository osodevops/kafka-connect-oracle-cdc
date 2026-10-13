#!/usr/bin/env bash
# Run from a clean, isolated checkout. No hosted runner or proprietary reference connector.
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo 'Usage: release/run-local-t2.sh <private results directory outside the checkout>' >&2
  exit 2
fi
repo_root=$(git rev-parse --show-toplevel)
mkdir -p "$1"
results=$(cd "$1" && pwd -P)
case "$results/" in
  "$repo_root/"*) echo 'Keep raw qualification logs outside the public checkout.' >&2; exit 2 ;;
esac
test -z "$(git status --porcelain)"
if [ "$(uname -s)" = Darwin ]; then
  export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
fi
: "${JAVA_HOME:?Set JAVA_HOME to JDK 17 or 21 before running Maven}"
if ! mkdir "$results/local-t2.lock" 2>/dev/null; then
  echo 'A run is active in this results directory; refusing a second run.' >&2
  exit 1
fi
trap 'rc=$?; echo "$rc" > "$results/local-t2-exit-code"; rmdir "$results/local-t2.lock"; exit "$rc"' EXIT
printf '%s\n' "$$" > "$results/local-t2.pid"
cd "$repo_root"
tested_commit=$(git rev-parse HEAD)
printf '%s\n' "$tested_commit" > "$results/local-t2-tested-commit"
export JAVA_HOME
./mvnw install -DskipTests -DskipE2E -Djacoco.skip=true --batch-mode --no-transfer-progress \
  > "$results/local-t2-install.log" 2>&1

for image_tag in 23.26.3-slim-faststart 23.9-faststart; do
  if [ -e "$results/STOP" ]; then exit 3; fi
  output="$results/local-t2-$image_tag"
  # Refuse old reports: a partial rerun must never inherit a previous PASS.
  if [ -e "$output" ]; then echo "Results already exist: $output" >&2; exit 1; fi
  mkdir "$output"
  printf '%s\n' "$image_tag" > "$results/local-t2-current-image"
  export JAVA_HOME
  set +e
  ./mvnw -pl e2e-tests clean verify -De2e.groups=nightly -Doracle.image.tag="$image_tag" \
    -Dnightly.kills=1000 -Dnightly.large.rows=5000000 --batch-mode --no-transfer-progress \
    > "$output/maven.log" 2>&1
  result=$?
  set -e
  printf '%s\n' "$result" > "$output/exit-code"
  if [ -d e2e-tests/target/failsafe-reports ]; then
    cp -R e2e-tests/target/failsafe-reports "$output/"
  fi
  for evidence in e2e-tests/target/*-evidence.json; do
    if [ -f "$evidence" ]; then cp "$evidence" "$output/"; fi
  done
  if [ "$result" -ne 0 ]; then exit "$result"; fi
  test "$(git rev-parse HEAD)" = "$tested_commit"
  test -z "$(git status --porcelain)"
  # Ryuk must remove the previous JVM's containers before the next fixed-subnet suite.
  sleep 10
done
python3 release/check_t2_evidence.py collect --results "$results" \
  --tested-commit "$tested_commit" --output "$results/latest.json"
printf '%s\n' PASS > "$results/local-t2-complete"
