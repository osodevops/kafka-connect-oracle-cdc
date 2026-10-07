#!/usr/bin/env bash
# Maven Central dry run. Runs the deploy release.yml runs (mvn clean deploy -Prelease) end to end,
# signs every artefact with a throwaway key, and points the publishing plugin at a loopback stub of
# the Central Portal (release/central_portal_stub.py) instead of central.sonatype.com. The stub keeps
# the uploaded bundle, which is then checked (release/check_central_bundle.py) and its signatures
# verified. Needs no secrets and makes no network call to Central.
#
# Why a stub: central-publishing-maven-plugin bundles and uploads in one step, and its
# skipPublishing switch removes a module from the bundle altogether (bench and e2e-tests use it to
# stay unpublished), so -DskipPublishing on the command line would produce an empty deployment.
#
# It works on a copy of the working tree under target/central-dry-run/src with the -SNAPSHOT suffix
# removed, because a SNAPSHOT takes the plugin's snapshot path rather than the release bundle.
# Install is skipped, so Maven writes only under that directory and the local repository it
# resolves from. To keep the local repository read-only as well, pass absolute paths (Maven runs
# inside the copy), for example:
#   -Dmaven.repo.local=$PWD/target/dry-run-m2 -Dmaven.repo.local.tail=$HOME/.m2/repository
#
# CENTRAL_DRY_RUN_SIGN=false skips signing (and the signature checks). Use it where gpg-agent cannot
# open its socket under target/, for example on macOS when the checkout path is long; CI signs.
# CENTRAL_DRY_RUN_GNUPGHOME overrides where the throwaway keyring lives.
#
# Usage: release/central-dry-run.sh [extra Maven arguments]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${ROOT}/target/central-dry-run"
SRC="${WORK}/src"
UPLOADED="${WORK}/uploaded-bundle.zip"
BUNDLE_DIR="${WORK}/bundle"
SIGN="${CENTRAL_DRY_RUN_SIGN:-true}"
export GNUPGHOME="${CENTRAL_DRY_RUN_GNUPGHOME:-${WORK}/gnupg}"
STUB_PID=""

cleanup() {
  if [ -n "${STUB_PID}" ]; then kill "${STUB_PID}" 2>/dev/null || true; fi
  if [ "${SIGN}" = "true" ]; then gpgconf --kill gpg-agent >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

rm -rf "${WORK}"
mkdir -p "${SRC}" "${BUNDLE_DIR}"

echo "Copying the working tree (tracked and unignored files) to ${SRC}"
(
  cd "${ROOT}"
  git ls-files -z --cached --others --exclude-standard \
    | while IFS= read -r -d '' f; do [ -f "$f" ] && printf '%s\0' "$f"; done \
    | tar --null -T - -cf -
) | tar -xf - -C "${SRC}"

SNAPSHOT_VERSION="$(python3 - "${SRC}/pom.xml" <<'PY'
import sys, xml.etree.ElementTree as ET
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
print(ET.parse(sys.argv[1]).getroot().find("m:version", ns).text.strip())
PY
)"
RELEASE_VERSION="${SNAPSHOT_VERSION%-SNAPSHOT}"
echo "Building ${RELEASE_VERSION} (working tree version ${SNAPSHOT_VERSION})"
python3 - "${SRC}" "${SNAPSHOT_VERSION}" "${RELEASE_VERSION}" <<'PY'
import pathlib, sys
src, old, new = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
if old == new:
    sys.exit(0)
for pom in sorted(src.glob("**/pom.xml")):
    text = pom.read_text(encoding="utf-8")
    needle = "<version>" + old + "</version>"
    if needle in text:
        pom.write_text(text.replace(needle, "<version>" + new + "</version>"), encoding="utf-8")
        print("  set " + new + " in " + str(pom.relative_to(src)))
PY

if [ "${SIGN}" = "true" ]; then
  mkdir -p "${GNUPGHOME}"
  chmod 700 "${GNUPGHOME}"
  echo "Generating a throwaway signing key in ${GNUPGHOME}"
  gpg --batch --quiet --pinentry-mode loopback --passphrase '' \
    --quick-gen-key 'OSO CDC release dry run (throwaway) <dry-run@example.invalid>' rsa3072 sign 1d
  KEY_ID="$(gpg --batch --list-secret-keys --with-colons | awk -F: '$1 == "fpr" { print $10; exit }')"
  SIGN_ARG="-Dgpg.keyname=${KEY_ID}"
else
  echo "Signing skipped (CENTRAL_DRY_RUN_SIGN=${SIGN})"
  SIGN_ARG="-Dgpg.skip=true"
fi

# The plugin needs a central server entry. These are placeholders, not credentials, and only ever
# reach the loopback stub.
cat > "${WORK}/settings.xml" <<'XML'
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">
  <servers>
    <server>
      <id>central</id>
      <username>dry-run-placeholder</username>
      <password>dry-run-placeholder</password>
    </server>
  </servers>
</settings>
XML

python3 "${ROOT}/release/central_portal_stub.py" "${WORK}/stub.port" "${UPLOADED}" &
STUB_PID=$!
for _ in $(seq 1 50); do [ -s "${WORK}/stub.port" ] && break; sleep 0.1; done
[ -s "${WORK}/stub.port" ] || { echo "the Central Portal stub did not start" >&2; exit 1; }
STUB_URL="http://127.0.0.1:$(cat "${WORK}/stub.port")"
echo "Central Portal stub listening at ${STUB_URL}"

(
  cd "${SRC}"
  ./mvnw -B --no-transfer-progress -s "${WORK}/settings.xml" clean deploy -Prelease -DskipTests \
    -DskipE2E -Dmaven.install.skip=true -DcentralBaseUrl="${STUB_URL}" \
    "${SIGN_ARG}" "$@"
)

[ -s "${UPLOADED}" ] || { echo "the build finished but uploaded no bundle" >&2; exit 1; }

if [ "${SIGN}" != "true" ]; then
  python3 "${ROOT}/release/check_central_bundle.py" --unsigned "${UPLOADED}" "${RELEASE_VERSION}"
  echo "Central dry run passed without signatures; uploaded bundle kept at ${UPLOADED}"
  exit 0
fi
python3 "${ROOT}/release/check_central_bundle.py" "${UPLOADED}" "${RELEASE_VERSION}"

echo "Verifying every signature in the bundle"
(cd "${BUNDLE_DIR}" && python3 -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(".")' "${UPLOADED}")
count=0
while IFS= read -r -d '' sig; do
  gpg --batch --quiet --verify "${sig}" "${sig%.asc}" 2>/dev/null \
    || { echo "bad signature: ${sig#"${BUNDLE_DIR}"/}" >&2; exit 1; }
  count=$((count + 1))
done < <(find "${BUNDLE_DIR}" -name '*.asc' -print0)
echo "Central dry run passed: ${count} signatures verified; uploaded bundle kept at ${UPLOADED}"
