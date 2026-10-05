#!/usr/bin/env python3
"""Checks a Maven Central bundle written by central-publishing-maven-plugin before it is uploaded.

Usage: check_central_bundle.py [--unsigned] <central-bundle.zip> <version>

Fails (exit 1) unless the bundle holds exactly the published modules at the given version, every
file has a signature and MD5 and SHA-1 checksums that match, and the POMs carry the metadata
Central requires. The modules that are never published (bench, e2e-tests) must be absent. When a
module is added to or removed from publishing, update PUBLISHED below. --unsigned drops the
signature requirement, for local dry runs that cannot run gpg-agent; releases are always signed.
"""

import hashlib
import sys
import xml.etree.ElementTree as ET
import zipfile

GROUP_PATH = "sh/oso"

# artifactId: the files (by suffix after "<artifactId>-<version>") each published module must ship.
PUBLISHED = {
    "kafka-connect-oracle-cdc-parent": [".pom"],
    "oracle-cdc-core": [".pom", ".jar", "-sources.jar", "-javadoc.jar", "-tests.jar"],
    "kafka-connect-oracle-cdc": [".pom", ".jar", "-sources.jar", "-javadoc.jar"],
    "oracle-cdc-doctor": [".pom", ".jar", "-sources.jar", "-javadoc.jar", "-cli.jar"],
}
NEVER_PUBLISHED = ["bench", "e2e-tests"]
SIDE_FILES = (".asc", ".md5", ".sha1", ".sha256", ".sha512")
POM_NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def main(argv):
    args = argv[1:]
    signed = "--unsigned" not in args
    args = [a for a in args if a != "--unsigned"]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    bundle, version = args
    errors = []

    if version.endswith("-SNAPSHOT"):
        errors.append("version %s is a SNAPSHOT; Central releases never are" % version)

    try:
        zf = zipfile.ZipFile(bundle)
    except (OSError, zipfile.BadZipFile) as e:
        print("cannot open bundle %s: %s" % (bundle, e), file=sys.stderr)
        return 1

    with zf:
        names = {n for n in zf.namelist() if not n.endswith("/")}

        for name in sorted(names):
            if not name.startswith(GROUP_PATH + "/"):
                errors.append("unexpected file outside %s: %s" % (GROUP_PATH, name))
        for artifact in NEVER_PUBLISHED:
            if any(n.startswith("%s/%s/" % (GROUP_PATH, artifact)) for n in names):
                errors.append("%s must never be published but is in the bundle" % artifact)

        artifacts = {n.split("/")[2] for n in names if n.count("/") >= 3}
        for extra in sorted(artifacts - set(PUBLISHED) - set(NEVER_PUBLISHED)):
            errors.append("unexpected module in the bundle: %s (update PUBLISHED?)" % extra)

        for artifact, suffixes in PUBLISHED.items():
            folder = "%s/%s/%s/" % (GROUP_PATH, artifact, version)
            if not any(n.startswith(folder) for n in names):
                errors.append("missing module %s at version %s" % (artifact, version))
                continue
            for suffix in suffixes:
                f = folder + artifact + "-" + version + suffix
                if f not in names:
                    errors.append("missing %s" % f)

        primaries = [n for n in names if not n.endswith(SIDE_FILES)]
        for f in sorted(primaries):
            data = zf.read(f)
            if signed and f + ".asc" not in names:
                errors.append("no signature for %s" % f)
            for ext, algo in ((".md5", hashlib.md5), (".sha1", hashlib.sha1)):
                if f + ext not in names:
                    errors.append("no %s checksum for %s" % (ext, f))
                    continue
                expected = zf.read(f + ext).decode("ascii").split()[0].strip().lower()
                actual = algo(data).hexdigest()
                if expected != actual:
                    errors.append("%s checksum of %s does not match" % (ext, f))

        for artifact in PUBLISHED:
            pom = "%s/%s/%s/%s-%s.pom" % (GROUP_PATH, artifact, version, artifact, version)
            if pom in names:
                errors.extend(check_pom(artifact, zf.read(pom)))

    if errors:
        print("Central bundle check FAILED (%d problems):" % len(errors), file=sys.stderr)
        for e in errors:
            print("  - " + e, file=sys.stderr)
        return 1
    print("Central bundle check passed: %d modules at %s, %d files %s"
          % (len(PUBLISHED), version, len(primaries),
             "signed and checksummed" if signed else "checksummed (signatures not required)"))
    return 0


def check_pom(artifact, content):
    """Central requires name, description, url, licences, developers and SCM; children inherit the
    last four from the parent, so only the parent must declare them."""
    errors = []
    root = ET.fromstring(content)

    def text(path):
        el = root.find(path, POM_NS)
        return el.text.strip() if el is not None and el.text else ""

    for field in ("m:name", "m:description"):
        if not text(field):
            errors.append("%s POM has no %s" % (artifact, field[2:]))
    if artifact == "kafka-connect-oracle-cdc-parent":
        for path, label in (
            ("m:url", "url"),
            ("m:licenses/m:license/m:name", "licence"),
            ("m:developers/m:developer/m:name", "developer"),
            ("m:scm/m:url", "scm url"),
            ("m:scm/m:connection", "scm connection"),
        ):
            if not text(path):
                errors.append("parent POM has no %s" % label)
    elif root.find("m:parent", POM_NS) is None:
        errors.append("%s POM has no parent to inherit Central metadata from" % artifact)
    return errors


if __name__ == "__main__":
    sys.exit(main(sys.argv))
