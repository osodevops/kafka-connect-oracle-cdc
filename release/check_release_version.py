#!/usr/bin/env python3
"""Checks that a release tag, the requested version and every version in the tree agree.

Usage: check_release_version.py <tag> <version> [repository root]

Fails (exit 1) unless: the version is a plain release (no -SNAPSHOT), the tag is "v" followed by the
version, the parent POM's version and every module's parent version equal it, no module declares a
version of its own, and .release-please-manifest.json records it. release-please moves all of these
together when it opens the release pull request; a mismatch means the tree was edited by hand.
"""

import json
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

NS = {"m": "http://maven.apache.org/POM/4.0.0"}
RELEASE = re.compile(r"^\d+\.\d+\.\d+(-[0-9A-Za-z.]+)?$")


def text(root, path):
    el = root.find(path, NS)
    return el.text.strip() if el is not None and el.text else None


def main(argv):
    if len(argv) not in (3, 4):
        print(__doc__, file=sys.stderr)
        return 2
    tag, version = argv[1], argv[2]
    repo = pathlib.Path(argv[3] if len(argv) == 4 else ".")
    errors = []

    if not RELEASE.match(version) or "SNAPSHOT" in version.upper():
        errors.append("version %r is not a release version (X.Y.Z, never a SNAPSHOT)" % version)
    if tag != "v" + version:
        errors.append("tag %r does not match version %r (expected v%s)" % (tag, version, version))

    parent = ET.parse(repo / "pom.xml").getroot()
    parent_version = text(parent, "m:version")
    if parent_version != version:
        errors.append("pom.xml version is %s, expected %s" % (parent_version, version))

    modules = [m.text.strip() for m in parent.findall("m:modules/m:module", NS)]
    if not modules:
        errors.append("pom.xml lists no modules")
    for module in modules:
        pom = ET.parse(repo / module / "pom.xml").getroot()
        inherited = text(pom, "m:parent/m:version")
        own = text(pom, "m:version")
        if inherited != version:
            errors.append("%s/pom.xml parent version is %s, expected %s" % (module, inherited, version))
        if own is not None and own != version:
            errors.append("%s/pom.xml declares its own version %s" % (module, own))

    manifest = json.loads((repo / ".release-please-manifest.json").read_text(encoding="utf-8"))
    if manifest.get(".") != version:
        errors.append(".release-please-manifest.json records %s, expected %s"
                      % (manifest.get("."), version))

    if errors:
        print("Release version check FAILED:", file=sys.stderr)
        for e in errors:
            print("  - " + e, file=sys.stderr)
        return 1
    print("Release version check passed: %s, %d modules, manifest and tag agree" % (version, len(modules)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
