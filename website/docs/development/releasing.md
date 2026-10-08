---
title: Releasing
description: How releases are made with release-please, the gates a release must pass, and what each release publishes.
---

# Releasing

Only release-please creates releases. Conventional commits on `main` (`feat:`, `fix:`, `docs:` and
the rest) keep a release pull request open with the next version and its changelog. Merging that
pull request creates the release and its `vX.Y.Z` tag, and the release workflow publishes it.
Nobody creates a tag or a GitHub release by hand, and nobody edits a version in a POM: release-please
moves the parent and every module together. After each release, release-please first opens a pull
request that moves `main` to the next `-SNAPSHOT` version.

Every commit carries a Developer Certificate of Origin sign-off (`git commit -s`).

## Gates

The release workflow publishes nothing unless, on the released commit:

- the quality gates and unit tests pass on Java 17 and 21;
- the T1 engine and connector tiers pass against Oracle Database Free;
- the Maven Central dry run passes: the release profile is built, signed with a throwaway key,
  uploaded to a local stand-in for the Central Portal and every file of the bundle is checked;
- the latest nightly run on `main` passed within the last seven days and kept its T2 evidence.

Before 1.0, a release is a preview: the 72-hour soak and the qualification on Oracle Database 19c
and 21c are part of the gate for 1.0.

## What a release publishes

| Where | What |
|---|---|
| [Maven Central](https://central.sonatype.com/namespace/sh.oso) | `oracle-cdc-core`, `kafka-connect-oracle-cdc` and `oracle-cdc-doctor` under the group `sh.oso`, signed |
| [GitHub release](https://github.com/osodevops/kafka-connect-oracle-cdc/releases) | The plugin ZIP, the `oracle-cdc-doctor` CLI jar, an SPDX SBOM of the ZIP and `SHA256SUMS` |
| GitHub Container Registry | `ghcr.io/osodevops/oracle-cdc-doctor`, tagged with the version and `latest`, for amd64 and arm64 |

The changelog of every release is in `CHANGELOG.md` and on its GitHub release page.

## Running the dry run locally

The Maven Central dry run needs no secrets:

```bash
release/central-dry-run.sh
```

Everything it writes stays under `target/`. On macOS, point `CENTRAL_DRY_RUN_GNUPGHOME` at a short
path such as `/tmp/cdr-gnupg`, because the GnuPG agent's socket path is limited in length.
