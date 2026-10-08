# Contributing

Thank you for helping build the OSO CDC Connector for Oracle Database.

## Getting started

- JDK 17 or 21 and Docker for the `e2e-tests` module. The Maven wrapper (`./mvnw`) fetches
  Maven 3.9.16, the version CI uses.
- `./mvnw clean verify -DskipE2E` runs every quality gate and unit test without Docker.
- `./mvnw -pl e2e-tests verify -De2e.groups=engine` runs the Oracle Database Free suites, and
  `-De2e.groups=connector` the suites with Kafka and a Connect worker as well.
- `./mvnw spotless:apply` formats Java and POM files before you commit.

## Pull requests

1. Fork and branch from `main`.
2. Add tests. Engine logic is tested against `FakeLogMiner` in the module; anything that needs
   Oracle or Kafka goes in `e2e-tests`. A data-loss fix is not complete without a regression test
   named by the invariant it protects and tagged with the public issue (`@Tag("dbz-2504")`).
3. Use conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`, `chore:`). They drive
   release-please.
4. Sign off every commit (`git commit -s`) to certify the Developer Certificate of Origin
   (https://developercertificate.org).
5. Run `./mvnw clean verify` locally.
6. Update documentation under `website/docs/` where behaviour changes; the configuration reference
   is generated, do not hand-edit it.

## Continuous integration

| Workflow | When | What it does |
|---|---|---|
| `ci.yml` | Every pull request and every push to `main` | Quality gates and unit tests on Java 17 and 21, the T1 engine and connector tiers against Oracle Database Free, the licence gate self-test, a Maven Central dry run, actionlint, and a check that no workflow creates tags or releases |
| `nightly.yml` | Every night, or on demand | The T2 fault and correctness suites and the T1 tiers on both Oracle Database Free images in the test matrix; evidence JSON and test reports kept as artefacts; a failure on `main` opens or updates an issue labelled `nightly-failure` |
| `codeql.yml` | Pull requests, pushes and weekly | CodeQL for Java, Python and the workflows |
| `dco.yml` | Pull requests | Every commit carries a `Signed-off-by` line |
| `dbz-issue-watch.yml` | Weekly | Lists Oracle connector issues closed in debezium/dbz for triage into the regression corpus; read-only |

The Maven Central dry run needs no secrets and can be run locally with
`release/central-dry-run.sh`. It builds the release profile, signs with a throwaway key, uploads
to a local stand-in for the Central Portal and checks the bundle; everything it writes stays under
`target/`.

## Releases

- Only release-please creates releases. Conventional commits on `main` keep a release pull request
  open; merging it creates the release and its `vX.Y.Z` tag, and `release.yml` publishes it. Never
  create a tag or a GitHub release by hand, and never edit the version in a POM: release-please
  moves the parent and every module together.
- `release.yml` refuses to publish unless, on the released commit, the quality gates and unit tests
  pass on Java 17 and 21, the T1 engine and connector tiers pass, and the Central dry run passes,
  and unless the latest nightly run on `main` passed within the last seven days with T2 evidence.
  The T3 soak and the T4 platform qualification are confirmed by the maintainer who merges the
  release pull request (`docs/testing_strategy.md`, section 9).
- A release publishes the signed artefacts to Maven Central under the group `sh.oso`; attaches the
  plugin ZIP (`osodevops-kafka-connect-oracle-cdc-<version>.zip`), the `oracle-cdc-doctor` CLI jar,
  an SPDX SBOM and `SHA256SUMS` to the GitHub release (with build provenance attestations when the
  repository is public); and pushes `ghcr.io/osodevops/oracle-cdc-doctor:<version>` for amd64 and
  arm64 with an SBOM and provenance.
- Maintainers check the publishing secrets with the manual `verify-release-secrets.yml` workflow
  before the first release and after rotating any of them.

## Clean-room rules

- Do not use Confluent Oracle CDC source code, binaries or non-public material, and do not
  install the Confluent connector on machines used for development of this project.
- Property names and behaviour of other connectors may be taken only from their public
  documentation, cited in `docs/research/`.
- Debezium is Apache-2.0. The default is to write our own code. If a Debezium source file is
  ever copied, it keeps its licence header and is listed in `NOTICE`, after review.
- No GPL or AGPL code or dependencies (the build's licence allowlist enforces this).
- Never read redo or archive files directly. LogMiner is the only capture API.

## Reporting issues

Use the issue templates. Redact credentials and any customer data from configurations and logs.
Security vulnerabilities go through the process in `SECURITY.md`, not public issues.
