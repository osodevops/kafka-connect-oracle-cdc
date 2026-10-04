# Contributing

Thank you for helping build the OSO CDC Connector for Oracle Database.

## Getting started

- JDK 17 or 21, Maven 3.9 or later, Docker for the `e2e-tests` module.
- `mvn clean verify -DskipE2E` runs every quality gate and unit test without Docker.
- `mvn -pl e2e-tests verify -De2e.groups=engine` runs the Oracle Database Free suites.
- `mvn spotless:apply` formats Java and POM files before you commit.

## Pull requests

1. Fork and branch from `main`.
2. Add tests. Engine logic is tested against `FakeLogMiner` in the module; anything that needs
   Oracle or Kafka goes in `e2e-tests`. A data-loss fix is not complete without a regression test
   named by the invariant it protects.
3. Use conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`, `chore:`). They drive
   release-please.
4. Sign off every commit (`git commit -s`) to certify the Developer Certificate of Origin
   (https://developercertificate.org).
5. Run `mvn clean verify` locally.
6. Update documentation under `website/docs/` where behaviour changes; the configuration reference
   is generated, do not hand-edit it.

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
