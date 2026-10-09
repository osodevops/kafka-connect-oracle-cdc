# CLAUDE.md: Project context for AI assistants

## What this is

OSO CDC Connector for Oracle Database is OSO's open-source (Apache-2.0) change data capture
source for Apache Kafka Connect. It reads Oracle Database redo through LogMiner and replaces
Confluent's proprietary Oracle CDC Source and Debezium's Oracle connector for LogMiner users.
Docs: https://kafkacdcconnector.com. Maven group `sh.oso`.

Never use "Oracle" in the product name or any domain name (Oracle trademark guidelines). The
repository and plugin artefact names are descriptive (kafka-connect-oracle-cdc). Say "for Oracle
Database" and keep the non-affiliation notice in README, site footer and Hub listing. Never
publish database benchmark figures (Oracle development licence terms); publish the harness.

| Module | Purpose |
|---|---|
| `oracle-cdc-core` | Capture engine, schema registry, doctor rules; test-jar `testkit` (FakeLogMiner, FaultyJdbc, FakeCatalog) |
| `kafka-connect-oracle-cdc` | `sh.oso.connect.oracle.OracleCdcSourceConnector`; builds the Connect plugin ZIP |
| `oracle-cdc-doctor` | Preflight checker, setup-sql generator, redo profiler, admin CLI (picocli, shaded JAR, container image) |
| `e2e-tests` | Testcontainers suites against Oracle Database Free and a real Connect worker; CORE-REF spikes; regression corpus; documentation generators |
| `bench` | Workload generator, materialiser and correctness oracle (no repo dependencies) |
| `tools/migration` | `migrate_from_debezium.py`, `migrate_from_confluent.py`, `takeover_scn.py`, `verify_cutover.py` |
| `docker/test-oracle` | Derived Oracle Free test image (ARCHIVELOG, supplemental logging, FREEPDB2, capture user) |
| `lab/` | Local compose stack, RAC and Data Guard scripts, AWS and OCI Terraform |
| `ops/` | JMX exporter config, Grafana dashboard, Prometheus alert rules |
| `website/` | Docusaurus docs site |
| `docs/` | PRD package and `docs/decisions/` ADRs (kept in repo, not published) |

## Building and testing

```bash
mvn clean verify -DskipE2E                        # all quality gates and T0 unit tests, no Docker
mvn -pl e2e-tests verify -De2e.groups=engine      # T1 against Oracle Database Free only
mvn -pl e2e-tests verify -De2e.groups=connector   # T1 with Kafka and a Connect worker as well
mvn clean package -DskipTests                     # plugin ZIP in kafka-connect-oracle-cdc/target
mvn spotless:apply                                # format before committing
gitleaks dir . --config .gitleaks.toml --redact   # secret scan; must be clean before every push
```

Build on JDK 17 or 21. `.mvn/jvm.config` adds the `jdk.compiler` exports that google-java-format
needs on JDK 17; the pinned formatter version must stay the newest Spotless allows on JDK 17
(1.28.0 at the time of writing; newer builds target Java 21).

Test tiers (docs/testing_strategy.md): `*Test` under surefire in every module (FakeLogMiner, no
Docker); `*IT` under failsafe only in `e2e-tests`. The tier is the class-name suffix:
`*EngineIT` (Oracle only), `*ConnectorIT` (Oracle, Kafka and a Connect worker), `*NightlyIT`
(faults and long runs), `*QualIT` (qualification: the container, or an external database such as
RDS with `-De2e.external.url`), kept in matching packages `e2e/engine`, `e2e/connector`,
`e2e/nightly`, `e2e/qual`.
`-De2e.groups=<tier>` selects a tier and fails the build if nothing matches. Do not rely on
surefire's `groups` tag filter or directory include patterns: the forked JVM re-filters classes
and only the plain suffix form survives. One Oracle container per JVM; tests isolate by schema,
never by restarting the database.

## Non-negotiable rules

- No silent data loss. Any unexpected condition stops the task with a typed
  `OracleCdcException`. Never add a config option that continues past missing redo,
  corruption or decode failure for captured tables.
- Never read redo or archive files directly. LogMiner only.
- Never copy code from Confluent or OpenLogReplicator. Debezium code may only be copied with
  its Apache-2.0 header kept and an entry in NOTICE, and only after review.
- Offsets only encode what the framework acknowledged.
- Every data-loss bug gets a regression test before the fix merges. Regression tests are
  named by invariant and tagged with the public issue (`@Tag("dbz-2504")`).
- No GPL or AGPL dependencies (JMH included); the licence allowlist in the build enforces it.
- No secrets, and nothing that looks like one. The repository is public: whatever is pushed is
  published and scanned (GitGuardian flagged a lab `--password` value on 8 October 2026). Real
  credentials (tokens, keys, passwords, AWS account secrets, signing material) never enter a file,
  commit message, PR, issue, evidence file or log. Even throwaway lab credentials are never
  written as a command-line option value or inside a JDBC URL or `user/password@` connect
  string: use `--password-env` or an environment variable, and `sqlplus / as sysdba` inside a
  container. Deliberately fake values in tests are named `CANARY-...` and marked
  `gitleaks:allow`, or live in the allowlisted paths of `.gitleaks.toml`. Run
  `gitleaks dir . --config .gitleaks.toml --redact` before every push; CI runs it too, and a
  finding blocks the push. Never rewrite published history to hide a value: rotate it if it is
  real, then fix forward.
- Never weaken or delete a failing regression test without explicit approval.

## Releasing

Conventional commits on main drive release-please. Merging the release PR tags vX.Y.Z; the
release workflow validates the POM version, builds, signs, deploys to Maven Central and attaches
the plugin ZIP and oracle-cdc-doctor image. Never tag by hand. A release requires the release
gate in docs/testing_strategy.md section 9. Commits carry a DCO sign-off (`git commit -s`).

## Support and security policy (public)

SUPPORT.md and SECURITY.md are the source of truth for commitments. Client-facing text must stay
consistent with them. Do not claim 24x7 support or features that are not in a published release.

## Conventions

- Config keys are `cdc.*`, documented only in the generated reference
  (website/docs/reference/configuration), regenerated by e2e-tests. Do not hand-edit those files.
- Passwords are Connect PASSWORD configs and never logged; row values are never logged unless
  `cdc.log.sensitive.data=true`.
- Design changes to the PRDs are recorded as ADRs in `docs/decisions/` plus a minimal PRD edit.
- Spike outputs under `oracle-cdc-core/src/main/resources/reference/` record facts and
  decisions, never timings.
- House style for client-facing text: UK English, no em dashes, no emoji, no tildes or arrow
  glyphs in prose, ranges written as words, inline code only for genuine identifiers.
- `sales/` is gitignored and never committed.
