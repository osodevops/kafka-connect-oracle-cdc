# ADR-0011: Regression test naming

**Status:** Accepted
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Regression tests are named by the invariant they protect, carry `@Tag("dbz-nnnn")` with a Javadoc link to the public issue, and a corpus index test keeps `docs/testing_strategy.md` section 5 and the package in sync. Test tiers are class-name suffixes (`*EngineIT`, `*ConnectorIT`, `*NightlyIT`) because failsafe's forked JVM honours only suffix includes.

## Amendments

### 9 October 2026: the qualification tier

A fourth suffix, `*QualIT` in package `e2e/qual`, selected by `-De2e.groups=qual` (profile
`e2e-qual`). Qualification suites are written against `support/TestDatabase`, so the same classes
run against the Oracle Database Free container (nightly, so they cannot rot) and against an
external database named by `-De2e.external.url` (the T4 runs on Amazon RDS, a standby or RAC). They
use only what every target allows: no SYSDBA, no container commands, no instance restarts. The
evidence file of an external run records the target, never a credential.

