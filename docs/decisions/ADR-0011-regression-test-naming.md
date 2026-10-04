# ADR-0011: Regression test naming

**Status:** Accepted
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Regression tests are named by the invariant they protect, carry `@Tag("dbz-nnnn")` with a Javadoc link to the public issue, and a corpus index test keeps `docs/testing_strategy.md` section 5 and the package in sync. Test tiers are class-name suffixes (`*EngineIT`, `*ConnectorIT`, `*NightlyIT`) because failsafe's forked JVM honours only suffix includes.
