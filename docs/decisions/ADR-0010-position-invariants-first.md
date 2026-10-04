# ADR-0010: Position invariants first

**Status:** Accepted (implemented in P1-07)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

`Position` is a versioned record from day one: resume SCN, last acknowledged commit (SCN, thread, container-qualified XID) and its event index, journal generation, schema epoch, database identity (DBID and RESETLOGS SCN), released XIDs, an opaque snapshot block and unknown keys preserved for a one-version downgrade. Offsets are primitive-only maps (lists and maps kill a Connect task at the first flush). Every record carries the position a restart may use once that record is acknowledged; records before the last of a transaction keep the resume SCN at that transaction's first capture; the resume candidate is computed when a transaction is emitted, never at step end. A heartbeat record at start makes a fresh connector's start SCN durable before the first change.
