# ADR-0006: Orphan release safety

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Absence from `GV$TRANSACTION` is advisory. A transaction is released as orphaned only after three negatives: absent from `GV$TRANSACTION` twice, no COMMIT or ROLLBACK found when re-mining from its last seen SCN to the safe SCN, and the owning `SESSION#`/`SERIAL#` gone. Released XIDs persist in a bounded ledger inside the position; a later COMMIT for a released XID is `OrphanReleaseViolationException` (stop). Amends PRD-00 CORE-TX-7. Implemented in Phase 1b (P1-15).
