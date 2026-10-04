# ADR-0004: Index-organised tables and ROWID chunking

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Index-organised tables always chunk by primary key (their ROWIDs are logical); heap tables chunk by key when one exists; ROWID ranges from `DBA_EXTENTS` are used only for keyless heap tables. A keyless table with ROW MOVEMENT enabled gets doctor warning DOC-7 and a documented limitation. Amends PRD-02 section 3 step 1. Implemented in Phase 1c (P1-19).
