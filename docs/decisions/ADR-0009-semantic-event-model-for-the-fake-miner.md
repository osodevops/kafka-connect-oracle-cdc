# ADR-0009: Semantic event model for the fake miner

**Status:** Accepted (implemented in P1-01)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

`FakeLogMiner` scripts semantic events (`TxStart`, `Dml`, `Commit`, `Rollback`, `Ddl`, `Unsupported`, `MissingScn`, `LogBoundary`) with faults at chosen positions; `LogMinerRowAdapter` maps real V$LOGMNR_CONTENTS rows to the same events and is tested only against Oracle Database Free. The engine, buffer, scheduler and position logic are therefore never coupled to the fake's quirks. Amends PRD-00 CORE-TEST-1.
