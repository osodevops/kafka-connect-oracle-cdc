# ADR-0012: Correctness oracle protocol

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Three assertions: table state equivalence at a quiesced check SCN against `SELECT ... AS OF SCN`, committed XID set equivalence with the workload ledger, and event-level invariants (no rolled-back rows, no uncommitted rows, monotonic `(commit_scn, thread, xid, event_index)`). ORA-01555 during the check marks the run inconclusive. The canonical normalisation table is shared with `verify_cutover.py`. The Phase 1a form, the ledger written inside each generator transaction and compared with the emitted transactions, is already in use by `EngineCorrectnessEngineIT`, `RestartNoLossConnectorIT` and the Strimzi edge-case harness.
