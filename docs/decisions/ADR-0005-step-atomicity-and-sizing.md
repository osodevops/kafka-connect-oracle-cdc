# ADR-0005: Step atomicity and sizing

**Status:** Accepted (implemented in P1-08 and P1-09)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

The window unit is the log count, starting at one log and doubling to `cdc.mining.max.logs.per.step` while steps finish within `cdc.mining.target.latency.ms`; SCN sub-ranges inside one log are used only for the last halvings. A timeout cancels the statement, halves the window and retries; three consecutive timeouts at one log raise `MiningStalledException`. A failed step is discarded whole: nothing of it is applied or buffered. A transient database error reopens the sessions and re-mines the same step (CORE-CONN-6); schemas are loaded before the buffer changes so a failure never leaves a half-applied step. Clarifies PRD-00 CORE-MINE-4 and CORE-MINE-5.
