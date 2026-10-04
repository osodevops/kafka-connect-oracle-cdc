# ADR-0003: Transaction journal atomicity

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Journal chunks are emitted as `SourceRecord`s from `poll()` to the journal topic, never from a side producer, so under exactly-once they share the Kafka transaction with data and offsets, and in at-least-once mode Connect acknowledges them in order. The resume position never advances past a journaled transaction until the chunk's record was acknowledged. Keys are `{xid, con_id, chunk, journal_generation}`; `journal_generation` increments per task start and chunks newer than the position's generation are unacknowledged writes, discarded and tombstoned on load. Amends PRD-00 CORE-TX-4 and CORE-TX-5. Implemented in Phase 1b (P1-14).
