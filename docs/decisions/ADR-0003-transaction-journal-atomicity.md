# ADR-0003: Transaction journal atomicity

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Journal chunks are emitted as `SourceRecord`s from `poll()` to the journal topic, never from a side producer, so under exactly-once they share the Kafka transaction with data and offsets, and in at-least-once mode Connect acknowledges them in order. The resume position never advances past a journaled transaction until the chunk's record was acknowledged. Keys are `{xid, con_id, chunk, journal_generation}`; `journal_generation` increments per task start and chunks newer than the position's generation are unacknowledged writes, discarded and tombstoned on load. Amends PRD-00 CORE-TX-4 and CORE-TX-5. Implemented in Phase 1b (P1-14).

## Amendment (5 October 2026, before P1-14)

Three rules make the journal and the resume position consistent without a side producer:

1. **Continuous chunking.** A transaction, once journaled, has every later change appended as a
   new chunk at the end of each mining step, and undo rows are journaled as undo markers resolved
   at commit (the same rule as the spill files). The journal is therefore complete up to the last
   journaled redo record, not only up to the moment the threshold was crossed.
2. **Resume rule.** `resume_scn` is the minimum of the safe mined SCN, the first captured SCN of
   every open non-journaled transaction, and for every journaled transaction the SCN after its last
   acknowledged chunk. A journaled transaction no longer pins the position at its start (CORE-POS-2)
   but still bounds it to what the journal holds.
3. **Reload boundary.** On restart the loader applies journaled events whose redo record SCN is
   below `resume_scn` and drops the rest, because mining restarts at `resume_scn` and produces
   those rows again. Chunks of a generation newer than the position's are unacknowledged writes and
   are discarded and tombstoned; a gap in chunk numbers is `JournalCorruptionException`.

Journaling requires `cdc.kafka.bootstrap.servers`, because the loader reads the compacted topic
with the connector's own consumer. Without it the thresholds are ignored with a warning at start
and a validation finding, and long transactions pin the position as before.
