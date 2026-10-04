# ADR-0007: Exactly-once batching bounds

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Kafka transactions need byte and time bounds as well as record counts: `cdc.eos.batch.max.bytes` (default 16 MiB) and `cdc.eos.split.max.bytes` (default 256 MiB). Doctor rule DOC-18 reads the broker's `transaction.max.timeout.ms` and validates the configuration with `exactly.once.support=required`. Connect exactly-once needs distributed mode; MSK Connect is treated as at-least-once until the T4 qualification verifies it. Implemented in Phase 1c (P1-12).
