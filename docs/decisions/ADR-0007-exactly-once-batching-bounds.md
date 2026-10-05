# ADR-0007: Exactly-once batching bounds

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Kafka transactions need byte and time bounds as well as record counts: `cdc.eos.batch.max.bytes` (default 16 MiB) and `cdc.eos.split.max.bytes` (default 256 MiB). Doctor rule DOC-18 reads the broker's `transaction.max.timeout.ms` and validates the configuration with `exactly.once.support=required`. Connect exactly-once needs distributed mode; MSK Connect is treated as at-least-once until the T4 qualification verifies it. Implemented in Phase 1c (P1-12).

## Amendment, 5 October 2026 (P1-12)

Implemented with batch-level commits: each record on the task's queue says whether a Kafka
transaction may end after it (the last record of an Oracle transaction, or a heartbeat, ops, journal
or DLQ record) and whether it must (a split). `EosBoundaries` returns a batch that stops at the chosen
record, calls `TransactionContext.commitTransaction()` for that batch, and holds the rest for the next
poll, so the commit never depends on the worker matching record objects. A Kafka transaction also
ends when the queue is empty at a boundary, so idle periods add no latency.

`cdc.eos.split.max.records` and `cdc.eos.split.max.bytes` split a large Oracle transaction at exact
event indexes. PRD-01 SRC-EOS-4 asks for `source.cdc.split=true`; field names with dots are not valid
in a Connect schema and a new source field would change every record, so split records carry the
header `cdc.split=true` instead, and the `transaction-split` ops event names the transaction.

DOC-18 (broker `transaction.max.timeout.ms` against the `cdc.eos.*` bounds) needs broker access from
the doctor and moves to P1-24 with the other doctor rules. Proven by `EosBoundariesTest`, the
exactly-once task test, and `ExactlyOnceConnectorIT` (a read_committed consumer sees every row once
and every Oracle transaction whole through two worker kills, and one split transaction).

