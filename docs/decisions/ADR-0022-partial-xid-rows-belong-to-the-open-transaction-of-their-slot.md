# ADR-0022: Rows with a partial XID belong to the open transaction of their undo slot

**Status:** Accepted
**Context:** PRD-00 CORE-TX-2, `LogSwitchStormNightlyIT` and `BrokerRestartNightlyIT` on GitHub
runners, 7 October 2026.

## Context

The first nightly run on GitHub-hosted runners (native x86, Oracle Database Free 23.26.3) failed
the correctness oracle without losing or duplicating a transaction: 35 rows in Kafka that the
database did not have, 10 stale values and 1 missing row across two suites. The same suites pass
on an Apple Silicon workstation, where the database runs emulated and much slower.

LogMiner had given rows written by a rollback the transaction sequence 0xFFFFFFFF instead of the
real one: the ROLLBACK row of a full rollback and the `ROLLBACK=1` undo rows of a rollback to a
savepoint. The buffer matched undo rows by the full XID, so they reached no transaction; the
transaction then committed with the changes the database had rolled back to its savepoint, which
is exactly the extra, stale and missing rows the oracle found. A ROLLBACK with the partial XID left
its transaction open until the orphan check. Oracle documents XIDUSN, XIDSLT and XIDSQN but not the
partial form.

## Decision

A row whose XIDSQN is 0xFFFFFFFF (`Xid.PARTIAL_SQN`) belongs to the transaction open in the same
container, undo segment (XIDUSN) and slot (XIDSLT) at that point of the redo. An undo slot holds one
active transaction at a time, so the pairing is unambiguous while that transaction is open. The
engine resolves each step's rows in order before anything else sees them (`PartialXidResolver`):
the open set starts from the buffer's open transactions and follows the step, adding a transaction
at its first row with a full XID and removing it at its COMMIT or ROLLBACK. A partial row is given
the full key of the open transaction of its slot; a partial row whose slot has no open transaction
undoes nothing that was captured and is dropped. The pair (USN, slot) is never kept as an identity
beyond that.

## Consequences

- A rollback to a savepoint removes the changes it undoes even when LogMiner reports its undo rows
  with the partial XID, and a ROLLBACK with the partial XID ends its transaction.
- Every consumer of a row's transaction (buffer, LOB assembly, orphan check, first-start scope,
  journal) sees the resolved key, because resolution happens before them.
- `RollbackRowsWithAPartialXidTest` (tag `partial-xid`) failed on three of its four cases before the
  change. The nightly log switch storm and broker restart suites are the end-to-end check, and only
  hosted runners have reproduced the partial XID so far.

## Evidence

Nightly run 37647409645 on `osodevops/kafka-connect-oracle-cdc` (7 October 2026): the correctness
oracle's FAIL verdicts for `LogSwitchStormNightlyIT` and `BrokerRestartNightlyIT` on
23.26.3-slim-faststart, and `MiningSessionEngineIT` on the same image, where the ROLLBACK row of the
rolled-back insert carried XID `5.0.4294967295` for the transaction `5.0.614`. An earlier CI run
showed the undo row of that insert with the partial XID too.

## PRD edits

PRD-00 CORE-TX-2 gains a sentence naming this ADR.
