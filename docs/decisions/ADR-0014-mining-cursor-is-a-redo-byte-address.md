# ADR-0014: The mining cursor is a redo byte address, not an SCN

**Status:** Accepted (evidence in `oracle-cdc-core/src/main/resources/reference/redo-flush-lag.md`)
**Context:** found on 5 October 2026 by the journal integration test, and the cause of one
intermittent miss in `RestartNoLossConnectorIT`.

## Problem

Each mining step was bounded by SCN: rows with `SCN >= cursor AND SCN < current SCN`. A redo record
receives its SCN when the change is made, but with private redo strands the record reaches the
online redo log only when the transaction commits, the strand fills, or a log switch binds it. LGWR's
three-second cycle does not bind other sessions' strands. The spike shows uncommitted inserts
invisible to LogMiner immediately and still invisible four seconds later in some trials, visible
after a log switch in every trial, and always carrying the SCNs of the original changes, below the
bound read before them. A step that covered those SCNs therefore returned without the rows, the next
step started above them, and they were never mined. A small transaction whose commit SCN was assigned
just before a step's bound was read and written just after the step's query could vanish whole.

Debezium forces LGWR to write by committing to a flush table before every iteration, which requires
write access to the source. CORE-POS-5 and PP-14 rule that out for this connector. A time lag on the
bound was tried and rejected: the spike shows the delay is unbounded on an idle database.

## Decision

The redo log is append-only in redo byte address order (RS_ID, then SSN within a record), whatever
the SCNs of the records. The cursor between steps is therefore the redo byte address of the last row
applied, and the query selects rows after it (`RS_ID > ? OR (RS_ID = ? AND SSN > ?)`) with the SCN
bound kept only as the upper limit. LogMiner's `STARTSCN` is the first SCN of the log holding the
cursor, not the cursor's SCN, so a late-bound record with an earlier SCN is returned. The SCN side of
the cursor stays for log selection, metrics and heartbeats.

The position carries the same shape: `resume_rs_id` and `resume_ssn` next to `resume_scn`, the
resume point being the earlier, in redo order, of the cursor and the first capture of the oldest open
non-journaled transaction (or the last journaled record of a journaled one), with the lower of the
two SCNs as the floor. A restart re-reads from the resume point inclusive. Commit order for the skip
rule (CORE-POS-3) is redo order of the COMMIT rows within a thread (`last_commit_rs_id`,
`last_commit_ssn`), so a commit written later with a lower SCN is never skipped. Positions without
redo byte addresses, written before this decision, keep the SCN rules.

`RedoRecordId` compares in redo order first and by SCN only when a side has no redo byte address.
The journal reload boundary and the resume calculation use that order. RAC needs one cursor per
thread (CORE-POS-4) and is unchanged in scope.

## Consequences

- No write to the source database and no latency floor.
- The cost of a step is unchanged: LogMiner already read each log from its start for an SCN range.
- `FakeLogMiner.late(scn)` models a late-bound strand; the engine test proves the rows are mined and
  survive a restart.

## Amendments

PRD-00 CORE-MINE-4 and CORE-POS-1 to CORE-POS-3 as noted in the PRD.
