# ADR-0023: Unqualified database shapes are refused at start

**Status:** Accepted
**Context:** PRD-00 CORE-CONN-5 and CORE-POS-4; code review of the mining cursor against
`V$THREAD` while planning RAC, RDS, standby and per-PDB capture, 8 October 2026.

## Context

Release 0.1.1 documents Oracle RAC as not supported, but nothing enforced it. The task read
`V$DATABASE` for the offset identity only; `TopologyProbe` (CORE-CONN-5) was called by the doctor
and the admin tools, never by the task; doctor rule DOC-13 was information and not part of fast
mode, so `validate()` never reported it. `LogInventory` adds the logs of every enabled thread, so
on a two-node RAC the task starts and mines.

The mining cursor is a redo byte address (ADR-0014) whose first field is the log sequence of one
thread, so comparing addresses across threads means nothing. `CommitOrder` orders commits by
thread before address, and `SkipRule` applies that order on restart. On RAC a restart therefore
skips every acknowledged-looking commit of the lower-numbered thread and redelivers the commits of
the higher one, and within a step the `RS_ID > ?` clause drops the other thread's rows. That is
silent loss, which the first rule of this project forbids.

## Decision

1. The task probes the topology before it reads any offset (`EngineFactory.Session.topology()`,
   backed by `TopologyProbe`) and `TopologyGuard.requireQualified` stops the start with
   `TopologyException` (CDC-5001) when more than one redo thread is enabled. A thread that
   `V$THREAD` shows DISABLED (an instance that left the cluster) does not count.
2. The engine stops with CDC-5001 on the first mined event whose `THREAD#` the start did not
   qualify, before that event is buffered. A thread enabled after the start cannot be placed by
   the single cursor, and the aborted step is never acknowledged, so a restart re-mines it (and is
   then refused by rule 1).
3. DOC-13 is a blocking finding in fast mode, attached to `cdc.database.host` by the validator,
   until the per-thread position (CORE-POS-4) exists; it then returns to information.
4. `cdc.rac.safety.lag.ms` and `cdc.database.fan.enabled` stay reserved and unread.
5. The archive destination is chosen in one place, `TopologyProbe`; the duplicate in
   `JdbcEngineFactory` is removed.

## Consequences

- A RAC database cannot start the connector in 0.1.x. Before this change it could, unsafely.
- Operators see the refusal twice: in `validate()` (DOC-13, with the thread list) and in the task
  status (CDC-5001). The runbook for CDC-5001 gains the case.
- Single-instance databases pay one topology probe per task start (`V$THREAD`, `V$PDBS`,
  `V$ARCHIVE_DEST_STATUS`), which the doctor already ran.
- The RAC release lifts the guard with its own ADR, together with the per-thread position.

## Evidence

`StopsWhenASecondRedoThreadAppearsTest` (tag `rac-single-thread`) fails on the tree before this
change: the rows of thread 2 were applied silently. `OracleCdcSourceTaskTest`
`refusesADatabaseWithTwoEnabledRedoThreadsAtStart` and `RulesTest`
`aSecondEnabledRedoThreadIsBlockingInFastMode` cover the start and the doctor.

## PRD edits

PRD-00 CORE-CONN-5: "Shapes the release is not qualified for stop the task with
`TopologyException` (ADR-0023)". PRD-05 DOC-13: Blocking until RAC is qualified, then Info.

## Amendments

None.
