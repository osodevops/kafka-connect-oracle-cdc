# ADR-0025: Standby capture is archive-only on an open physical standby, bounded by the applied SCN

**Status:** Accepted. Implemented: the role and open-mode rules, the applied-SCN bound, the
re-check after a reconnect and dictionary builds on the primary. Still to come: qualification on
the Data Guard lab.
**Context:** PRD-01 Phase 2 (archive-log-only standby), PRD-00 CORE-LOG-6 and CORE-CONN-5; research
on Data Guard and on Debezium's standby support, 8 and 9 October 2026.

## Context

Capturing from a physical standby takes mining load off the primary and, after a failover, the
standby holds exactly the redo the new primary has. A physical standby applies the primary's redo
block for block, so SCNs, redo addresses and the database identity (DBID, RESETLOGS SCN) are the
primary's. It has no online redo of its own to mine and, open read-only, cannot write: no
dictionary build, no signal or flush table. Debezium captures from a standby only when it is open
read-only (Active Data Guard), in archive-log-only mode, with the dictionary taken from the primary,
and calls the feature incubating.

Before this decision the task accepted any database role and open mode at start: doctor rule
DOC-14 blocked online mode on a non-primary in `validate()` only, and nothing refused a mounted,
logical or snapshot standby. On a standby open read-only with apply, the archive-only safe end
(the highest SCN every thread has archived to) can be ahead of the SCN redo apply has reached, and
rows past it would be decoded against a dictionary that does not have their DDL yet.

## Decision

1. **Which shapes are mined, by capture mode** (`TopologyGuard.roleRefusal`, shared by the task
   start, the reconnect check and DOC-14):
   - online: a PRIMARY open READ WRITE only;
   - archive_only: a PRIMARY open READ WRITE, or a PHYSICAL STANDBY open READ ONLY or READ ONLY
     WITH APPLY;
   - refused in both: MOUNTED (no dictionary to read), LOGICAL STANDBY and SNAPSHOT STANDBY (redo
     and SCNs of their own).
   A refusal stops the task with `TopologyException` (CDC-5001) before any offset is read.
2. **The archive-only safe end never passes the database's current SCN.** On a physical standby
   that is the applied SCN; on a primary the bound never binds.
3. **After every reconnect** the role and open mode are checked again with the identity (ADR-0023
   amendment). A switchover that turns the captured standby into the primary keeps archive-only
   capture going; a role the capture mode cannot mine stops the task.
4. DOC-14 warns about a physical standby open READ ONLY without apply: archived logs arrive but
   the safe end does not move until apply runs.
5. Dictionary builds (`DBMS_LOGMNR_D.BUILD`) cannot run on a read-only standby. The key
   `cdc.dictionary.database.url` names the primary they run on, with the connector's credentials;
   the build reaches the standby through the redo. Without it, builds on a standby are switched off
   at start with an ops event, and DOC-24 blocks the configuration in `validate()`. DOC-24 never
   echoes the URL, which can carry a password. Snapshots stay on the captured database, reading
   `AS OF SCN` at an applied SCN.

## Consequences

- Standby capture is accepted but **not qualified** until the Data Guard lab run; the docs say so.
- A mounted standby, which DOC-14 used to accept in archive-only mode, is now refused: LogMiner
  could not decode its redo with the online catalog anyway.
- One more `V$DATABASE` read per archive-only step for the bound.
- A failover still stops the task: the new primary has a new RESETLOGS SCN (ADR-0023 amendment).

## Evidence

`TopologyGuardTest`, `RulesTest.standbyNeedsArchiveOnlyModeAndAnOpenDatabase`,
`LogInventoryTest` (applied bound) and `OracleCdcSourceTaskTest` (online on a standby refused at
start). The standby's own `V$ARCHIVED_LOG` rows, `CURRENT_SCN` as the applied point and flashback
queries on the standby are confirmed on the lab.

## PRD edits

PRD-00 CORE-LOG-6 (bounded by the applied SCN; the accepted roles). PRD-05 DOC-14.

## Amendments

None.
