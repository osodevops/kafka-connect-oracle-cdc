# ADR-0027: Per-PDB mining: Oracle chooses the logs, the connector keeps the stop conditions

**Status:** Accepted. Implemented: `cdc.mining.mode` and range mode. Still to come: qualification on
Amazon RDS with the CDB architecture (dev-account lab) and on Autonomous Database (needs an OCI
account).
**Context:** PRD-01 Phase 3 (per-PDB mining for Autonomous Database and RDS CDB), PRD-00
CORE-LOG-1/2 and CORE-MINE-1; the `pdb-local-mining` reference spike, 9 October 2026.

## Context

With the CDB architecture on Amazon RDS (every release from 21c, and 19c by choice) the client
always connects to the tenant database as a local user and cannot reach CDB$ROOT; Autonomous
Database allows LogMiner only as `START_LOGMNR` with an SCN or time range. The engine adds every
log of a step with `DBMS_LOGMNR.ADD_LOGFILE` and mines at CDB$ROOT as a common user.

The spike (`oracle-cdc-core/src/main/resources/reference/pdb-local-mining.md`) established on
Oracle Database Free, connected to FREEPDB1 as a local user with LOGMINING:

- `ADD_LOGFILE` is refused inside a PDB (ORA-65040); `START_LOGMNR` with an SCN range and the online
  catalog works without it, and the rows are visible before a log switch;
- `V$LOGMNR_LOGS` is populated, and `V$ARCHIVED_LOG`, `V$LOG` and `V$THREAD` are readable from the
  PDB, so the logs a step needs can still be listed and checked for continuity;
- RS_ID, SSN, THREAD#, XID and SRC_CON_ID carry values (SRC_CON_ID is the PDB's), so the redo byte
  address cursor (ADR-0014) and the container keys work unchanged;
- a range below the oldest redo fails with ORA-01291;
- a dictionary from the redo with DDL tracking is not available from a PDB (ORA-01371).

## Decision

1. New key `cdc.mining.mode`: `logs` (adds the logs, as before), `range` (START_LOGMNR with the SCN
   range only; Oracle chooses the logs) and `auto` (the default: range when the connection is to a
   pluggable database, logs otherwise). `logs` on a PDB connection stops the start with CDC-5001.
2. In range mode the log inventory still lists the logs of every step and runs the continuity
   checks, so a gap or a purged log stops the task exactly as in logs mode (CDC-2001, CDC-2002);
   only `ADD_LOGFILE` is skipped. Oracle's own ORA-01291 for an uncovered range is classified as
   before.
3. The lag-case replay (P1-17) needs a dictionary from the redo, which a PDB cannot use: in range
   mode such rows stop the task with `DictionaryUnavailableException` (CDC-6001), and dictionary
   builds are switched off at start with an ops event. No row is decoded against the wrong layout.
4. The catalog, object and dictionary queries are unchanged: from a PDB the `CDB_` views and
   `V$CONTAINERS` return that PDB's rows with its container id. Snapshots and LOB reselects switch
   container only when the session is not already in the table's PDB.
5. DOC-4 accepts a local user inside a PDB (no common-user or CONTAINER_DATA check) and blocks
   `cdc.mining.mode=logs` there.

## Consequences

- One connector mines one PDB in range mode; several PDBs from one connector still need CDB$ROOT.
- A lag case that logs mode would replay stops a range-mode task instead (CDC-6001, then a
  resnapshot of the affected tables). Keeping the connector's lag short avoids it.
- Autonomous Database's one-LogMiner-session limit and two-day archive retention are not yet
  handled specially; its qualification is blocked on an OCI account.

## Evidence

`PdbLocalMiningRefEngineIT` (reference document above), `PdbLocalCaptureEngineIT` (a local user
in FREEPDB1 captures DML and the rows after a DDL in range mode; rows written before an unmined DDL
stop the task with CDC-6001 and nothing is delivered), `JdbcLogMinerSessionRangeModeTest` (no
`ADD_LOGFILE`; a redo-dictionary start is CDC-6001), `RulesTest`
(`aLocalUserInsideAPdbNeedsRangeModeNotACommonUser`), `CoreConfigTest`.

## PRD edits

PRD-00 CORE-MINE-1 (range form) and the configuration table (`cdc.mining.mode`). PRD-05 DOC-4.

## Amendments

None.
