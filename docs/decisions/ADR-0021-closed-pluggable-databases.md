# ADR-0021: Closed pluggable databases are a transient wait and a doctor finding

**Status:** Accepted
**Context:** PRD-05 doctor rules, PRD-00 CORE-ERR, `DatabaseRestartNightlyIT`, 6 October 2026.

## Context

After `SHUTDOWN ABORT` and `STARTUP`, the connector reconnected in the moments before the
pluggable databases opened, and LogMiner failed with ORA-16331 "container FREEPDB2 is not open".
LogMiner with the online catalog reads every container whose redo lies in the mined range, so a
closed PDB stops mining whether or not the connector captures it. The code was unclassified, so
the task stopped with CDC-1001 although the condition clears as soon as the PDBs open.

## Decision

1. ORA-16331 is a transient database error (`OraErrorClassifier.TRANSIENT`): the engine retries
   with backoff within `cdc.retry.max.time.ms`, and the operator action names the fix if it
   persists (open the PDB and `SAVE STATE`).
2. A new full-mode doctor rule, DOC-21, reads `V$PDBS` and `DBA_PDB_SAVED_STATES`: a PDB that is
   not open read-write or read-only is a warning; an open PDB with no saved state is information,
   because after a restart it stays closed until opened by hand. Neither blocks: a closed PDB with
   no redo in the mined range does no harm.

## Consequences

- A PDB that stays closed while its redo is in range makes the task wait, then stop with CDC-1001
  when the retry budget runs out; nothing is skipped.
- `oracle-cdc-doctor check` now reports 21 rules.

## Evidence

`DatabaseRestartNightlyIT` (nine reconnects through the restart, both workloads correct);
`OraErrorClassifierTest`; `FullRulesTest.closedPluggableDatabasesWarnAndUnsavedStatesInform`.

## PRD edits

PRD-05 section 3: DOC-21 in the rule table.
