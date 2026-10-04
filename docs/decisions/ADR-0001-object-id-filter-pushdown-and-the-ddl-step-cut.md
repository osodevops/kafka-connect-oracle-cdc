# ADR-0001: Object-ID filter pushdown and the DDL step cut

**Status:** Accepted (spike P0-07, 4 October 2026; amended 5 October 2026)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

The mining query always has three OR branches: row changes on captured object ids, DDL rows and transaction control rows (minus excluded users). The row branch pushes `DATA_OBJ#` down to the server; the spike (`reference/object-id-stability.md`) showed `DATA_OBJ#` is the logical OBJECT_ID and survives TRUNCATE, MOVE and partition maintenance of existing partitions, while rows written into a partition created by SPLIT, MERGE, ADD or EXCHANGE carry a new id. Steps are staged and applied atomically; a DDL that creates, exchanges, drops or renames a segment cuts the step after that DDL, the ids are re-resolved and the next step starts at the DDL with the applied prefix skipped (`StepCursor`). CREATE TABLE and DROP TABLE cut for any owner so a new table that matches the include patterns is picked up without a restart (SRC-SEL-4).

## Amendments

**Amendment, 5 October 2026.** OBJECT_IDs are allocated per container. The Strimzi `oracle_restart` case mined a CDB$ROOT AWR table whose id equalled a captured PDB table's id and the decoder stopped on an unknown column. The pushdown and the row naming pair `SRC_CON_ID` with `DATA_OBJ#` (`ObjectKey`); a non-CDB uses container 0 without a container predicate. DDL rows are mined for every non-Oracle owner rather than only captured owners, otherwise a connector started before its tables exist can never see the CREATE TABLE that would fix it.
