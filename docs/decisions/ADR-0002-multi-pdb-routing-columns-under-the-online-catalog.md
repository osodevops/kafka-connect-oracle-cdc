# ADR-0002: Multi-PDB routing columns under the online catalog

**Status:** Accepted (spike P0-06, 4 October 2026; amended 5 October 2026)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

`SRC_CON_ID` and `SRC_CON_NAME` are populated on DML, DDL, START and COMMIT rows for every PDB when mining at CDB$ROOT with `DICT_FROM_ONLINE_CATALOG` on Oracle Database 23ai Free (`reference/pdb-routing.md`). Multi-PDB capture stays in 1.0: one connector mines once at the root and routes by `SRC_CON_NAME`, with `SRC_CON_ID` as the stable identity. The buffer keys transactions by `(SRC_CON_ID, XID)` because undo is local to a PDB and the same XID can be open in two PDBs at once.

## Amendments

**Amendment, 5 October 2026.** The same holds for object ids: the mining filter and the row adapter are keyed by `(SRC_CON_ID, DATA_OBJ#)` (see ADR-0001).
