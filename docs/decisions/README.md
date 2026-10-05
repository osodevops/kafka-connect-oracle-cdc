# Architecture decision records

Design changes to the PRDs are recorded here, one file per decision, named
`ADR-nnnn-<slug>.md`, with the minimal corresponding edit made in the PRD. Spike-driven decisions
link the evidence file under `oracle-cdc-core/src/main/resources/reference/`.

| ADR | Title | Status |
|---|---|---|
| 0001 | Object-ID filter pushdown and the DDL step cut | Accepted |
| 0002 | Multi-PDB routing columns under the online catalog | Accepted |
| 0003 | Journal atomicity with offsets | Accepted |
| 0004 | Index-organized tables and ROWID chunking | Accepted |
| 0005 | Mining step atomicity and window sizing | Accepted |
| 0006 | Orphan release safety | Accepted |
| 0007 | Exactly-once batching bounds | Accepted |
| 0008 | Dictionary lag case | Accepted |
| 0009 | Semantic event model for FakeLogMiner | Accepted |
| 0010 | Position invariants first | Accepted |
| 0011 | Regression test naming | Accepted |
| 0012 | Correctness oracle protocol | Accepted |
| 0013 | Metrics transport | Accepted |
| 0014 | Mining cursor is a redo byte address | Accepted |
| 0015 | LOBs are assembled per statement, and undo targets the newest change | Accepted |
| 0016 | The lag case is replayed per step, and rows keep the version they were decoded with | Accepted |

Template: Context, Decision, Consequences, Evidence, PRD edits.

Each row links to `ADR-nnnn-<slug>.md` in this directory; amendments are appended to the ADR rather than rewriting it.
