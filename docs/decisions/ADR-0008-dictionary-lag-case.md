# ADR-0008: Dictionary lag case

**Status:** Accepted (spike P0-09, 4 October 2026)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

DML mined after an ALTER TABLE on the same table decodes with generic `COL n` names and STATUS 2 under the online catalog. Replay with a dictionary stored in the redo logs (`DBMS_LOGMNR_D.BUILD` with `STORE_IN_REDO_LOGS`) plus `DDL_DICT_TRACKING` decodes the pre-DDL rows with real names on 23ai (`reference/dictionary-replay.md`), so PRD-03 section 3 steps 4 and 5 stand. Until P1-17 implements the replay, the engine stops with `DecodeException` on such a row rather than guess.
