---
title: Schema and DDL
description: The schema topic, DDL classification and recovery when redo predates a schema change.
---

# Schema and DDL

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

Table schemas are versioned by SCN and stored in a compacted topic. DDL in redo is classified; supported forms update the schema, unsupported forms stop the task. Redo written before a schema change decodes with a dictionary stored in the redo logs by DBMS_LOGMNR_D.BUILD.
