---
title: Multi-PDB capture
description: One mining session at CDB$ROOT routing changes from several PDBs.
---

# Multi-PDB capture

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

The engine mines at the container root and routes rows by SRC_CON_NAME, keying transactions by (container id, transaction id). The spike on Oracle Database 23ai confirmed both columns are populated for DML, DDL and transaction control rows under the online catalog.
