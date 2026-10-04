---
title: Snapshots
description: SCN-anchored chunked snapshots that resume after a restart and interleave with streaming.
---

# Snapshots

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

Initial and signal-driven snapshots read chunks AS OF a single SCN, planned by primary key (index-organised tables always by key, keyless heap tables by ROWID range), with a frontier recorded in the position so a restart continues where it stopped.
