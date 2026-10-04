---
title: How capture works
description: LogMiner mining of uncommitted redo, step scheduling and the no-silent-loss rule.
---

# How capture works

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

The engine mines redo with LogMiner in steps sized by log count, applying each step atomically; a step that fails midway is discarded and re-mined. Captured tables are pushed down to LogMiner by object id, with the step cut at any DDL that creates a new segment. Every unexpected condition stops the task with a typed error.
