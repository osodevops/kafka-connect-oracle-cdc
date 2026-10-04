---
title: Transaction buffer and journal
description: Why offsets never pin on a long transaction and how large transactions leave the heap.
---

# Transaction buffer and journal

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

Changes are buffered per transaction until COMMIT. The buffer has a memory budget; the largest transactions spill to local disk, and transactions older or larger than a threshold are journaled to a compacted Kafka topic as records emitted from poll(), so they share the Kafka transaction with data and offsets. The resume position can then move past the start of a journaled transaction.
