---
title: Offsets and recovery
description: The versioned position, what it encodes and how restart replays exactly the unacknowledged suffix.
---

# Offsets and recovery

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

The position is a versioned record (resume SCN, last commit SCN, event index, database identity, container id, released transaction ids). Offsets only encode what Kafka Connect acknowledged. After a restart the engine resumes from the position and skips already-delivered events of the in-flight transaction.
