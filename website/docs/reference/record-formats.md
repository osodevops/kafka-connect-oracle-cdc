---
title: Record formats
description: The Debezium-compatible envelope shipped first and the Confluent-compatible format that follows.
---

# Record formats

**Status:** the Debezium-compatible envelope (`op` of `c`, `u`, `d` and `r`, `before`, `after`,
`source`, `ts_ms`) is the Phase 1a format. The Confluent Oracle CDC Source compatible format,
selected with `cdc.output.format=confluent`, is Phase 2.

Every record carries headers with the Oracle transaction id, commit SCN, container name and
event index, so consumers can detect duplicates after an at-least-once restart.
