---
title: Configuration reference
description: Every cdc.* property, generated from the connector's ConfigDef.
sidebar_position: 1
---

# Configuration reference

The pages in this section are generated from the connector's `ConfigDef` by
`ConfigDocsGeneratorTest` in the `e2e-tests` module and checked on every build, so they cannot
drift from the code. Every property starts with `cdc.`; passwords are Connect `PASSWORD` types
and never appear in logs.

Groups:

<!-- BEGIN GENERATED: configuration groups (ConfigDocsGeneratorTest; do not edit by hand) -->
- [Database](database.md)
- [Capture](capture.md)
- [Mining](mining.md)
- [Transaction buffer](transaction-buffer.md)
- [Transaction journal](transaction-journal.md)
- [Transactions](transactions.md)
- [LOBs](lobs.md)
- [Errors and retries](errors-and-retries.md)
- [Snapshots](snapshots.md)
- [Topics](topics.md)
- [Record format](record-format.md)
- [Task](task.md)
- [Exactly-once](exactly-once.md)
<!-- END GENERATED: configuration groups -->
