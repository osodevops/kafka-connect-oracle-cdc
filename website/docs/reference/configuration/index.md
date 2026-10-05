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

- Database: connection, credentials, PDB list, wallet and TLS
- Capture: online or archive-only mining, archive destination
- Mining: step sizing, timeouts, fetch size, session age
- Transaction buffer, transaction journal and transactions: memory budget, spill, journal thresholds, long and orphaned transactions
- LOBs: how CLOB, NCLOB and BLOB values are published, the size limit and the placeholder for values the redo does not carry
- Errors and retries: decode error policy, retry budget, extra transient codes
- Topics: prefix and template, table and user selection, key policy, tombstones
- Record format: output format, decimal and temporal modes
- Task: poll batch, linger and shutdown timeout
