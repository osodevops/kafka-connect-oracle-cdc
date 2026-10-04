---
title: "Errors and retries"
description: "Errors and retries properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 8
---

# Errors and retries properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.on.decode.error` | string | `fail` | medium | fail stops on an undecodable DML row; dlq routes the raw row to the DLQ with an ops event. Corruption and MISSING_SCN always stop. |
| `cdc.retry.max.time.ms` | long | `86400000` | low | Total time to keep retrying transient database errors before the task fails. |
| `cdc.retry.extra.error.codes` | list | empty | low | Extra ORA codes to treat as transient, for example ORA-12345,ORA-54321. |
| `cdc.log.sensitive.data` | boolean | `false` | low | Allow row values in log output. Off by default. |
