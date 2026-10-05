---
title: "LOBs"
description: "LOBs properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 8
---

# LOBs properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.lob.mode` | string | `skip` | medium | skip leaves CLOB, NCLOB, BLOB and XMLTYPE columns out of the records; inline assembles values from redo and publishes cdc.unavailable.placeholder where a value is not in the redo; reselect does the same and then queries the values that are still unavailable AS OF the commit SCN, one query per row. |
| `cdc.lob.max.bytes` | long | `1048576` | medium | Largest LOB value published, in bytes (UTF-8 for CLOB and NCLOB). Larger values follow cdc.lob.oversize.action; assembly memory per value is bounded by this limit. |
| `cdc.lob.oversize.action` | string | `fail` | medium | fail stops the task with CDC-3003 when a committed transaction wrote a LOB value above cdc.lob.max.bytes; placeholder publishes cdc.unavailable.placeholder instead. |
| `cdc.unavailable.placeholder` | string | `__cdc_unavailable_value` | low | Value published for a LOB column whose value is not available (a before image, an unchanged LOB in an update, a partial write or an oversize value); BLOB columns carry its UTF-8 bytes. |
