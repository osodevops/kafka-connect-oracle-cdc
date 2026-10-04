---
title: "Capture"
description: "Capture properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 3
---

# Capture properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.capture.mode` | string | `online` | medium | online mines online and archived logs; archive_only never adds online logs (required for standby capture). |
| `cdc.archive.destination` | string | none | low | Archive destination name (for example LOG_ARCHIVE_DEST_1). Default: the lowest valid local destination. |
