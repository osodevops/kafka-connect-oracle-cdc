---
title: "Capture"
description: "Capture properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 3
---

# Capture properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.capture.mode` | string | `online` | medium | online mines online and archived logs; archive_only mines archived logs only and never adds online logs. |
| `cdc.start.scn` | long | none | low | SCN to start streaming from when the connector has no stored offset, for example when it takes over from another connector; ignored once an offset exists. Every archived log from it onwards must still exist, or the task stops with CDC-2002. Empty starts at the current SCN. |
| `cdc.archive.destination` | string | none | low | Archive destination name (for example LOG_ARCHIVE_DEST_1). Default: the lowest valid local destination. |
