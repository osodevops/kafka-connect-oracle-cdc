---
title: "Task"
description: "Task properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 13
---

# Task properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.poll.max.records` | int | `2000` | low | Most records one poll() returns. |
| `cdc.poll.linger.ms` | long | `50` | low | How long poll() waits for the first record before returning nothing. |
| `cdc.shutdown.timeout.ms` | long | `30000` | low | How long stop() waits for the engine thread before interrupting it. |
