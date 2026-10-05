---
title: Runbooks
description: One runbook per error code; the connector's error message links here.
---

# Runbooks

When a capture task stops, its error message in the Kafka Connect task status starts with a code
such as `[CDC-2002]` and ends with a link to the runbook for that code, at
`https://kafkacdcconnector.com/runbooks/` followed by the runbook name. The `stop` event on the
[ops topic](../../reference/ops-topic.md) carries the same code and link. The
[error classes](../../reference/error-classes.md) page lists every code; a build check makes sure
each one has a runbook here.

Each runbook says:

- **What the connector observed**: the condition, and how to recognise it in the message.
- **Why it stopped rather than continued**: the data that continuing would have lost or corrupted.
- **Confirm the cause**: queries and commands that show the condition in the database or the
  worker.
- **Recover**: the steps, in order of preference.

## Commands the runbooks use

The runbooks use the Kafka Connect REST API, with the worker's address and the connector's name in
two shell variables:

```bash
CONNECT=http://localhost:8083
NAME=orders-cdc
curl -s "$CONNECT/connectors/$NAME/status"                                            # task state and error
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"  # restart a failed task
```

A restarted task resumes from its last acknowledged position. Kafka Connect does not restart a
failed task by itself; Strimzi users can enable automatic restarts on the `KafkaConnector` resource.

Some recoveries move the connector's offset. The procedure, and what each kind of change means for
the data, is in [offsets and recovery](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand).
Where a runbook says to reload a table, it means a [`snapshot` signal](../signals.md), which
republishes the table's current rows while streaming continues.

The runbooks use Kafka Connect's REST API for offsets. [`oracle-cdc-admin`](../admin.md) wraps
those steps, refuses an SCN whose redo is purged, and starts a resnapshot of chosen tables.
