---
title: "CDC-7003 Resume floor violation"
description: "Runbook for CDC-7003 RESUME_FLOOR_VIOLATION: LogMiner returned a row or a COMMIT that a restart from an offset already handed to Kafka Connect would skip."
slug: /runbooks/resume-floor-violation
---

# CDC-7003 Resume floor violation

**Code:** `CDC-7003` (`RESUME_FLOOR_VIOLATION`). Not retried: the task stops at once.

## What the connector observed

Every record the connector hands to Kafka Connect carries an offset. For each redo thread, the
offset names the redo record that thread is read again from after a restart (its resume address)
and the last commit on that thread that was delivered. A restart reads each thread again from its
resume address and skips the thread's commits up to its last delivered commit.

LogMiner then returned, on one redo thread, something that a restart from an offset already handed
over would not deliver:

- a row whose redo address lies before that thread's resume address in such an offset, so a
  restart would never read it again, or
- a COMMIT whose redo address is at or before that thread's last delivered commit in such an
  offset, so a restart would skip it as delivered.

The message names the thread, the record (SCN, RS_ID and SSN), the transaction where there is one,
and the address it lies below. Nothing of that transaction was published.

Records are compared by redo address within one thread, never by SCN. A commit with an SCN lower
than changes already delivered is normal: a transaction's redo can be written to the log after
later changes (a private redo strand bound late, or a RAC instance whose log writer is behind), and
its redo address then follows them. That is mined and delivered without this error.

## Why it stopped rather than continued

Within a thread, LogMiner returns redo in the order it was written, and each step of the connector
starts after the last record it applied on each thread. A record below what an offset already
covers can therefore only come from redo returned out of order or from an offset computed ahead of
what was applied. Publishing it would look correct until the next restart from that offset, which
would skip it without a trace. Stopping keeps the loss from happening.

## Confirm the cause

1. Read the task's error from the Kafka Connect status and note the thread, the two redo
   addresses and the SCN.
2. Check the connector's log before the stop for reconnects (`reconnected` events on the
   [ops topic](../../reference/ops-topic.md)) and redo thread changes (`thread-state-changed`), which
   show what the database was doing at that moment.
3. Restart the task (see below). If it mines past the same place, the redo was returned out of
   order once; keep the log and the message anyway.

## Recover

1. Restart the failed task. It resumes from its last acknowledged offset and mines the range again;
   nothing acknowledged is skipped and nothing is lost.
2. If the task stops again with CDC-7003 at the same records, do not move the offset past them.
   Report the issue on GitHub with the message, the task log around the stop and the Oracle
   release update, and keep the archived logs of that SCN range until it is resolved.
