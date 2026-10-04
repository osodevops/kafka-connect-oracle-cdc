---
title: "CDC-4003 Mining stalled"
description: "Runbook for CDC-4003 MINING_STALLED."
slug: /runbooks/mining-stalled
---

# CDC-4003 Mining stalled

**Code:** `CDC-4003` (`MINING_STALLED`). **Status:** placeholder until the engine code that raises this
condition lands; the code and this URL are stable and already referenced by the error message.

## What the connector observed

To be written with the implementation.

## Why it stopped rather than continued

The connector never continues past a condition that could lose or corrupt captured data. Each
runbook explains the specific risk for its code.

## Confirm the cause

SQL and `oracle-cdc-doctor` commands that confirm the condition from the database.

## Recover

Exact recovery steps, including when an offset change or a resnapshot is the right answer.
