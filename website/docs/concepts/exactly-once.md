---
title: Exactly-once delivery
description: Kafka transactions aligned to Oracle transaction boundaries with byte and time bounds.
---

# Exactly-once delivery

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

With exactly.once.source.support enabled the connector defines transaction boundaries itself, committing a Kafka transaction at Oracle commit boundaries, batched by records, time and bytes, and splitting very large Oracle transactions with an event on the ops topic. The doctor checks the broker's transaction.max.timeout.ms.
