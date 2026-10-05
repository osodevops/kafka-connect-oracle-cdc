---
title: Exactly-once delivery
description: Kafka transactions aligned to Oracle transaction boundaries with record, byte and time bounds.
---

# Exactly-once delivery

With exactly-once source support enabled on the Connect worker
(`exactly.once.source.support=enabled`, distributed mode) and the connector registered with
`exactly.once.support=required` and `transaction.boundary=connector`, the connector decides where
each Kafka transaction ends. A consumer reading with `isolation.level=read_committed` then sees every
change once, and never part of an Oracle transaction, across task restarts and worker failures.

## Where a Kafka transaction ends

A Kafka transaction ends only after the last record of an Oracle transaction, or after a heartbeat,
ops or DLQ record written between Oracle transactions. Several small Oracle transactions share one
Kafka transaction until one of these is reached, at the next Oracle commit:

| Bound | Default |
|---|---|
| `cdc.eos.batch.max.records` | 5,000 records |
| `cdc.eos.batch.max.bytes` | 16 MiB of change data |
| `cdc.eos.batch.max.ms` | 500 ms since the Kafka transaction began |

The connector also ends the Kafka transaction as soon as no more records are waiting, so a quiet
database does not hold records back, and an idle heartbeat commits a small transaction on its own.
The offset committed with each Kafka transaction is the position after its last Oracle transaction.

## Very large Oracle transactions

An Oracle transaction with more than `cdc.eos.split.max.records` changes (default 500,000), or more
than `cdc.eos.split.max.bytes` of change data (default 256 MiB), is split into consecutive Kafka
transactions, so none outlives the broker's `transaction.max.timeout.ms`. Each part ends at an exact
event index, so a restart resumes inside the Oracle transaction without duplicates. Records of a split
transaction carry the header `cdc.split=true`, and a `transaction-split` event on the ops topic names
the transaction and the number of Kafka transactions it took. A `read_committed` consumer can see the
first parts of a split transaction before the last.

## At-least-once mode

Without `transaction.boundary=connector` the connector delivers at least once: after a restart, at
most the Oracle transaction that was in flight is delivered again, and the `cdc.xid` and
`cdc.event_index` headers identify the duplicates.
