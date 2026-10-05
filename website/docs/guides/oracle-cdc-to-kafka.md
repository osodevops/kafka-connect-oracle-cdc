---
title: Oracle Database change data capture to Kafka
description: A practical guide to streaming Oracle Database changes into Apache Kafka with LogMiner, what makes it hard, and how to run it safely.
---

# Oracle Database change data capture to Kafka

Change data capture (CDC) turns every committed insert, update and delete in a database into an
event that other systems can consume. With Oracle Database as the source and Apache Kafka as the
destination, CDC feeds analytics platforms, search indexes, caches, microservices and replicas
without changing the applications that write to the database. This guide explains how CDC from
Oracle Database works, what makes it hard to get right, and how to run it with the OSO CDC
Connector.

## Ways to capture changes from Oracle Database

| Approach | How it works | Trade-offs |
|---|---|---|
| Query polling | Select rows changed since the last poll, using a timestamp or version column | Misses deletes and intermediate states; needs the column on every table; adds query load |
| Triggers | Triggers copy each change to a shadow table that is then read | Adds work to every write transaction; schema changes need trigger changes |
| LogMiner | Reads the changes from the redo log through Oracle's `DBMS_LOGMNR` package | Included in all editions; needs ARCHIVELOG mode and supplemental logging; the connector must handle open transactions, redo retention and schema changes |
| XStream and GoldenGate | Oracle's replication products stream logical change records | Separately licensed ([Oracle XStream guide](https://docs.oracle.com/database/121/XSTRM/xstrm_intro.htm)) |
| Reading redo files directly | Third-party parsers of the binary redo format | Oracle states these are unsupported and may breach its licence terms ([Oracle blog](https://blogs.oracle.com/dataintegration/binary-log-readers)) |

LogMiner is the capture interface Oracle provides with the database at no extra cost, and it sees
every change, including deletes, without touching the applications. The OSO CDC Connector uses
LogMiner only and never reads redo files directly.

## How LogMiner-based CDC works

Every change Oracle makes is first written to the redo log. In ARCHIVELOG mode, filled online redo
logs are copied to archived logs before they are reused. LogMiner reads online and archived logs
and returns their contents as rows of `V$LOGMNR_CONTENTS`: one row per change, with the SQL that
redoes it, the transaction it belongs to, its System Change Number (SCN) and its position in the
redo.

With minimal supplemental logging, the redo also carries what LogMiner needs to rebuild rows; with
table-level ALL COLUMNS logging, an update or delete carries the whole row before the change, not
only the changed columns. See [supplemental logging](../database-setup/supplemental-logging.md).

A CDC connector on LogMiner mines the redo in steps, decodes each row into column values, groups
changes by transaction, and publishes a transaction's changes when its COMMIT arrives. The details
of how this connector does that are in [how capture works](../concepts/how-capture-works.md).

## What makes it hard

**Open transactions.** LogMiner returns changes before their transactions commit. A connector must
hold them until the COMMIT or ROLLBACK, and must be able to restart without losing the ones it held.
The usual answer, restarting from the start of the oldest open transaction, means a single long
transaction holds the restart point back for as long as it is open. This connector spills large
transactions to disk and journals long ones to Kafka, so the restart point moves on
([transaction buffer and journal](../concepts/transaction-buffer-and-journal.md)).

**Redo retention.** The connector needs every redo log from its restart point onwards. If archived
logs are deleted first, the changes in them are gone. A connector that quietly continues from a
later log loses data without anyone noticing; this one stops with a typed error and a
[runbook](../operations/runbooks/log-purged.md). Size retention for the longest outage plus lag
([redo sizing](../database-setup/redo-sizing.md)).

**Schema changes.** LogMiner decodes redo with the current data dictionary. Rows written before a
DDL on their table come back with generic column names when they are mined later. This connector
keeps schema versions per table and mines such ranges again with a dictionary stored in the redo
([schema and DDL](../concepts/schema-and-ddl.md)).

**Initial load.** A new consumer usually needs the rows that already exist, not just new changes,
and the two must line up. This connector reads tables in chunks, each as of one SCN, and publishes
each chunk in the right place among the streamed changes, so a consumer that keeps the latest
record per key converges to the table ([snapshots](../concepts/snapshots.md)).

**Delivery guarantees.** A Kafka Connect source connector delivers at least once by default: after
a crash some records can arrive twice. With Kafka Connect's exactly-once source support, this
connector commits Kafka transactions at Oracle commit boundaries, so a `read_committed` consumer
sees each change once and never part of an Oracle transaction
([exactly-once delivery](../concepts/exactly-once.md)).

**Large values.** CLOB, NCLOB and BLOB changes are written to the redo in pieces, and some values
are not in the redo at all. See [LOB columns](../reference/record-formats.md#lob-columns) for the
choices.

**Quiet databases.** If the captured tables do not change, a connector's committed position does
not move, and the redo it would need on restart grows old. This connector writes heartbeat records
that carry its position, without writing anything to the source database.

## Running it

1. **Prepare the database.** ARCHIVELOG mode, supplemental logging, and a mining user with the
   LogMiner grants. `oracle-cdc-doctor setup-sql` writes the script for your DBA and
   `oracle-cdc-doctor check` confirms the result ([database setup](../database-setup/index.md)).
2. **Try it locally.** The [Docker Compose lab](../getting-started/quick-start.md) runs Oracle
   Database Free, Kafka and Kafka Connect with the connector on a laptop.
3. **Install the plugin** on your Connect workers ([installation](../getting-started/installation.md))
   and register a [first connector](../getting-started/first-connector.md).
4. **Monitor it.** Export the [metrics](../reference/metrics.md) to Prometheus and load the shipped
   [dashboard and alerts](../operations/dashboards-and-alerts.md). Read the
   [ops topic](../reference/ops-topic.md) to see what the connector did and why.
5. **Know the runbooks.** Every condition that stops the connector has a code and a
   [runbook](../operations/runbooks/index.md) that explains the recovery.

## Choosing a connector

If you are choosing between connectors, or moving from one, the [comparison](../comparison/index.md)
sets this connector beside Confluent's Oracle CDC Source and Debezium's Oracle connector, and the
[benchmark harness](../comparison/index.md#measure-it-yourself) lets you measure them on your own
database.
