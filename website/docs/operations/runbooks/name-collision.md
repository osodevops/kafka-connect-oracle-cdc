---
title: "CDC-6004 Name collision"
description: "Runbook for CDC-6004 NAME_COLLISION: two columns adjust to one field name, or two tables route to one topic only because characters were replaced."
slug: /runbooks/name-collision
---

# CDC-6004 Name collision

**Code:** `CDC-6004` (`NAME_COLLISION`). Raised at task start, when a table joins the captured set,
or when a table's schema is first built. Not retried.

## What the connector observed

Two different source names became the same target name only because characters were replaced.
There are two forms, and the message names both source names and the shared target:

- **Two columns, one field.** With `cdc.field.name.adjustment.mode=avro`, every character that is
  not allowed in an Avro name becomes an underscore, so columns such as `AMOUNT#` and `AMOUNT$`
  (or `AMOUNT$` and an existing `AMOUNT_`) would both become field `AMOUNT_`. The message reads
  "Columns ... and ... of table ... both become field ...". Columns left out by
  `cdc.columns.exclude`, and LOB columns under `cdc.lob.mode=skip`, have no field and cannot
  collide.
- **Two tables, one topic.** Characters Kafka does not allow in a topic name become underscores,
  so tables `APP.ORDER#` and `APP.ORDER$` would both go to `<prefix>.<pdb>.APP.ORDER_` with the
  default template. The message reads "Tables ... and ... both route to topic ..." and quotes what
  `cdc.topic.template` made of each name before the replacement.

A template that sends several tables to one topic on purpose, because it leaves out `${table}`,
`${schema}` or `${pdb}`, is not a collision: the tables' expanded templates are identical, and the
connector routes them as configured.

## Why it stopped rather than continued

Two columns that share a field would overwrite each other's values in every record, so one of them
would be lost without trace. Two tables that share a topic only by accident mix their rows on that
topic; on a compacted topic a row of one table and a row of the other with equal keys overwrite each
other, so consumers lose rows and nobody chose that. The task stops before it builds the first
record of the second table or of the colliding layout, so nothing has been published wrongly.

## Confirm the cause

Find the two names the message quotes:

```sql
SELECT owner, table_name FROM dba_tables
WHERE owner = :owner AND REGEXP_REPLACE(table_name, '[^A-Za-z0-9._-]', '_') = :sanitised_name;
SELECT column_name FROM dba_tab_columns
WHERE owner = :owner AND table_name = :table
AND REGEXP_REPLACE(column_name, '[^A-Za-z0-9_]', '_') = :field_name;
```

Check the connector's `cdc.topic.template`, `cdc.field.name.adjustment.mode` and
`cdc.tables.include` in its configuration (`curl -s "$CONNECT/connectors/$NAME/config"`).

## Recover

For two columns that adjust to one field:

- Set `cdc.field.name.adjustment.mode=avro_unicode`. It writes each replaced character, and the
  underscore itself, as `_u` and four hexadecimal digits, so different column names never meet.
  Field names change for every column that contains an underscore or a replaced character, so
  consumers that read fields by name must change as well.
- Or leave one of the columns out of the records with `cdc.columns.exclude`, or rename one of them
  in the database.

For two tables that route to one topic:

- Set `cdc.topic.template` so the two tables stay apart, or capture only one of them with
  `cdc.tables.exclude`.
- Or rename one of the tables in the database.

Changing the template or the adjustment mode changes topic names, record schema names or field
names for consumers and for Schema Registry subjects; plan that change as you would any change of
record format. Records already published are not affected. Then restart the task:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```

The task resumes from its last acknowledged position. If it stopped while a table joined the
captured set, reload that table afterwards with a [`snapshot` signal](../signals.md) if it held rows
when it joined.
