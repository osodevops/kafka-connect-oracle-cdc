# ADR-0020: Avro-safe names are opt-in, and name collisions stop the task

**Status:** Accepted
**Context:** PRD-01 SRC-TOP-1 and SRC-FMT-1, PRD-03 SCH-4, PRD-04 MIG-3, 6 October 2026.

## Context

PRD-03 SCH-4 derives the Connect schema names from the table (`${prefix}.${schema}.${table}.Value`
and the `.Key` and `.Envelope` siblings), and the row and key field names are the column names.
Oracle allows `$` and `#` in unquoted names and almost anything in quoted names (lower case,
spaces, slashes as SAP uses), and a topic prefix may contain `-`. Avro requires every
dot-separated part of a full name, and every field name, to match `[A-Za-z_][A-Za-z0-9_]*`. Avro's
Java library builds a record with an invalid namespace without complaint but refuses to parse the
schema text it writes, and refuses an invalid field name at once, so a worker with an Avro
converter fails the task, or the registry refuses the schema, on such a table. Debezium users
solve this with `schema.name.adjustment.mode` and `field.name.adjustment.mode`, and the migration
translator had no target for them.

Separately, the topic router replaces characters Kafka does not allow with `_`, so `APP.ORDER#` and
`APP.ORDER$` both routed to `<prefix>.<pdb>.APP.ORDER_`. On a compacted topic equal keys of the two
tables overwrite each other: a silent loss of rows.

## Decision

1. **Two settings, off by default.** `cdc.schema.name.adjustment.mode` and
   `cdc.field.name.adjustment.mode` take `none` (default), `avro` or `avro_unicode`, with
   Debezium's documented meaning. With `none` every name and every record is exactly what it was
   before the settings existed.
2. **The rules.** `avro` replaces each character that is not an ASCII letter, digit or underscore
   with `_`, and puts `_` in front of a name that starts with a digit, keeping the digit
   (Debezium's behaviour since DBZ-6559, 2.3). `avro_unicode` replaces each such character, the
   underscore itself and a leading digit with `_u` and four lower-case hexadecimal digits of the
   UTF-16 code unit (`_` is the escape character), so distinct names never meet. Debezium documents
   both modes but not the leading digit; the leading-digit rule comes from DBZ-6559 and from the
   Avro naming rule that the documented wording ("characters that cannot be used") applies to.
3. **Where they apply.** Schema adjustment applies to each dot-separated part of the per-table
   `.Key`, `.Value` and `.Envelope` names. Debezium adjusts the full name as one string in which
   dots and digits after the first character are allowed, which leaves a later part such as a
   quoted table `"1T"` invalid for Avro's parser; part by part fixes that and gives the same result
   for every other name. Field adjustment applies to the
   key and value fields taken from columns, after `cdc.columns.exclude` (ADR-0018) and the LOB
   mode have removed theirs; a dot in a column name is replaced, because a field name has no
   namespace. The fixed names (`io.debezium.connector.oracle.Source`, `event.block`, the semantic
   types, the envelope, source and transaction fields) are valid already and never change. Topic
   names are not affected.
4. **Values follow columns.** The envelope keeps, with the cached schemas of a table version, a map
   from each field name to its column and reads image values by the column's own name.
5. **Collisions stop the task (CDC-6004, `NAME_COLLISION`).** Two columns of one table that adjust
   to one field, and two tables whose expanded templates differ but sanitise to one topic, raise
   `NameCollisionException` naming both sources and the shared target. Field collisions are found
   when a table version's schemas are first built, before its first record; topic collisions when
   the task starts (over every captured table), when a table joins the captured set, and in any
   case before a table's first record is built. Tables whose expanded templates are identical
   share a topic by design and are never a collision. A table that leaves the captured set
   releases its topic, so a table that later takes the same topic (after a DROP, or a rename from
   one such name to the other) is not held to the old claim.

## Consequences

- Existing deployments see no change; users with Avro converters opt in, as Debezium 2.x users do.
- A migration from Debezium carries the modes over; a Debezium 1.x configuration, which adjusted
  schema names by default and sanitised field names by default with an Avro converter, gets the
  same behaviour pinned by the translator so record names and registered schemas stay compatible.
- Configurations that silently merged two tables through sanitising now stop with CDC-6004 and
  need a template, an exclusion or a rename.
- Two tables whose adjusted schema names coincide but whose topics differ are allowed: subjects
  under `TopicNameStrategy` differ, and nothing is overwritten.

## Evidence

`NameAdjustmentTest` (the rules), `DebeziumEnvelopeNamesTest` (names, values by column for before
and after images, keys, ROWID keys, snapshots, the column filter, both collisions, and `none`
equal to the unset output down to the JSON bytes), `TopicRouterTest`, `NameCollisionTaskTest`, and
`AvroNamesRoundTripTest`: with Apicurio Registry's `AvroData` (Apache-2.0, test scope) every
column type of `TypeRoundTripEngineIT` converts to Avro, through the schema text and Avro binary
encoding, and back to equal schemas and records in both modes, while `none` is refused.

## PRD edits

PRD-03 SCH-4: names adjusted per `cdc.schema.name.adjustment.mode` and
`cdc.field.name.adjustment.mode` (ADR-0020). PRD-01 section 5: the two keys.
