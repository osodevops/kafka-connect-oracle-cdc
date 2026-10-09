---
title: "Mining"
description: "Mining properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 4
---

# Mining properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.mining.target.latency.ms` | long | `2000` | medium | Latency goal for the adaptive mining window. The only tuning knob: window size follows from it. |
| `cdc.mining.max.logs.per.step` | int | `8` | low | Upper bound on redo logs mined in one step while catching up. |
| `cdc.mining.fetch.size` | int | `10000` | low | JDBC fetch size for V$LOGMNR_CONTENTS. |
| `cdc.mining.query.timeout.ms` | long | `600000` | low | Per-step timeout. On expiry the step is discarded, the session restarted and the window halved; it never fails the task by itself. A socket read on any database connection times out one minute after this, so a connection the network drops silently is reopened rather than waited on. |
| `cdc.mining.session.max.age.ms` | long | `3600000` | low | The LogMiner session is restarted at this age to release PGA. |
| `cdc.mining.decode.threads` | int | `0` | low | Decode threads; 0 means the number of cores minus one, at most 8. |
| `cdc.mining.inlist.max` | int | `1000` | low | Most captured object ids in one IN list of the mining query; more ids are split across several IN lists. Oracle allows at most 1000 entries in one list, and the task fails at start with a larger value. |
| `cdc.mining.catchup.threshold.ms` | long | `300000` | low | Reserved for the lag that would enable parallel catch-up mining, which is not built yet. |
| `cdc.mining.catchup.parallelism` | int | `2` | low | Reserved for the number of catch-up sessions over adjacent SCN windows. Parallel catch-up mining is not built yet. |
| `cdc.rac.safety.lag.ms` | long | `-1` | low | Reserved for the hold-back from the cluster SCN on RAC so late-archiving threads are not missed. RAC is not supported yet. |
| `cdc.dictionary.build.interval.ms` | long | `86400000` | low | Interval between data dictionary builds into the redo (DBMS_LOGMNR_D.BUILD), which let the connector decode rows written before a later DDL on their table. Needs EXECUTE ON DBMS_LOGMNR_D; without it builds are switched off with an ops event. One build also runs at start when the archived logs hold none. 0 switches builds off. |
| `cdc.dictionary.build.time` | string | `02:00` | low | Time of day, in the database's time, of the first scheduled dictionary build; later builds follow every cdc.dictionary.build.interval.ms. |
| `cdc.dictionary.database.url` | string | none | low | JDBC URL of the database dictionary builds run on, with the connector's user and password. Set it to the primary when the connector captures from a physical standby, which is read-only and cannot write a build; the build reaches the standby through the redo. Empty: the captured database. |
