# Oracle LogMiner: Implementation Reference

**Status:** Research, source-linked. Collected 4 October 2026.
**Audience:** Engineers implementing `oracle-cdc-core`. Everything here is from Oracle's public documentation unless stated.

Primary sources: [Oracle 19c Utilities, Using LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html) and [Oracle AI Database 26ai Utilities, Using LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html).

---

## 1. Licensing and positioning

- LogMiner is included in all database editions ([Oracle Database Licensing Information](https://docs.oracle.com/en/database/oracle/oracle-database/18/dblic/Licensing-Information.html)). XStream needs a GoldenGate licence ([XStream guide](https://docs.oracle.com/database/121/XSTRM/xstrm_intro.htm)).
- The 19c Utilities guide states: "LogMiner is intended for use as a debugging tool, to extract information from the redo logs to solve problems. It is not intended to be used for any third party replication of data in a production environment" ([19c LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)). We could not find that sentence in the 26ai guide during this research. In October 2025 Oracle publicly described LogMiner as a free change-detection API included with database licences and used for CDC by tools such as Debezium ([Oracle blog](https://blogs.oracle.com/dataintegration/binary-log-readers)). The feasibility report treats this as a support-positioning risk, not a licensing blocker.
- Oracle's GoldenGate paper characterises LogMiner CDC as single-threaded, "Low-medium" throughput and "Medium-high" source impact, and lists `continuous_mine` desupport, rollback handling, unsupported types and multitenant limits as things to test in any LogMiner tool ([Oracle GoldenGate Advantages](https://www.oracle.com/a/ocom/docs/techpaper-goldengate-advantages.pdf)). This is a vendor paper selling GoldenGate; we use it as a checklist.

## 2. Privileges

- `EXECUTE_CATALOG_ROLE` and the `LOGMINING` privilege for the LogMiner packages and `V$LOGMNR_CONTENTS`; `SYSDBA` or `LOGMINING` to query contents; `CDB_DBA` for multitenant administration ([19c LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)).
- Our connector also needs `SELECT` on captured tables, `FLASHBACK` on captured tables (snapshots and reselect), and `SELECT` on `V$DATABASE`, `V$LOG`, `V$LOGFILE`, `V$ARCHIVED_LOG`, `V$THREAD`, `GV$TRANSACTION`, `V$TRANSACTION`, `DBA_OBJECTS`, `DBA_TAB_COLS`, `DBA_CONSTRAINTS`, `DBA_CONS_COLUMNS`, `DBA_LOG_GROUPS`, `DBA_LOG_GROUP_COLUMNS`, `DBA_TABLES`, `DBA_EXTENTS`. `oracle-cdc-doctor` generates the exact grant script (PRD-05).

## 3. Session lifecycle

1. `DBMS_LOGMNR.ADD_LOGFILE` for each online or archived log needed (not for per-PDB mining).
2. `DBMS_LOGMNR.START_LOGMNR(STARTSCN, ENDSCN, OPTIONS)`.
3. Query `V$LOGMNR_CONTENTS` (rows in SCN order by default).
4. `DBMS_LOGMNR.END_LOGMNR`.

Options and parameters are not persistent between `START_LOGMNR` calls; it can be called again inside a session to change the range. If no added log matches the range, start fails with ORA-01291 ([19c LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)).

| Option | Behaviour | Our use |
|---|---|---|
| `DICT_FROM_ONLINE_CATALOG` | Uses the current dictionary; fastest; cannot decode SQL for older table versions after DDL | Default |
| `DICT_FROM_REDO_LOGS` | Uses a dictionary written to redo by `DBMS_LOGMNR_D.BUILD`; consistent; costs resources | DDL replay windows only |
| `DDL_DICT_TRACKING` | Applies DDL seen in redo to LogMiner's dictionary; not valid with online catalog | With redo dictionary only |
| `COMMITTED_DATA_ONLY` | Returns committed transactions in commit order; stages each transaction in database memory until commit and can fail with out of memory | Not used. Large transactions would move the memory problem into the source database |
| `SKIP_CORRUPTION` | Returns `CORRUPTED_BLOCKS` rows instead of failing | Not used. Corruption is a stop condition |
| `NO_ROWID_IN_STMT` | Omits ROWID from reconstructed SQL | Used (simplifies parsing) |
| `NO_SQL_DELIMITER` | Omits trailing semicolon | Used |
| `CONTINUOUS_MINE` | Desupported in 19c | Not available |

## 4. `V$LOGMNR_CONTENTS` columns we depend on

Documented in the 19c guide: `OPERATION` (INSERT, UPDATE, DELETE, DDL, also `CORRUPTED_BLOCKS`, `MISSING_SCN`), `SCN`, `COMMIT_SCN`, `XIDUSN`, `XIDSLT`, `XIDSQN`, `SEG_OWNER`, `SEG_NAME`, `USERNAME`, `SQL_REDO`, `SQL_UNDO`, `INFO`, `STATUS` (2 invalid SQL or dictionary mismatch, 3 not guaranteed accurate, 1343 skipped corruption), `OBJECT_ID`, `CON_ID`, `SRC_CON_NAME`, `SRC_CON_ID`, `SRC_CON_DBID`, `SRC_CON_GUID` ([19c LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)).

We also rely on columns documented in the `V$LOGMNR_CONTENTS` reference rather than the Utilities guide: `OPERATION_CODE`, `ROLLBACK`, `RS_ID`, `SSN`, `CSF` (continuation flag for SQL longer than one row), `ROW_ID`, `DATA_OBJ#`, `THREAD#`, `TIMESTAMP`, `START_SCN`, `COMMIT_TIMESTAMP`, `CLIENT_ID`, `SESSION#`, `SERIAL#`. **Engineering task CORE-REF-1:** verify each against the database reference for 19c, 21c, 23ai and 26ai before coding, and record the result in `oracle-cdc-core/src/main/resources/logminer-columns.md`. The Utilities guide we read does not describe these columns.

## 5. Dictionary strategy facts

- Online catalog: requires an open database; reconstructs SQL only for the latest table version; after DDL may return non-executable `SQL_REDO` for older changes.
- Redo dictionary (`DBMS_LOGMNR_D.BUILD` with `STORE_IN_REDO_LOGS`): requires ARCHIVELOG; Oracle recommends running it off-peak; it can span several logs, all of which must be added.
- Flat file dictionary: backward compatibility only, not transactionally consistent; PDB flat-file dumps desupported from 19c.

Source: [19c LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html).

## 6. Supplemental logging

- Off by default; LogMiner is not usable without at least minimal supplemental logging (`ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`).
- Identification key levels: `ALL`, `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, plus procedural. Table level `ALTER TABLE ... ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` affects one table.
- Enabling identification key logging on an open database invalidates DML cursors (database level: all; table level: that table).
- LOB, LONG, ADT and other out-of-line columns are never supplementally logged; 32 KB extended VARCHAR2 is treated as a LOB.
- Long table or column names (over 30 characters) are not supported by `DBMS_LOGMNR` with supplemental logging.
- In 26ai, CDB behaviour depends on undo mode: with local undo, per-PDB supplemental logging works without minimal logging at `CDB$ROOT`; switching to shared undo with per-PDB logging fails with ORA-60526 ([26ai LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html)).

**Our policy:** minimal supplemental logging at database level plus `ALL` columns on captured tables only. This matches Confluent's advice to enable full logging only on tables of interest ([Confluent best practices](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/best-practices.html)) and keeps redo growth limited to captured tables.

## 7. Data types and storage

Supported (with compatibility conditions): BINARY_DOUBLE, BINARY_FLOAT, BLOB, CHAR, CLOB, NCLOB, DATE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, SecureFiles LOBs (no fragment operations; no `SQL_UNDO`), LONG, LONG RAW, NCHAR, NUMBER, NVARCHAR2, VARRAY objects, simple and nested ADTs without collections, RAW, TIMESTAMP variants, VARCHAR2 including 32 KB extended, XMLType (CLOB, object-relational, binary XML; mine only with committed data for completeness). Storage: heap, partitioned, IOT with overflow, clusters, basic and advanced compression, HCC ([19c LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)).

**Whole table ignored** if it has any of: BFILE, nested tables, objects with nested tables, **identity columns**, temporal validity columns, PKREF, PKOID, nested table attributes. The identity column case is the one most likely to surprise customers, and `oracle-cdc-doctor` must flag it before go-live.

Temporary tables produce no SQL redo. Redo logs and dictionary must come from the same database and RESETLOGS SCN.

## 8. Multitenant

- `V$LOGMNR_CONTENTS` is root-restricted in a CDB; `SRC_CON_NAME` and `SRC_CON_ID` identify the PDB "only when mining with a LogMiner dictionary". **Engineering task CORE-REF-2:** confirm these columns are populated with the online catalog on 19c, 21c, 23ai and 26ai (Oracle Free ships a CDB, so this runs in CI).
- From 19c RU10, **per-PDB mining**: connect to a PDB, do not add log files, call `START_LOGMNR` with an SCN taken from `DBA_LOGMNR_DICTIONARY_BUILDLOG`. Oracle says this is required for Autonomous Database ([19c LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)).
- RAC: add archived logs from all redo threads active in the range or results are partial.

## 9. Managed platforms

| Platform | Fact |
|---|---|
| Autonomous Database | Archived logs kept for at most 48 hours; mining older files raises ORA-1285 ([Autonomous LogMiner](https://docs.oracle.com/en/cloud/paas/autonomous-json-database/ajdug/autonomous-logminer.html)) |
| Amazon RDS for Oracle | No shell; `rdsadmin.rdsadmin_util.force_logging`, `alter_supplemental_logging`, `switch_logfile`, `add_logfile` (under 2 GiB), `drop_logfile`; instances start with four 128 MB online logs ([AWS RDS docs](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.Oracle.CommonDBATasks.Log.html)) |
| Amazon RDS CDB | `CDB$ROOT` not accessible; Debezium unsupported ([dbz#1925](https://github.com/debezium/dbz/issues/1925)) |

RDS's default four 128 MB logs will switch far more often than the 2 GB plus logs used by the Debezium users above, so `oracle-cdc-doctor` must report switch rate and recommend resizing.

## 10. Redo profiling query pattern

Pepkor found its noisy tables by mining an archived window and grouping by owner and table ([Debezium blog, Pepkor](https://debezium.io/blog/2026/04/20/oracle-cdc-replication-lag/)). `oracle-cdc-doctor redo-profile` automates this: mine a window with no table filter, group by `SEG_OWNER`, `SEG_NAME`, `OPERATION`, report top N and flag truncate-and-reload patterns (DDL TRUNCATE followed by bulk INSERT in the same window).

## 11. Test database

`gvenzl/oci-oracle-free` publishes Apache-2.0 container build files for Oracle Database Free 23ai and 26ai (`23.26.x`) with `-faststart` variants designed for automated tests, ARM images from 23.5, and an existing `FREEPDB1` PDB ([gvenzl/oci-oracle-free](https://github.com/gvenzl/oci-oracle-free)). The page does not state whether ARCHIVELOG mode is enabled, so our test image enables it explicitly in an init script and the suite asserts it before running.
