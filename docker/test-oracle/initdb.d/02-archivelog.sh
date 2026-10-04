#!/usr/bin/env bash
# Switch the database to ARCHIVELOG mode (requires a shutdown and a mounted restart) and enable
# minimal supplemental logging. Idempotent: skips the restart when already in ARCHIVELOG.
set -Eeuo pipefail

mode=$(sqlplus -s / as sysdba <<'SQL'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
SELECT log_mode FROM v$database;
EXIT
SQL
)
mode=$(echo "$mode" | tr -d '[:space:]')
echo "oracle-cdc-test-db: current log mode is ${mode}"

if [ "$mode" != "ARCHIVELOG" ]; then
  sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
EXIT
SQL
  echo "oracle-cdc-test-db: ARCHIVELOG enabled"
fi

sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
-- Minimal supplemental logging at database level only. Tests add ALL COLUMNS per table so the
-- partial-before-image path stays testable (docs/research/logminer_reference.md section 6).
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER PLUGGABLE DATABASE ALL SAVE STATE;
EXIT
SQL
echo "oracle-cdc-test-db: minimal supplemental logging enabled"
