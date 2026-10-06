#!/usr/bin/env bash
# Runs on every start. Fails fast (non-zero exit stops the container) unless the database is in
# the state the test suites assume, so a broken first start is an obvious failure instead of a
# hanging wait strategy.
set -Eeuo pipefail

out=$(sqlplus -s / as sysdba <<'SQL'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 200
SELECT 'LOG_MODE=' || log_mode || ' SUPP_MIN=' || supplemental_log_data_min FROM v$database;
SELECT 'PDB ' || name || '=' || open_mode FROM v$pdbs WHERE name IN ('FREEPDB1', 'FREEPDB2', 'FREEPDB3') ORDER BY name;
SELECT 'USER=' || username FROM dba_users WHERE username = 'C##CDC';
EXIT
SQL
)
echo "oracle-cdc-test-db: $out" | tr '\n' ' '; echo

fail=0
echo "$out" | grep -q 'LOG_MODE=ARCHIVELOG' || { echo "ASSERT FAILED: not in ARCHIVELOG mode" >&2; fail=1; }
echo "$out" | grep -q 'SUPP_MIN=YES' || { echo "ASSERT FAILED: minimal supplemental logging off" >&2; fail=1; }
echo "$out" | grep -q 'PDB FREEPDB1=READ WRITE' || { echo "ASSERT FAILED: FREEPDB1 not READ WRITE" >&2; fail=1; }
echo "$out" | grep -q 'PDB FREEPDB2=READ WRITE' || { echo "ASSERT FAILED: FREEPDB2 not READ WRITE" >&2; fail=1; }
echo "$out" | grep -q 'PDB FREEPDB3=READ WRITE' || { echo "ASSERT FAILED: FREEPDB3 not READ WRITE" >&2; fail=1; }
echo "$out" | grep -q 'USER=C##CDC' || { echo "ASSERT FAILED: capture user C##CDC missing" >&2; fail=1; }
exit $fail
