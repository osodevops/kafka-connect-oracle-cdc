-- Archive to an explicit location rather than the fast recovery area, so a full FRA can never
-- hang a test run with ORA-00257. Takes effect on the restart performed by 02-archivelog.sh.
ALTER SYSTEM SET log_archive_dest_1 = 'LOCATION=/opt/oracle/archive' SCOPE=SPFILE;
ALTER SYSTEM SET log_archive_format = 'arch_%t_%s_%r.arc' SCOPE=SPFILE;
