---
title: "CDC-5002 Missing privilege"
description: "Runbook for CDC-5002 PRIVILEGE: the database refused the connector user a grant, a view or the login itself."
slug: /runbooks/privilege
---

# CDC-5002 Missing privilege

**Code:** `CDC-5002` (`PRIVILEGE`). Not retried: the task stops at once.

## What the connector observed

A database call failed with one of:

| Error | Usual meaning |
|---|---|
| ORA-01031 | Insufficient privileges for a package or an operation |
| ORA-00942 | Table or view does not exist; for a `V$` view or a captured table, a missing grant |
| ORA-01017 | Invalid user name or password |
| ORA-28000 | The account is locked |
| ORA-01045 | The user lacks `CREATE SESSION` |

The message names the operation that failed and the ORA code. It can happen at start, or later
when a grant is revoked, the password is changed or the account is locked.

A missing `EXECUTE ON DBMS_LOGMNR_D` does not stop the task: dictionary builds are switched off
with a `dictionary-build` event on the ops topic instead.

## Why it stopped rather than continued

Retrying cannot create a grant, and repeated failed logins can lock the account. Carrying on without
a view the connector relies on, such as the log inventory or the transaction table, would remove a
check that protects against data loss.

## Confirm the cause

Run the doctor with the connector's configuration; rule DOC-4 checks the grant profile, the views
the engine reads, the common user and `CONTAINER_DATA` in a container database:

```bash
java -jar oracle-cdc-doctor-cli.jar check --config connector.json
```

Or check directly, as a DBA:

```sql
SELECT username, account_status, expiry_date, common FROM dba_users WHERE username = 'C##CDC';
SELECT privilege FROM dba_sys_privs WHERE grantee = 'C##CDC';
SELECT owner, table_name, privilege FROM dba_tab_privs WHERE grantee = 'C##CDC';
SELECT * FROM cdb_container_data WHERE username = 'C##CDC';
```

## Recover

1. Grant what is missing. `oracle-cdc-doctor setup-sql` prints the complete script (see
   [database setup](../../database-setup/index.md)); run only the statements that are missing.
2. For a locked or expired account, unlock it or set a new password
   (`ALTER USER c##cdc ACCOUNT UNLOCK;`), then update `cdc.database.password` through your Connect
   config provider.
3. Restart the task. It resumes from its last acknowledged position:

   ```bash
   curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
   ```
