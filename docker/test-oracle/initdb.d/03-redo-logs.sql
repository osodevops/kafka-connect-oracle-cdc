-- Uniform 50 MB online redo logs so tests can force frequent, predictable log switches. Adds groups 4 to 6 next
-- to the existing members, then cycles every group that is not 50 MB out, drops it and re-adds it at 50 MB
-- so V$LOG has six small groups. Explicit file names: the Free image does not use Oracle-managed
-- files for redo. Safe to re-run (no-op once the originals are 50 MB).
WHENEVER SQLERROR EXIT FAILURE
SET SERVEROUTPUT ON
DECLARE
  l_dir VARCHAR2(512);

  PROCEDURE add_group(p_group IN NUMBER) IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE GROUP ' || p_group
      || ' (''' || l_dir || 'redo' || LPAD(p_group, 2, '0') || '.log'') SIZE 50M REUSE';
  END;
BEGIN
  SELECT SUBSTR(member, 1, INSTR(member, '/', -1)) INTO l_dir
    FROM v$logfile WHERE ROWNUM = 1;
  DBMS_OUTPUT.PUT_LINE('oracle-cdc-test-db: redo directory ' || l_dir);

  FOR g IN 4 .. 6 LOOP
    BEGIN
      add_group(g);
    EXCEPTION WHEN OTHERS THEN
      IF SQLCODE != -1184 THEN RAISE; END IF; -- ORA-01184: group already exists
    END;
  END LOOP;

  FOR rec IN (SELECT group# FROM v$log WHERE bytes <> 50 * 1024 * 1024 ORDER BY group#) LOOP
    -- in ARCHIVELOG mode a group can be dropped only once it is inactive and archived (ORA-00350
    -- otherwise); on a slow host the archiver lags the switch, so wait for it instead of switching
    FOR attempt IN 1 .. 120 LOOP
      DECLARE
        l_status   VARCHAR2(16);
        l_archived VARCHAR2(3);
      BEGIN
        SELECT status, archived INTO l_status, l_archived FROM v$log WHERE group# = rec.group#;
        EXIT WHEN l_status = 'UNUSED' OR (l_status = 'INACTIVE' AND l_archived = 'YES');
        IF l_status IN ('CURRENT', 'ACTIVE') THEN
          EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE';
          EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT';
        ELSE
          DBMS_SESSION.SLEEP(1);
        END IF;
      END;
    END LOOP;
    EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE GROUP ' || rec.group#;
    add_group(rec.group#);
    DBMS_OUTPUT.PUT_LINE('oracle-cdc-test-db: resized redo log group ' || rec.group# || ' to 50M');
  END LOOP;
END;
/
SELECT group#, bytes / 1024 / 1024 AS mb, status FROM v$log ORDER BY group#;
