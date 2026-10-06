-- A third pluggable database, FREEPDB3, for the multi-PDB acceptance criterion (one connector,
-- three PDBs, one LogMiner session, per-PDB topics). FREEPDB2 comes from ORACLE_DATABASE in the
-- Dockerfile; this one is cloned from the seed with a USERS tablespace like the others, opened, and
-- its open state saved so it opens again after a restart (DOC-21).
CREATE PLUGGABLE DATABASE FREEPDB3 ADMIN USER pdbadmin IDENTIFIED BY "pdbadmin"
  DEFAULT TABLESPACE users DATAFILE '/opt/oracle/oradata/FREE/FREEPDB3/users01.dbf'
  SIZE 20M AUTOEXTEND ON NEXT 10M
  FILE_NAME_CONVERT = ('/opt/oracle/oradata/FREE/pdbseed/', '/opt/oracle/oradata/FREE/FREEPDB3/');
ALTER PLUGGABLE DATABASE FREEPDB3 OPEN;
ALTER PLUGGABLE DATABASE FREEPDB3 SAVE STATE;
