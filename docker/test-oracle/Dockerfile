# Derived Oracle Database Free test image for the OSO CDC Connector for Oracle Database.
#
# Base image terms: the user who builds this image pulls gvenzl/oracle-free and accepts Oracle's
# terms for Oracle Database Free. This derived image is for development and testing only and is
# never redistributed outside OSO's own registry (docs/07_test_lab_and_budget.md section 5).
#
# What the first start adds (hooks run once as SYS in CDB$ROOT, in alphabetical order):
#   01  archive destination outside the fast recovery area
#   02  ARCHIVELOG mode and minimal supplemental logging
#   03  three small online redo log groups (forces frequent switches in tests)
#   03a a third PDB, FREEPDB3, opened with its state saved
#   04  common capture user C##CDC and the WORKLOAD schema in every PDB
# ORACLE_DATABASE=FREEPDB2 makes the entrypoint create the second PDB before the hooks run.
# startdb.d/00-assert.sh runs on every start and fails fast if any of this is missing.
ARG BASE_TAG=23.26.3-slim-faststart
FROM gvenzl/oracle-free:${BASE_TAG}

ARG BASE_TAG
ENV ORACLE_DATABASE=FREEPDB2

COPY --chown=oracle:oinstall initdb.d/ /container-entrypoint-initdb.d/
COPY --chown=oracle:oinstall startdb.d/ /container-entrypoint-startdb.d/

USER root
RUN chmod 0755 /container-entrypoint-initdb.d/*.sh /container-entrypoint-startdb.d/*.sh \
 && mkdir -p /opt/oracle/archive && chown oracle:oinstall /opt/oracle/archive
USER oracle

LABEL org.opencontainers.image.title="oracle-cdc-test-db" \
      org.opencontainers.image.description="Oracle Database Free with ARCHIVELOG, supplemental logging, FREEPDB2, FREEPDB3 and the C##CDC capture user, for OSO CDC Connector tests" \
      org.opencontainers.image.source="https://github.com/osodevops/kafka-connect-oracle-cdc" \
      sh.oso.oracle-cdc.base-tag="${BASE_TAG}"
