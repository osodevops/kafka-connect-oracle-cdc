# syntax=docker/dockerfile:1
# Container image for oracle-cdc-doctor, the preflight checker and operations CLI of the OSO CDC
# Connector for Oracle Database. release.yml builds it from the shaded CLI jar of the release build
# and pushes it to ghcr.io/osodevops/oracle-cdc-doctor with an SBOM and provenance attestations.
#
# Build context: the oracle-cdc-doctor module directory, after `mvn package` has written
# target/oracle-cdc-doctor-<version>-cli.jar.
#
#   docker build -f src/main/docker/Dockerfile --build-arg VERSION=<version> -t oracle-cdc-doctor .
#   docker run --rm -v "$PWD:/work" oracle-cdc-doctor check --config connector.json
#
# The image carries no Oracle Database software; the Oracle JDBC driver inside the jar is
# redistributed under its Free Use Terms and Conditions.
# Dependabot (docker ecosystem) keeps this base current.
FROM eclipse-temurin:25-jre

ARG VERSION
ARG REVISION=unknown
LABEL org.opencontainers.image.title="oracle-cdc-doctor" \
      org.opencontainers.image.description="Preflight checker and operations CLI for the OSO CDC Connector for Oracle Database" \
      org.opencontainers.image.vendor="OSO" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.url="https://kafkacdcconnector.com" \
      org.opencontainers.image.source="https://github.com/osodevops/kafka-connect-oracle-cdc" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

RUN groupadd --system --gid 10001 doctor \
 && useradd --system --uid 10001 --gid doctor --home-dir /work --no-create-home --shell /usr/sbin/nologin doctor \
 && mkdir -p /opt/oracle-cdc-doctor /work \
 && chown doctor:doctor /work

COPY --chmod=0444 target/oracle-cdc-doctor-${VERSION}-cli.jar /opt/oracle-cdc-doctor/oracle-cdc-doctor-cli.jar

USER 10001:10001
WORKDIR /work
ENTRYPOINT ["java", "-XX:+UseSerialGC", "-XX:MaxRAMPercentage=75", "-jar", "/opt/oracle-cdc-doctor/oracle-cdc-doctor-cli.jar"]
CMD ["--help"]
