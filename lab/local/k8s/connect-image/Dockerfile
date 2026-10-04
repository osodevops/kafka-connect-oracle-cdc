# Strimzi Kafka Connect image with the OSO CDC Connector plugin. Built locally for linux/arm64 and
# loaded into minikube, so no registry or public artefact URL is needed (spec.image, not spec.build).
ARG STRIMZI_VERSION
ARG KAFKA_VERSION
FROM quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}
USER root:root
COPY plugin/ /opt/kafka/plugins/oracle-cdc/
RUN chown -R 1001:0 /opt/kafka/plugins/oracle-cdc
USER 1001
