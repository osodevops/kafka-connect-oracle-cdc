# Local lab stack. Run from anywhere: make -C lab/local/compose <target>
REPO_ROOT := $(abspath $(CURDIR)/../../..)
PLUGIN_DIR := $(shell ls -d $(REPO_ROOT)/kafka-connect-oracle-cdc/target/*-kafka-connect-plugin/osodevops-kafka-connect-oracle-cdc-* 2>/dev/null | head -1)
CONFIG ?= examples/connector-freepdb1.json
NAME ?= oracle-cdc
PROFILES ?=

.PHONY: build plugin up down ps logs register status topics consume delete oracle-sql

build: ## package the connector without tests
	cd $(REPO_ROOT) && mvn -q package -DskipTests -DskipE2E --batch-mode --no-transfer-progress

plugin: ## copy the exploded plugin directory into plugins/ (bind-mounted into Connect)
	@test -n "$(PLUGIN_DIR)" || { echo "plugin dir missing; run make build"; exit 1; }
	rm -rf plugins/kafka-connect-oracle-cdc && mkdir -p plugins && cp -R "$(PLUGIN_DIR)" plugins/kafka-connect-oracle-cdc
	@ls plugins/kafka-connect-oracle-cdc/lib | head -3

JMX_AGENT_VERSION ?= 1.0.1
JMX_AGENT = observability/jmx_prometheus_javaagent.jar

$(JMX_AGENT): ## the Prometheus JMX exporter agent (Apache-2.0) the Connect worker loads
	curl -fsSL -o $@ https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$(JMX_AGENT_VERSION)/jmx_prometheus_javaagent-$(JMX_AGENT_VERSION).jar

up: build plugin $(JMX_AGENT) ## start Oracle, Kafka and Connect (add PROFILES="--profile observability")
	docker compose $(PROFILES) up -d --build --wait
	@echo "Connect: http://localhost:8083   Kafka: localhost:9092   Oracle: localhost:1521/FREE (c##cdc/cdc)"

down:
	docker compose --profile observability --profile chaos --profile debezium down -v

ps:
	docker compose ps

logs:
	docker compose logs -f connect

register: ## register CONFIG (default examples/connector-freepdb1.json)
	curl -sf -X POST -H 'Content-Type: application/json' --data @$(CONFIG) http://localhost:8083/connectors | python3 -m json.tool

status:
	curl -sf http://localhost:8083/connectors/$(NAME)/status | python3 -m json.tool

delete:
	curl -sf -X DELETE http://localhost:8083/connectors/$(NAME)

topics:
	kcat -L -b localhost:9092 | grep -E '^  topic' | sort

consume: ## TOPIC=cdc.FREEPDB1.WORKLOAD.ORDERS make consume
	kcat -C -b localhost:9092 -t $(TOPIC) -o beginning -e -J

oracle-sql: ## open sqlplus as the capture user at CDB$ROOT
	docker exec -it oracle-cdc-lab-oracle sqlplus 'c##cdc/cdc@//localhost:1521/FREE'

check: ## run the correctness oracle against the compose stack (after a bench workload)
	java -jar $(REPO_ROOT)/bench/target/bench-*-cli.jar check --bootstrap-servers localhost:9092 \
	  --url jdbc:oracle:thin:@//localhost:1521/FREEPDB1 --user workload --password workload \
	  --topic-prefix cdc --pdb FREEPDB1 --tables WL_T1,WL_T2,WL_T3 --out /tmp/oracle-cdc-evidence.json
