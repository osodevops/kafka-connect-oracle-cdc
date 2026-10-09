# Local lab stack. Run from anywhere: make -C lab/local/compose <target>
REPO_ROOT := $(abspath $(CURDIR)/../../..)
# The test image's throwaway workload user (docker/test-oracle/initdb.d/04-capture-user.sql);
# passed by environment so no password appears on a command line (CLAUDE.md, secrets rule).
export WORKLOAD_PASSWORD ?= workload
PLUGIN_DIR := $(shell ls -d $(REPO_ROOT)/kafka-connect-oracle-cdc/target/*-kafka-connect-plugin/osodevops-kafka-connect-oracle-cdc-* 2>/dev/null | head -1)
CONFIG ?= examples/connector-freepdb1.json
NAME ?= oracle-cdc
PROFILES ?=

.PHONY: build plugin up down ps logs register status topics consume delete oracle-sql check soak

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

oracle-sql: ## open sqlplus as the capture user at CDB$ROOT (prompts for its password)
	docker exec -it oracle-cdc-lab-oracle sqlplus 'c##cdc@//localhost:1521/FREE'

check: ## run the correctness oracle against the compose stack (after a bench workload)
	java -jar $(REPO_ROOT)/bench/target/bench-*-cli.jar check --bootstrap-servers localhost:9092 \
	  --url jdbc:oracle:thin:@//localhost:1521/FREEPDB1 --user workload --password-env WORKLOAD_PASSWORD \
	  --topic-prefix cdc --pdb FREEPDB1 --tables WL_T1,WL_T2,WL_T3 --out /tmp/oracle-cdc-evidence.json

# Soak (testing strategy T3): the paced bench workload against the registered connector, the
# correctness oracle every CHECK_EVERY hours and once at the end, evidence under SOAK_OUT. The table
# topics must be empty, so start from a fresh stack: make down up register soak HOURS=1 CHECK_EVERY=0.25
HOURS ?= 72
CHECK_EVERY ?= 6
SOAK_OUT ?= soak-out/$(shell date -u +%Y%m%dT%H%M%SZ)
SOAK_HEAP ?= 12g
BENCH_JAR = $(firstword $(wildcard $(REPO_ROOT)/bench/target/bench-*-cli.jar))

soak: ## soak for HOURS (default 72), a check every CHECK_EVERY hours (default 6); keep the Mac awake
	@test -n "$(BENCH_JAR)" || { echo "bench CLI jar missing; run make build"; exit 1; }
	java -Xmx$(SOAK_HEAP) -jar $(BENCH_JAR) soak \
	  --url jdbc:oracle:thin:@//localhost:1521/FREEPDB1 --user workload --password-env WORKLOAD_PASSWORD \
	  --hours $(HOURS) --check-every $(CHECK_EVERY) \
	  --bootstrap-servers localhost:9092 --topic-prefix cdc --pdb FREEPDB1 --tables WL_T1,WL_T2,WL_T3 \
	  --connect-url http://localhost:8083 --connector $(NAME) --metrics-url http://localhost:9404/metrics \
	  --out $(SOAK_OUT)
