# minikube + Strimzi lab. Run from anywhere: make -C lab/local/k8s <target>
include versions.env
export
REPO_ROOT := $(abspath $(CURDIR)/../../..)
PLUGIN_DIR := $(shell ls -d $(REPO_ROOT)/kafka-connect-oracle-cdc/target/*-kafka-connect-plugin/osodevops-kafka-connect-oracle-cdc-* 2>/dev/null | head -1)
KUBECTL := kubectl --context $(MINIKUBE_PROFILE) -n $(NAMESPACE)
OPERATOR_YAML := strimzi-cluster-operator-$(STRIMZI_VERSION).yaml

.PHONY: up down cluster operator kafka oracle-image connect-image images connect connector status plugins consume logs smoke \
        chaos-kill-worker chaos-rollout chaos-oracle-restart chaos-partition chaos-unpartition

up: cluster operator kafka images connect connector status ## everything from a clean machine

cluster:
	minikube status -p $(MINIKUBE_PROFILE) >/dev/null 2>&1 || \
	  minikube start -p $(MINIKUBE_PROFILE) --driver=docker --cpus=$(MINIKUBE_CPUS) --memory=$(MINIKUBE_MEMORY) --disk-size=$(MINIKUBE_DISK)
	kubectl --context $(MINIKUBE_PROFILE) apply -f namespace.yaml

operator: ## pinned Strimzi release manifest, namespace-scoped to $(NAMESPACE)
	test -f $(OPERATOR_YAML) || curl -fsSL -o $(OPERATOR_YAML) \
	  https://github.com/strimzi/strimzi-kafka-operator/releases/download/$(STRIMZI_VERSION)/$(OPERATOR_YAML)
	sed 's/namespace: .*/namespace: $(NAMESPACE)/' $(OPERATOR_YAML) | $(KUBECTL) apply -f -
	$(KUBECTL) wait --for=condition=Established crd/kafkas.kafka.strimzi.io crd/kafkanodepools.kafka.strimzi.io \
	  crd/kafkaconnects.kafka.strimzi.io crd/kafkaconnectors.kafka.strimzi.io --timeout=120s
	$(KUBECTL) wait --for=condition=Available deployment/strimzi-cluster-operator --timeout=300s
	@# a single-replica operator on a busy laptop loses its leader lease and exits; no election needed
	$(KUBECTL) set env deployment/strimzi-cluster-operator STRIMZI_LEADER_ELECTION_ENABLED=false
	@# kubectl caches API discovery on disk for hours; purge it so the new CRDs are visible
	rm -rf ~/.kube/cache/discovery/$$(kubectl --context $(MINIKUBE_PROFILE) config view --minify -o jsonpath='{.clusters[0].cluster.server}' | sed -E 's#https?://##; s#[:/]#_#g')

kafka:
	@for i in 1 2 3 4 5 6; do $(KUBECTL) apply -f kafka.yaml && break; echo "retrying apply (discovery cache)"; sleep 10; $(KUBECTL) api-resources --api-group=kafka.strimzi.io >/dev/null; done
	$(KUBECTL) wait kafka/lab --for=condition=Ready --timeout=600s

oracle-image: ## build the derived Oracle image and load it into minikube
	docker build --build-arg BASE_TAG=$(ORACLE_BASE_TAG) -t oracle-cdc-test-db:$(ORACLE_BASE_TAG) $(REPO_ROOT)/docker/test-oracle
	minikube -p $(MINIKUBE_PROFILE) image load oracle-cdc-test-db:$(ORACLE_BASE_TAG)

connect-image: ## package the connector, build the Strimzi Connect image with the plugin, load it, roll Connect onto it
	cd $(REPO_ROOT) && mvn -q package -DskipTests -DskipE2E --batch-mode --no-transfer-progress
	@test -n "$(PLUGIN_DIR)" || { echo "plugin dir missing"; exit 1; }
	rm -rf connect-image/plugin && cp -R "$(PLUGIN_DIR)" connect-image/plugin
	@# a unique tag per build: minikube keeps an existing tag, so re-loading :dev never reaches the pods
	$(eval CONNECT_TAG := dev-$(shell date +%Y%m%d%H%M%S))
	docker build --build-arg STRIMZI_VERSION=$(STRIMZI_VERSION) --build-arg KAFKA_VERSION=$(KAFKA_VERSION) -t oracle-cdc-connect:$(CONNECT_TAG) -t oracle-cdc-connect:dev connect-image
	minikube -p $(MINIKUBE_PROFILE) image load oracle-cdc-connect:$(CONNECT_TAG)
	@# patch the running KafkaConnect when it exists so Strimzi rolls the workers onto the new image
	-$(KUBECTL) get kafkaconnect connect >/dev/null 2>&1 && $(KUBECTL) patch kafkaconnect connect --type merge -p '{"spec":{"image":"oracle-cdc-connect:$(CONNECT_TAG)"}}'
	@echo "connect image oracle-cdc-connect:$(CONNECT_TAG) loaded"

images: oracle-image connect-image
	$(KUBECTL) apply -f oracle.yaml
	$(KUBECTL) rollout status statefulset/oracle --timeout=900s

connect:
	$(KUBECTL) apply -f metrics-configmap.yaml -f connect.yaml
	$(KUBECTL) wait kafkaconnect/connect --for=condition=Ready --timeout=600s

connector:
	$(KUBECTL) apply -f connector.yaml
	@for i in $$(seq 1 60); do \
	  s=$$($(KUBECTL) get kafkaconnector oracle-cdc -o jsonpath='{.status.connectorStatus.connector.state}' 2>/dev/null); \
	  [ "$$s" = "RUNNING" ] && echo "connector RUNNING" && exit 0; sleep 5; done; \
	  echo "connector not RUNNING"; $(KUBECTL) get kafkaconnector oracle-cdc -o yaml | tail -30; exit 1

status:
	$(KUBECTL) get kafka,kafkanodepool,kafkaconnect,kafkaconnector,pods,pvc

plugins: ## the plugin must be listed exactly once
	$(KUBECTL) exec $$($(KUBECTL) get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[0].metadata.name}') -- curl -s localhost:8083/connector-plugins | python3 -c 'import sys,json; [print(p["class"], p["version"]) for p in json.load(sys.stdin)]'

consume: ## TOPIC=cdc.FREEPDB1.WORKLOAD.ORDERS make consume (read_committed)
	$(KUBECTL) exec lab-dual-0 -- bin/kafka-console-consumer.sh --bootstrap-server lab-kafka-bootstrap:9092 \
	  --topic $(TOPIC) --from-beginning --isolation-level read_committed --timeout-ms 20000 --property print.headers=true

logs:
	$(KUBECTL) logs -l strimzi.io/name=connect-connect -c connect-connect --tail=200 -f

smoke: plugins ## plugin listed once, connector RUNNING, Oracle survives a pod restart
	$(KUBECTL) get kafkaconnector oracle-cdc -o jsonpath='{.status.connectorStatus.connector.state}{"\n"}' | grep RUNNING
	$(MAKE) chaos-oracle-restart

chaos-kill-worker: ## SIGKILL one Connect worker (no grace period)
	$(KUBECTL) delete pod $$($(KUBECTL) get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[0].metadata.name}') --grace-period=0 --force
	$(KUBECTL) wait kafkaconnect/connect --for=condition=Ready --timeout=300s

chaos-rollout: ## Strimzi-driven rolling update of the Connect cluster
	$(KUBECTL) annotate kafkaconnect connect strimzi.io/manual-rolling-update=true --overwrite
	sleep 10; $(KUBECTL) wait kafkaconnect/connect --for=condition=Ready --timeout=600s

chaos-oracle-restart: ## delete the database pod gracefully; it must come back from the PVC with ARCHIVELOG intact
	$(KUBECTL) delete pod oracle-0 --grace-period=120
	$(KUBECTL) rollout status statefulset/oracle --timeout=900s
	$(KUBECTL) exec oracle-0 -- bash -c "echo \"SELECT log_mode FROM v\\\$$database;\" | sqlplus -s c##cdc/cdc@//localhost:1521/FREE" | grep ARCHIVELOG

chaos-partition: ## deny Connect to Oracle traffic (needs a policy-enforcing CNI: minikube start --cni=calico)
	$(KUBECTL) apply -f chaos/deny-connect-to-oracle.yaml

chaos-unpartition:
	$(KUBECTL) delete -f chaos/deny-connect-to-oracle.yaml --ignore-not-found

down:
	minikube delete -p $(MINIKUBE_PROFILE)
