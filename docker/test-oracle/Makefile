# Derived Oracle Database Free test image.
IMAGE ?= oracle-cdc-test-db
BASE_TAG ?= 23.26.3-slim-faststart
TAG ?= $(BASE_TAG)
PASSWORD ?= oracle
CONTAINER ?= oracle-cdc-test-db

.PHONY: build run stop logs sqlplus status

build:
	docker build --build-arg BASE_TAG=$(BASE_TAG) -t $(IMAGE):$(TAG) .

run: ## first start takes a few minutes while the hooks convert the database
	docker run -d --name $(CONTAINER) -p 1521:1521 -e ORACLE_PASSWORD=$(PASSWORD) --shm-size=2g $(IMAGE):$(TAG)
	@echo "follow with: make logs"

stop:
	-docker rm -f $(CONTAINER)

logs:
	docker logs -f $(CONTAINER)

sqlplus:
	docker exec -it $(CONTAINER) sqlplus / as sysdba

status:
	docker exec $(CONTAINER) bash -c "sqlplus -s / as sysdba <<'SQL'\nSET HEADING OFF FEEDBACK OFF\nSELECT log_mode, supplemental_log_data_min FROM v\$$database;\nSELECT name, open_mode FROM v\$$pdbs;\nSELECT group#, bytes/1048576, status FROM v\$$log ORDER BY 1;\nEXIT\nSQL"
