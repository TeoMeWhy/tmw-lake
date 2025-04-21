ENV_FILE = .env
SPARK_DEFAULTS_TEMPLATE_PATH = config/spark-defaults.conf.template
SPARK_DEFAULTS_PATH = config/spark-defaults.conf
NETWORK_NAME=datalake-network

include $(ENV_FILE)
export $(shell sed 's/=.*//' $(ENV_FILE))

.PHONY: run
run: up deploy-flows


.PHONY: create-network
create-network:
	@if ! docker network inspect $(NETWORK_NAME) > /dev/null 2>&1; then \
		echo "🔧 Criando rede $(NETWORK_NAME)..."; \
		docker network create $(NETWORK_NAME); \
	else \
		echo "✅ Rede $(NETWORK_NAME) já existe."; \
	fi


.PHONY: build
build: create-network
	envsubst < $(SPARK_DEFAULTS_TEMPLATE_PATH) > $(SPARK_DEFAULTS_PATH)

.PHONY: up	
up: build
	docker compose up --build -d


.PHONY: deploy-flows
deploy-flows:
	docker exec prefect-server python deploy.py


.PHONY: down
down:
	docker compose down
	docker network rm $(NETWORK_NAME)
