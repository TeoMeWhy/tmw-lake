ENV_FILE = .env
SPARK_DEFAULTS_TEMPLATE_PATH = config/spark-defaults.conf.template
SPARK_DEFAULTS_PATH = config/spark-defaults.conf

include $(ENV_FILE)
export $(shell sed 's/=.*//' $(ENV_FILE))

all: build up

build:
	envsubst < $(SPARK_DEFAULTS_TEMPLATE_PATH) > $(SPARK_DEFAULTS_PATH)
	
up:
	docker compose up --build -d

down:
	docker compose down

.PHONY: all build up 