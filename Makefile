NETWORK_NAME := kafka_net
COMPOSE := docker compose

.PHONY: network build infra seed app up down ps logs logs-bootstrap clean reset

network:
	@docker network inspect $(NETWORK_NAME) >/dev/null 2>&1 || docker network create $(NETWORK_NAME)

build: network
	@$(COMPOSE) build bf-topic-init bf-seed-bootstrap bf-generator bf-processor bf-dashboard

infra: network
	@$(COMPOSE) up -d zookeeper kafka kafka-2 kafka-3 minio

seed: infra
	@$(COMPOSE) up --build bf-topic-init bf-seed-bootstrap

app: seed
	@$(COMPOSE) up -d --build bf-generator bf-processor bf-dashboard

up: build app

down:
	@$(COMPOSE) down

ps:
	@$(COMPOSE) ps

logs:
	@$(COMPOSE) logs -f --tail=100 bf-topic-init bf-seed-bootstrap bf-generator bf-processor bf-dashboard

logs-bootstrap:
	@$(COMPOSE) logs --tail=100 bf-topic-init bf-seed-bootstrap

clean: down
	@rm -rf trabalho/data/out

reset: down
	@$(COMPOSE) down -v
	@rm -rf trabalho/data/out