COMPOSE := docker compose

.PHONY: up down logs ps clean test

up:
	$(COMPOSE) up -d kafka kafka-ui

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f kafka kafka-ui

ps:
	$(COMPOSE) ps

clean:
	$(COMPOSE) down -v

test:
	PYTHONPATH=. python -m pytest -q ingest/tests spark/tests tests
