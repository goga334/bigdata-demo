SHELL := /bin/bash
COMPOSE := docker compose
DC := $(COMPOSE) -f docker-compose.yml

.PHONY: help restart start-docker config down up-core airflow-init up-airflow status logs-airflow kafka-smoke nuke

help:
	@echo "make restart       - повний перезапуск стеку (Docker→Postgres+Kafka→airflow-init→Airflow)"
	@echo "make status        - показати стан контейнерів"
	@echo "make logs-airflow  - останні логи webserver/scheduler"
	@echo "make kafka-smoke   - міні-тест Kafka→bronze всередині webserver"
	@echo "make nuke          - знести все з volume-ами"
	@echo "make config        - валідація docker-compose.yml"

restart: start-docker config down up-core airflow-init up-airflow status
	@echo ""
	@echo "✅ Стек перезапущено."
	@echo "UI Airflow:  http://localhost:8080  (admin / admin)"
	@echo ""

start-docker:
	@echo "→ Перевіряю docker demon…"
	@docker info >/dev/null 2>&1 || \
	( echo "…не запущено, пробую підняти"; \
	  (sudo /usr/local/share/docker-init.sh >/dev/null 2>&1 || true); \
	  (sudo service docker start >/dev/null 2>&1 || sudo /etc/init.d/docker start >/dev/null 2>&1 || true); \
	  sleep 2; \
	  docker info >/dev/null 2>&1 || (echo "❌ Docker не стартував. Перевір devcontainer/Codespace або права до /var/run/docker.sock" && exit 1) )
	@echo "✓ Docker працює"

config:
	@echo "→ Валідація compose…"
	@$(DC) config >/dev/null
	@echo "✓ docker-compose.yml валідний"

down:
	@echo "→ Зупиняю попередні сервіси (якщо були)…"
	@$(DC) down --remove-orphans >/dev/null || true
	@echo "✓ Зупинено"

up-core:
	@echo "→ Піднімаю Postgres + Kafka…"
	@$(DC) up -d postgres kafka
	@echo "…чекаю декілька секунд…"
	@sleep 5

airflow-init:
	@echo "→ Ініціалізація Airflow…"
	@$(DC) run --rm airflow-init

up-airflow:
	@echo "→ Піднімаю Airflow webserver + scheduler…"
	@$(DC) up -d airflow-webserver airflow-scheduler
	@echo "…даю їм стартанути…"
	@sleep 5

status:
	@echo "→ Статус контейнерів:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

logs-airflow:
	@echo "→ Останні логи webserver:"
	@docker logs --tail=80 bigdata-demo-airflow-webserver-1 2>/dev/null || true
	@echo ""
	@echo "→ Останні логи scheduler:"
	@docker logs --tail=80 bigdata-demo-airflow-scheduler-1 2>/dev/null || true

kafka-smoke:
	@echo "→ Швидкий тест Kafka→bronze всередині webserver…"
	@docker exec -i bigdata-demo-airflow-webserver-1 bash -lc "PYTHONPATH=/opt python -c \"from app.kafka_io import produce_events; print('Producing 50…'); produce_events('kafka:9092', n=50)\""
	@docker exec -i bigdata-demo-airflow-webserver-1 bash -lc "PYTHONPATH=/opt python -c \"from app.kafka_io import consume_to_bronze; print('Consuming…'); consume_to_bronze('kafka:9092', '/opt/app/data/bronze_events.parquet', max_msgs=50, timeout_s=5); print('OK')\""
	@echo "→ Файли у /opt/app/data:"
	@docker exec -i bigdata-demo-airflow-webserver-1 bash -lc "ls -lh /opt/app/data || true"

nuke:
	@echo "⚠️  Зношу все з volume-ами…"
	@$(DC) down -v --remove-orphans
