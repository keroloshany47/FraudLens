# ─────────────────────────────────────────────────────────────────
# FraudLens Makefile
# Usage: make <target>
# ─────────────────────────────────────────────────────────────────

.PHONY: help setup start stop restart logs \
 seed stream dbt-run dbt-test dbt-docs \
 kafka-topics kafka-status \
 spark-status airflow-status \
 clean reset check

# ── COLORS ────────────────────────────────────────────────────
CYAN := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

help: ## Show this help message
	@echo ""
	@echo " $(CYAN)FraudLens$(RESET) — Available commands"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf " $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ── SETUP ─────────────────────────────────────────────────────
setup: ## First-time setup: copy .env, create folders, init containers
	@echo "$(CYAN) Setting up FraudLens...$(RESET)"
	@[ -f .env ] || (cp .env.example .env && echo "$(GREEN) .env created from .env.example$(RESET)")
	@mkdir -p data/raw airflow/logs
	@echo "$(CYAN) Starting core services...$(RESET)"
	docker compose up -d kafka postgres prometheus grafana airflow-init
	@echo "$(CYAN) Waiting for Airflow init to complete...$(RESET)"
	@sleep 20
	docker compose up -d airflow-scheduler airflow-webserver spark-master spark-worker
	@echo "$(CYAN) Waiting for Kafka to be ready...$(RESET)"
	@sleep 15
	@$(MAKE) kafka-topics
	@echo ""
	@echo "$(GREEN) FraudLens is ready!$(RESET)"
	@$(MAKE) urls

# ── START / STOP ──────────────────────────────────────────────
start: ## Start all core services
	docker compose up -d kafka postgres prometheus grafana \
	 airflow-scheduler airflow-webserver spark-master spark-worker
	@echo "$(GREEN) Core services started$(RESET)"
	@$(MAKE) urls

stop: ## Stop all services
	docker compose down
	@echo "$(YELLOW) All services stopped$(RESET)"

restart: ## Restart all services
	@$(MAKE) stop
	@$(MAKE) start

# ── LOGS ──────────────────────────────────────────────────────
logs: ## Tail logs for all services
	docker compose logs -f --tail=50

logs-kafka: ## Tail Kafka logs
	docker compose logs -f kafka

logs-spark: ## Tail Spark logs
	docker compose logs -f spark-master spark-worker

logs-airflow: ## Tail Airflow scheduler logs
	docker compose logs -f airflow-scheduler

logs-producer: ## Tail Kafka producer logs
	docker compose logs -f kafka-producer

# ── DATA PIPELINE ─────────────────────────────────────────────
seed: ## Load fraudTrain.csv into PostgreSQL OLTP via Airflow
	@echo "$(CYAN) Triggering batch load DAG...$(RESET)"
	docker compose exec airflow-webserver \
	 airflow dags trigger dag_batch_load
	@echo "$(GREEN) DAG triggered — watch at http://localhost:8082$(RESET)"

stream: ## Start the Kafka producer (replay fraudTest.csv as live stream)
	@echo "$(CYAN) Starting Kafka producer (streaming mode)...$(RESET)"
	docker compose --profile streaming up -d kafka-producer
	@echo "$(GREEN) Producer started — events flowing to raw_transactions$(RESET)"

stream-stop: ## Stop the Kafka producer
	docker compose stop kafka-producer
	docker compose rm -f kafka-producer

# ── DBT ───────────────────────────────────────────────────────
dbt-run: ## Run all dbt models (staging → intermediate → marts)
	@echo "$(CYAN) Running dbt models...$(RESET)"
	docker compose --profile dbt run -d dbt
	docker compose exec dbt dbt run --profiles-dir . --target dev --select staging+
	@echo "$(GREEN) dbt models built$(RESET)"

dbt-test: ## Run all dbt data quality tests
	@echo "$(CYAN) Running dbt tests...$(RESET)"
	docker compose exec dbt dbt test --profiles-dir . --target dev
	@echo "$(GREEN) dbt tests complete$(RESET)"

dbt-docs: ## Generate and serve dbt docs locally (http://localhost:8083)
	@echo "$(CYAN) Generating dbt docs...$(RESET)"
	docker compose exec dbt dbt docs generate --profiles-dir . --target dev
	docker compose exec dbt dbt docs serve --profiles-dir . --port 8083
	@echo "$(GREEN) dbt docs at http://localhost:8083$(RESET)"

dbt-debug: ## Test dbt database connection
	docker compose --profile dbt run -d dbt
	docker compose exec dbt dbt debug --profiles-dir . --target dev

# ── KAFKA ─────────────────────────────────────────────────────
kafka-topics: ## Create all Kafka topics (including DLQ)
	@echo "$(CYAN) Creating Kafka topics...$(RESET)"
	bash kafka/topics/create_topics.sh
	@echo "$(GREEN) Topics created$(RESET)"

kafka-status: ## List all Kafka topics and their partition count
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
	 --bootstrap-server localhost:9092 --describe

kafka-lag: ## Show consumer group lag
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
	 --bootstrap-server localhost:9092 --describe --all-groups

dlq-check: ## Show messages in the Dead Letter Queue topic
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	 --bootstrap-server localhost:9092 \
	 --topic dlq_transactions \
	 --from-beginning \
	 --max-messages 20

# ── SPARK ──────────────────────────────────────────────────────
spark-submit: ## Submit the Spark Structured Streaming job
	@echo "$(CYAN) Submitting Spark streaming job...$(RESET)"
	docker compose exec spark-master \
	 /opt/spark/bin/spark-submit \
	 --master spark://spark-master:7077 \
	 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
	 --driver-memory 512m \
	 --executor-memory 1g \
	 --conf spark.sql.shuffle.partitions=4 \
	 --py-files /opt/spark-apps/jobs/utils/geo_utils.py,/opt/spark-apps/jobs/utils/fraud_scorer.py,/opt/spark-apps/jobs/utils/dlq_handler.py \
	 /opt/spark-apps/jobs/stream_processor.py

spark-status: ## Show Spark master status
	@echo "$(CYAN)Spark UI: http://localhost:8080$(RESET)"
	@docker compose ps spark-master spark-worker

# ── STATUS ─────────────────────────────────────────────────────
status: ## Show status of all containers + UIs
	@echo ""
	@echo "$(CYAN)Container status:$(RESET)"
	@docker compose ps
	@echo ""
	@$(MAKE) urls

urls: ## Print all service URLs
	@echo ""
	@echo "$(CYAN) Service URLs$(RESET)"
	@echo " $(GREEN)Airflow UI$(RESET) → http://localhost:8082 (admin / admin)"
	@echo " $(GREEN)Spark UI$(RESET) → http://localhost:8080"
	@echo " $(GREEN)Spark Worker$(RESET) → http://localhost:8081"
	@echo " $(GREEN)Grafana$(RESET) → http://localhost:3000 (admin / fraudlens123)"
	@echo " $(GREEN)Prometheus$(RESET) → http://localhost:9090"
	@echo " $(GREEN)PostgreSQL$(RESET) → localhost:5432 (fraudlens / fraudlens_secret)"
	@echo ""

airflow-status: ## Check Airflow DAGs status
	docker compose exec airflow-webserver \
	 airflow dags list

# ── CLEAN ──────────────────────────────────────────────────────
clean: ## Stop services and remove containers (keep volumes)
	docker compose down --remove-orphans
	@echo "$(YELLOW) Containers removed (volumes preserved)$(RESET)"

reset: ## FULL RESET — remove containers AND all data volumes
	@echo "$(RED) This will delete ALL data (PostgreSQL, Prometheus, Grafana)$(RESET)"
	@read -p "Are you sure? (yes/no): " confirm; \
	 [ "$$confirm" = "yes" ] && \
	 docker compose down -v --remove-orphans && \
	 echo "$(RED) Full reset complete$(RESET)" || \
	 echo "$(GREEN) Reset cancelled$(RESET)"

check: ## Validate docker-compose.yml syntax
	docker compose config --quiet && \
	 echo "$(GREEN) docker-compose.yml is valid$(RESET)"
