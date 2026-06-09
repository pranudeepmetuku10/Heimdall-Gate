SHELL := /bin/bash
PY    ?= python3
VENV  ?= .venv
BIN   := $(VENV)/bin

.DEFAULT_GOAL := help

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# -- Python environment --------------------------------------------------------

$(VENV)/bin/activate:
	$(PY) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip wheel

.PHONY: install
install: $(VENV)/bin/activate ## Create venv and install deps
	$(BIN)/pip install -r requirements.txt

.PHONY: install-dev
install-dev: install ## Install dev/test deps too
	$(BIN)/pip install -r requirements-dev.txt

# -- Local Kafka ---------------------------------------------------------------

.PHONY: kafka-up
kafka-up: ## Start local Kafka (KRaft) and UI
	docker compose up -d
	@echo "Kafka UI: http://localhost:8080"

.PHONY: kafka-down
kafka-down: ## Stop local Kafka
	docker compose down

.PHONY: kafka-nuke
kafka-nuke: ## Stop and delete local Kafka data
	docker compose down -v

.PHONY: topics
topics: ## Create the Heimdall Kafka topics on local broker
	./scripts/create_topics.sh

# -- Ingestion service ---------------------------------------------------------

.PHONY: ingest
ingest: ## Run the polling ingestion loop
	$(BIN)/python -m ingest

.PHONY: ingest-once
ingest-once: ## Run one polling sweep and exit
	$(BIN)/python -m ingest --once

.PHONY: consume
consume: ## Tail the raw business topic for inspection
	docker exec -it heimdall-kafka \
	  kafka-console-consumer.sh \
	    --bootstrap-server kafka:9092 \
	    --topic heimdall.raw.business \
	    --from-beginning \
	    --max-messages 20 \
	    --property print.key=true \
	    --property key.separator=' | '

# -- Tests ---------------------------------------------------------------------

.PHONY: test
test: ## Run unit tests
	$(BIN)/pytest -q

.PHONY: lint
lint: ## Run ruff lint
	$(BIN)/ruff check ingest validation config tests

.PHONY: fmt
fmt: ## Format with ruff
	$(BIN)/ruff format ingest validation config tests

# -- Smoke -----------------------------------------------------------------

.PHONY: smoke
smoke: ## Run a one-shot ingestion + DLQ injection against local Kafka
	./scripts/smoke_test.sh

.PHONY: clean
clean: ## Remove build/test caches and the venv
	rm -rf $(VENV) .pytest_cache .ruff_cache **/__pycache__ build dist *.egg-info
