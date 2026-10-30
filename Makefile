# Everything runs in Docker (compose project "lakehouse", host ports 16092/16432).
# Only `make unit` needs a local Python (uv).
SHELL := /bin/bash
COMPOSE := docker compose
APP := $(COMPOSE) run --rm -T app

.PHONY: up build test unit test-1m demo bench down

up:        ## Kafka (KRaft, 1 broker) + Postgres 17
	$(COMPOSE) up -d --wait kafka postgres

build:     ## the app image: pipeline, dbt, tests
	$(COMPOSE) --profile app build app

test: up build  ## every test, in the app container next to the brokers
	$(APP) python -m pytest -q

unit:      ## the tests that need no Kafka/Postgres, on the host
	uv run pytest -q -m "not integration"

test-1m: up build  ## the two-phase end-to-end comparison at 1M messages (raw output in results/)
	mkdir -p results/raw
	$(APP) env LAKEHOUSE_N=1000000 python -m pytest -v -k two_phases 2>&1 | tee results/raw/test-1m-$$(date -u +%Y%m%dT%H%M%SZ).log

demo: up build  ## 100k messages through the whole pipeline, then verify
	$(APP) python -m lakehouse produce --n 100000
	$(APP) python -m lakehouse ingest --stop-at-eof
	$(APP) sh -c 'cd dbt && dbt run --profiles-dir . && dbt test --profiles-dir .'
	$(APP) python -m lakehouse verify --n 100000

bench: up build  ## 1M messages, 1 and 3 ingest workers, 3 runs each -> results/bench.jsonl
	scripts/bench.sh 1 3
	scripts/bench.sh 3 3

down:      ## stop and delete this project's containers and volumes
	$(COMPOSE) --profile app down -v
