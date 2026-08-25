SHELL := /bin/bash
.PHONY: demo demo-full test e2e bench query check-env terraform-validate inspect catalog

ENV := set -a && source ./env.sh --quiet && set +a

DEMO_MONTHS ?= 3
DEMO_CUSTOMERS ?= 200
DEMO_FULL_MONTHS ?= 24
DEMO_FULL_CUSTOMERS ?= 5000

check-env:
	$(ENV) && python3 scripts/check_env.py

inspect:
	$(ENV) && python3 scripts/aws_inspect.py all

catalog:
	$(ENV) && python3 src/catalog.py

demo:
	$(ENV) && docker compose up -d
	$(ENV) && python3 scripts/bootstrap.py
	$(ENV) && python3 src/data_gen.py --months $(DEMO_MONTHS) --customers $(DEMO_CUSTOMERS) --out data
	$(ENV) && python3 src/upload_bronze.py --in data
	$(ENV) && python3 src/pipeline.py --customers $(DEMO_CUSTOMERS)
	$(ENV) && python3 src/facts.py
	$(ENV) && python3 src/catalog.py

demo-full:
	$(MAKE) demo DEMO_MONTHS=$(DEMO_FULL_MONTHS) DEMO_CUSTOMERS=$(DEMO_FULL_CUSTOMERS)

test:
	$(ENV) && python3 src/data_gen.py --months 6 --customers 200 --out data/bronze --seed 42
	$(ENV) && pytest tests/ -v --ignore=tests/test_e2e.py

e2e:
	$(ENV) && pytest tests/test_e2e.py -v -s

bench:
	$(ENV) && python3 src/bench.py --out benchmarks/results.json

query:
	$(ENV) && python3 -c "import sys; sys.path.insert(0,'src'); from common import warehouse; \
	con = warehouse.connect(); \
	warehouse.read_parquet(con, 's3://bank-gold/dim_customer/**/*.parquet', 'dim_customer'); \
	print(con.execute(open('sql/cohort_retention.sql').read()).fetchall())"

terraform-validate:
	terraform -chdir=terraform/azure init -backend=false
	terraform -chdir=terraform/azure validate
