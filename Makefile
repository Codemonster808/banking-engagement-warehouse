.PHONY: demo test bench query check-env terraform-validate

check-env:
	python3 scripts/check_env.py

demo:
	docker compose up -d
	python3 scripts/bootstrap.py
	python3 src/data_gen.py --months 24 --customers 5000 --out data/bronze
	python3 src/gold.py --bronze "data/bronze/month=*.jsonl"

test:
	pytest tests/ -v

bench:
	python3 src/bench.py --out benchmarks/results.json

query:
	python3 -c "import sys; sys.path.insert(0,'src'); from common import warehouse; \
	con = warehouse.connect(); \
	warehouse.read_parquet(con, 's3://bank-gold/dim_customer/**/*.parquet', 'dim_customer'); \
	print(con.execute(open('sql/cohort_retention.sql').read()).fetchall())"

terraform-validate:
	terraform -chdir=terraform/azure init -backend=false
	terraform -chdir=terraform/azure validate
