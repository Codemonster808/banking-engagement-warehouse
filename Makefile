.PHONY: demo test e2e bench query check-env terraform-validate

check-env:
	python3 scripts/check_env.py

demo:
	docker compose up -d
	python3 scripts/bootstrap.py
	python3 src/data_gen.py --months 24 --customers 5000 --out data
	python3 src/upload_bronze.py --in data
	python3 src/pipeline.py --customers 5000
	python3 src/facts.py

test:
	python3 src/data_gen.py --months 6 --customers 200 --out data/bronze --seed 42
	pytest tests/ -v --ignore=tests/test_e2e.py

e2e:
	pytest tests/test_e2e.py -v -s

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
