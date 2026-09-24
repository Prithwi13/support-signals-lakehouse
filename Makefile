PY ?= python3
export PYTHONPATH := src

.PHONY: install test lint run dashboard airflow package tf-init tf-plan tf-apply tf-destroy clean

install:            ## create .venv and install everything
	$(PY) -m venv .venv && .venv/bin/pip install -U pip && .venv/bin/pip install -e ".[dev,dashboard]"

test:               ## unit + end-to-end tests (no network, no AWS)
	pytest -q

lint:
	ruff check src tests dags glue dashboard

run:                ## full local pipeline on real GitHub data (set GITHUB_TOKEN for 5,000 req/hr)
	$(PY) -m signals all

dashboard:          ## Streamlit dashboard over data/warehouse.duckdb
	streamlit run dashboard/app.py

airflow:            ## local Airflow at http://localhost:8080 running the real DAG
	docker compose up --build

package:            ## zip the package for AWS Glue (--extra-py-files)
	rm -rf dist && mkdir -p dist && cd src && zip -qr ../dist/signals.zip signals -x '*/__pycache__/*'

tf-init:
	cd infra/terraform && terraform init
tf-plan: package
	cd infra/terraform && terraform plan
tf-apply: package
	cd infra/terraform && terraform apply
tf-destroy:         ## tear everything down (stops all AWS charges)
	cd infra/terraform && terraform destroy

clean:
	rm -rf data dist .pytest_cache spark-warehouse metastore_db derby.log
