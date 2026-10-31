.PHONY: sync dbt-build airflow-test test

sync:
	uv sync --dev

dbt-build:
	DBT_DUCKDB_PATH=$(PWD)/dbt_project/dev.duckdb uv run dbt build \
		--project-dir dbt_project --profiles-dir dbt_project

airflow-test:
	AIRFLOW_HOME=$(PWD)/.airflow \
	AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/airflow/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	AIRFLOW__CORE__EXECUTOR=SequentialExecutor \
	AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(PWD)/.airflow/airflow.db \
	DBT_DUCKDB_PATH=$(PWD)/dbt_project/dev.duckdb \
	bash -c "uv run airflow db migrate && uv run airflow dags test nightly_orders 2026-10-27"

test:
	uv run pytest -q
