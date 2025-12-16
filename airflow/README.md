text
# Airflow ETL Pipeline: synthetic_events

This directory contains the **Airflow implementation** of the shared ETL pipeline, using the DAG `etl_synthetic_events`.

## Prerequisites

- Docker Desktop (or Docker Engine) installed and running.
- Docker Compose (v2, built into modern Docker Desktop).

## Files

- `docker-compose.yml` – starts Airflow webserver, scheduler, and Postgres.
- `dags/etl_airflow_dag.py` – defines the DAG and tasks.
- `../shared/etl_core.py` – shared extract/transform/load logic.
- `../shared/synthetic_events.csv` – input CSV mounted into the container.

The CSV is mounted to `/opt/airflow/shared/synthetic_events.csv` inside the Airflow container.

## Start Airflow

From the `airflow` folder:

cd airflow
docker compose up -d

text

Wait 30–60 seconds for the services to be healthy.

Airflow UI:

- URL: `http://localhost:8081`
- Username: `airflow`
- Password: `airflow`

## Run the DAG (default paths)

Trigger from the host using the webserver service:

docker compose exec airflow-webserver airflow dags trigger etl_synthetic_events -c "{"input_path": "/opt/airflow/shared/synthetic_events.csv", "output_path": "/opt/airflow/shared/airflow_output"}"

text

Then open the UI:

1. Go to `http://localhost:8081`.
2. Open the `etl_synthetic_events` DAG.
3. On the **Graph** / **Grid** tab, confirm that:
   - `compute_paths`
   - `extract_transform`
   - `load_data`
   all turn green (success).

The output Parquet will be written under:

- In container: `/opt/airflow/shared/airflow_output`
- On host: `<project-root>/shared/airflow_output`

## Run with Custom Input/Output Paths

You can override `input_path` and `output_path` via the JSON config:

docker compose exec airflow-webserver airflow dags trigger etl_synthetic_events -c "{"input_path": "/opt/airflow/shared/synthetic_events.csv", "output_path": "/opt/airflow/shared/airflow_output_custom"}"

text

Adjust paths to any mounted location inside the container.

## Backfill / Historical Run

For a “backfill-style” run, trigger the DAG again with the same input file. The dataset contains events for November 2025, so each run processes that historical period:

docker compose exec airflow-webserver airflow dags trigger etl_synthetic_events -c "{"input_path": "/opt/airflow/shared/synthetic_events.csv", "output_path": "/opt/airflow/shared/airflow_output"}"

text

You can take a screenshot of the Graph view showing all tasks green as evidence of a successful historical run.

## Retries

- The combined `extract_transform` task is configured with:
  - `retries=2`
  - `retry_delay=10` seconds
- Inside the task, a random failure is simulated some of the time to exercise the retry logic.

Logs for retries can be viewed per task from the Airflow UI.