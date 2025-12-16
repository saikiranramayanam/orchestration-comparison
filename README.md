text
# ETL Orchestration: Airflow vs Prefect vs Dagster

## Overview

This project implements the **same ETL pipeline** in three orchestration frameworks:

- Apache Airflow (DAG)
- Prefect (Flow)
- Dagster (Job)

All three use a **shared core module** (`shared/etl_core.py`) and produce **bit‑for‑bit identical Parquet outputs** when run on the same `synthetic_events.csv`.

The ETL steps:

1. **Extract**: Read `synthetic_events.csv` (synthetic user events).
2. **Transform**:
   - Filter out events from blocked countries (e.g. `["Russia", "China"]).
   - Compute session duration per user (first vs last timestamp).
   - Count events per user per day.
3. **Load**: Write the final aggregated data to Parquet, **partitioned by event_date**.

## Repository Structure

.
├── airflow/ # Airflow DAG + Docker compose
│ └── dags/
│ └── etl_airflow_dag.py
├── prefect/ # Prefect flow
│ └── flow.py
├── dagster/ # Dagster job
│ └── etl_job.py
├── shared/
│ ├── etl_core.py # Shared extract/transform/load logic
│ └── synthetic_events.csv # Synthetic input dataset
├── COMPARISON.md # Detailed framework comparison
├── check_parity.py # Script to verify outputs are identical
└── README.md # This file

text

> Note: `synthetic_events.csv` is stored under `shared/` and mounted into Airflow’s container.

## Prerequisites

- Python 3.11 (for Prefect and Dagster)
- `pip`
- Docker + Docker Compose (for Airflow)

Recommended:

pip install pandas pyarrow

text

## How to Run All Three Pipelines

### 1. Airflow

See `airflow/README.md` for full details. Short version:

cd airflow
docker compose up -d # start webserver, scheduler, postgres

then, inside the webserver container:
docker compose exec airflow-webserver airflow dags trigger etl_synthetic_events -c "{"input_path": "/opt/airflow/shared/synthetic_events.csv", "output_path": "/opt/airflow/shared/airflow_output"}"

text

UI: `http://localhost:8081` (user: `airflow`, password: `airflow`).

### 2. Prefect

cd prefect
pip install prefect pandas pyarrow
python flow.py

text

This reads `../synthetic_events.csv` and writes Parquet partitions under `prefect/prefect_output/`.

To use different paths, run the flow from Python:

from flow import etl_flow
etl_flow(input_path="../synthetic_events.csv", output_path="./some_other_output")

text

### 3. Dagster

cd dagster
pip install dagster pandas pyarrow
python etl_job.py

text

This reads `../synthetic_events.csv` and writes Parquet partitions under `dagster/dagster_output/`.

To change paths, edit the `run_config` dict at the bottom of `etl_job.py`.

## Output Parity Check

After running all three pipelines once:

cd <project-root>
python check_parity.py

text

Expected output:

Airflow == Prefect: True
Prefect == Dagster: True

text

This verifies that all three implementations produce **identical** Parquet outputs.

## More Details

See `COMPARISON.md` for:

- Setup experience
- Concepts (DAG vs Flow vs Job)
- UI / observability
- Parameterization, retries, backfills
- Final recommendation for different use cases
