text
# Dagster ETL Job: synthetic_events

This directory contains the **Dagster implementation** of the shared ETL pipeline as a job `etl_job`.

## Prerequisites

- Python 3.11
- `pip`

Recommended to reuse the same virtual environment as Prefect:

In project root
..venv\Scripts\activate # or source .venv/bin/activate
pip install dagster pandas pyarrow

text

## Files

- `etl_job.py` – defines the Dagster job and ops.
- `../shared/etl_core.py` – shared extract/transform/load logic.
- `../synthetic_events.csv` – input CSV in project root.

## Run the Job (default config)

From this folder:

cd dagster
python etl_job.py

text

This will:

- Execute `etl_job.execute_in_process(...)` with a `run_config` that:
  - Reads `../synthetic_events.csv`.
  - Uses `["Russia", "China"]` as blocked countries.
  - Writes Parquet partitions to `dagster/dagster_output`.

You should see logs for:

- `extract_op`
- `transform_op`
- `load_op`
- Final line: `Dagster run success: True`

## Change Input/Output Paths

Paths are configured in the `run_config` dict at the bottom of `etl_job.py`:

run_config = {
"ops": {
"extract_op": {
"config": {"input_path": "../synthetic_events.csv"}
},
"transform_op": {
"config": {"blocked_countries": ["Russia", "China"]}
},
"load_op": {
"config": {"output_path": "./dagster_output"}
},
}
}

text

To run with a different output folder, change `./dagster_output` to another path and run `python etl_job.py` again.

## Retries

This minimal implementation focuses on matching the graph and output with Airflow/Prefect. Dagster supports per‑op retry policies, which can be added via op definitions if needed.

## Backfill / Historical Run

Each call to `etl_job.execute_in_process()` processes the full `synthetic_events.csv` (November 2025 data) and rewrites all partitions:

python etl_job.py # first “historical” run
python etl_job.py # second run, effectively a backfill

text

Repeated runs behave like backfills over the full historical range present in the CSV.