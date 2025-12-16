text
# Prefect ETL Flow: synthetic_events

This directory contains the **Prefect implementation** of the shared ETL pipeline as a flow `etl_flow`.

## Prerequisites

- Python 3.11
- `pip`

Recommended to create a virtual environment in the project root:

python -m venv .venv
..venv\Scripts\activate # Windows

source .venv/bin/activate # Linux/macOS
pip install prefect pandas pyarrow

text

## Files

- `flow.py` – defines the Prefect flow and tasks.
- `../shared/etl_core.py` – shared extract/transform/load logic.
- `../synthetic_events.csv` – input CSV in the project root.

## Run the Flow (default paths)

From this folder:

cd prefect
python flow.py

text

This will:

- Read `../synthetic_events.csv`.
- Filter blocked countries, compute session duration, aggregate per user per day.
- Write partitioned Parquet output to `prefect/prefect_output/` (subfolders by `event_date`).

You will see Prefect logs like:

- Flow run starting
- Task run `extract_task`, `transform_task`, `load_task`
- Flow run completed

## Run with Custom Input/Output Paths

Instead of using the `__main__` block, you can import and call the flow with different arguments:

from flow import etl_flow

etl_flow(
input_path="../synthetic_events.csv",
output_path="./prefect_output_custom",
)

text

You can also add extra parameters (e.g. date, blocked countries) by extending the flow signature.

## Retries

- `transform_task` is decorated with `@task(retries=2, retry_delay_seconds=10)`.
- A failure in the transform step will cause Prefect to retry that task up to two times, with 10 seconds between attempts.

## Backfill / Historical Run

The synthetic dataset covers November 2025. Re-running `etl_flow` with the same input acts as a manual backfill over that historical period:

python flow.py

or
etl_flow(input_path="../synthetic_events.csv", output_path="./prefect_output")

text

Each run recomputes all partitions for the month.
