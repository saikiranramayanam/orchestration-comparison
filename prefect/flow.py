from pathlib import Path
import sys
import os

# --- Make repo root importable so `shared` works ---
CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent  # one level up from prefect/
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from prefect import flow, task
from shared.etl_core import extract_csv, transform_events, load_to_parquet


@task
def extract_task(input_path: str):
    # Read the CSV and return a DataFrame
    input_path = Path(input_path)
    df = extract_csv(str(input_path))
    return df


@task(retries=2, retry_delay_seconds=10)
def transform_task(df):
    # Apply the business logic: filter countries, compute sessions, aggregate
    blocked = ["Russia", "China"]
    transformed_df = transform_events(df, blocked_countries=blocked)
    return transformed_df


@task
def load_task(df, output_path: str):
    # Write the transformed DataFrame to partitioned Parquet
    output_path = Path(output_path)
    load_to_parquet(df, str(output_path))


@flow
def etl_flow(input_path: str, output_path: str):
    df = extract_task(input_path)
    transformed_df = transform_task(df)
    load_task(transformed_df, output_path)


if __name__ == "__main__":
    etl_flow(
        input_path="../synthetic_events.csv",
        output_path="./prefect_output",
    )
