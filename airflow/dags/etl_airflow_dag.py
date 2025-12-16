from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
from pathlib import Path

# Make sure /opt/airflow/shared is on Python path inside the container
SHARED_PATH = Path("/opt/airflow/shared")
if str(SHARED_PATH) not in sys.path:
    sys.path.append(str(SHARED_PATH))

from etl_core import extract_csv, transform_events, load_to_parquet  # type: ignore

# Default arguments for all tasks in this DAG
default_args = {
    "owner": "airflow",
    "retries": 0,  # retries only on transform task
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="etl_synthetic_events",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule=None,  # manual runs only for now
    catchup=False,
    tags=["etl", "comparison"],
) as dag:
    from airflow.utils.context import Context

    def get_paths(context: Context) -> dict:
        conf = context["dag_run"].conf or {}
        input_path = conf.get(
            "input_path",
            "/opt/airflow/shared/synthetic_events.csv",
        )
        final_output_path = conf.get(
            "output_path",
            "/opt/airflow/shared/airflow_output",
        )
        return {
            "input_path": input_path,
            "final_output_path": final_output_path,
        }

    def compute_paths(**context):
        return get_paths(context)

    paths = PythonOperator(
        task_id="compute_paths",
        python_callable=compute_paths,
    )

    def extract_and_transform(**context):
        """
        Single task: extract CSV and apply transform_events.
        Returns transformed DataFrame as JSON via XCom.
        """
        from random import random
        import pandas as pd

        ti = context["ti"]
        paths = ti.xcom_pull(task_ids="compute_paths")

        # simulate intermittent failure to exercise retries
        if random() < 0.3:
            raise RuntimeError("Simulated random failure in extract_and_transform")

        df = extract_csv(paths["input_path"])
        transformed = transform_events(df, blocked_countries=["Russia", "China"])

        # Return JSON so it can be passed via XCom
        return transformed.to_json(orient="split", date_format="iso")

    def load_from_xcom(**context):
        """
        Load step: read transformed JSON from XCom, normalize types to match
        Prefect/Dagster, then write final partitioned Parquet.
        """
        import pandas as pd

        ti = context["ti"]
        paths = ti.xcom_pull(task_ids="compute_paths")
        df_json = ti.xcom_pull(task_ids="extract_transform")

        df = pd.read_json(df_json, orient="split")

        # Normalize types to match Prefect output
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
        df["session_duration_seconds"] = df["session_duration_seconds"].astype(float)

        load_to_parquet(df, paths["final_output_path"])

    extract_transform = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_and_transform,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_from_xcom,
    )

    paths >> extract_transform >> load
