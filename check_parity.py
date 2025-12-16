import pandas as pd

airflow_path = "shared/airflow_output"       # your Airflow output
prefect_path = "prefect/prefect_output"      # Prefect output
dagster_path = "dagster/dagster_output"      # Dagster output

airflow_df = pd.read_parquet(airflow_path)
prefect_df = pd.read_parquet(prefect_path)
dagster_df = pd.read_parquet(dagster_path)

print("Airflow == Prefect:", airflow_df.equals(prefect_df))
print("Prefect == Dagster:", prefect_df.equals(dagster_df))
