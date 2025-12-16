import pandas as pd

airflow_df = pd.read_parquet("shared/airflow_output")
prefect_df = pd.read_parquet("prefect/prefect_output")

print("Airflow shape:", airflow_df.shape)
print("Prefect shape:", prefect_df.shape)
print("\nAirflow columns:", list(airflow_df.columns))
print("Prefect columns:", list(prefect_df.columns))

print("\nFirst 5 rows Airflow:")
print(airflow_df.head())

print("\nFirst 5 rows Prefect:")
print(prefect_df.head())
