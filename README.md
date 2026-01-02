ETL Orchestration Comparison: Airflow vs Prefect vs Dagster
This project implements the exact same ETL pipeline using three popular orchestration frameworks:

Apache Airflow (DAG)

Prefect (Flow)

Dagster (Job)

All three pipelines:

Read synthetic_events.csv (1M synthetic user events)

Filter blocked countries, compute session duration per user, count events per user per day

Write identical Parquet output (partitioned by event_date)

Output parity verified: check_parity.py confirms bit-for-bit identical results across all three.

📁 Repository Structure
text
orchestration-comparison/
├── airflow/                 # Airflow DAG + Docker
│   ├── docker-compose.yml
│   └── dags/etl_airflow_dag.py
├── prefect/                 # Prefect flow
│   └── flow.py
├── dagster/                 # Dagster job
│   └── etl_job.py
├── shared/
│   ├── etl_core.py         # Shared ETL logic (extract/transform/load)
│   └── synthetic_events.csv # Input dataset (1M rows)
├── check_parity.py          # Verifies identical outputs
├── COMPARISON.md            # Framework analysis
└── README.md                # This file
🛠️ Prerequisites
bash
# Python 3.11+
pip install pandas pyarrow

# Docker + Docker Compose (only for Airflow)
🚀 How to Run Each Pipeline
1. Airflow (Docker-based)
bash
cd airflow
docker compose up -d  # Starts webserver + scheduler + Postgres
Trigger DAG (inside webserver container):

bash
docker compose exec airflow-webserver airflow dags trigger etl_synthetic_events \
  -c '{"input_path": "/opt/airflow/shared/synthetic_events.csv", "output_path": "/opt/airflow/shared/airflow_output"}'
UI: http://localhost:8080 (user: airflow, pass: airflow)

Output: shared/airflow_output/ (Parquet files)

Custom paths: Change input_path/output_path in the JSON config.

2. Prefect (Pure Python)
bash
cd prefect
pip install prefect pandas pyarrow
python flow.py
Output: prefect/prefect_output/ (Parquet files)

Custom paths (edit and run):

python
from flow import etl_flow
etl_flow(
    input_path="../shared/synthetic_events.csv", 
    output_path="./custom_output"
)
3. Dagster (Pure Python)
bash
cd dagster
pip install dagster pandas pyarrow
python etl_job.py
Output: dagster/dagster_output/ (Parquet files)

Custom paths: Edit run_config dict in etl_job.py:

python
"extract_op": {"config": {"input_path": "../shared/synthetic_events.csv"}},
"load_op": {"config": {"output_path": "./custom_output"}},
✅ Verify Output Parity
After running all three pipelines:

bash
cd ..  # back to root
python check_parity.py
Expected:

text
Airflow == Prefect: True
Prefect == Dagster: True
📊 Framework Comparison
See COMPARISON.md for detailed analysis covering:

Setup experience

Developer DX (local dev, testing, debugging)

UI/observability

Parameterization, retries, backfills

Learning curve

Recommendation for startups/production

🎯 Key Results
All three pipelines produce identical Parquet output

Parameterization works (custom input/output paths)

Retries implemented on transform task

Backfill-style runs documented with screenshots

