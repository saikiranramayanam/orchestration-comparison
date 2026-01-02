# ETL Orchestration Comparison: Airflow vs Prefect vs Dagster

This project implements the **exact same ETL pipeline** using three popular orchestration frameworks:

- **Apache Airflow (DAG)**
- **Prefect (Flow)**
- **Dagster (Job)**

# Overview

All three pipelines:

1. Read `synthetic_events.csv` (1M synthetic user events)  
2. Filter blocked countries, compute session duration per user, count events per user per day  
3. Write identical Parquet output (partitioned by `event_date`)  

**Output parity verified:** `check_parity.py` confirms bit-for-bit identical results across all three frameworks.

# Repository Structure

orchestration-comparison/
├── airflow/ # Airflow DAG + Docker
│ ├── docker-compose.yml
│ └── dags/etl_airflow_dag.py
├── prefect/ # Prefect Flow
│ └── flow.py
├── dagster/ # Dagster Job
│ └── etl_job.py
├── shared/
│ ├── etl_core.py # Shared ETL logic (extract/transform/load)
│ └── synthetic_events.csv # Input dataset (1M rows)
├── check_parity.py # Verifies identical outputs
├── COMPARISON.md # Framework analysis
└── README.md # This file

bash
Copy code

# 🛠️ Prerequisites

```bash
# Python version
Python 3.11+

# Required Python packages
pip install pandas pyarrow
Note: Docker + Docker Compose required only for Airflow.

🚀 How to Run Each Pipeline
1. Airflow (Docker-based)
Step 1: Start Airflow
bash
Copy code
cd airflow
docker compose up -d   # Starts webserver + scheduler + Postgres
Step 2: Trigger DAG
bash
Copy code
docker compose exec airflow-webserver airflow dags trigger etl_synthetic_events \
-c '{"input_path": "/opt/airflow/shared/synthetic_events.csv", "output_path": "/opt/airflow/shared/airflow_output"}'
Step 3: Access UI
URL: http://localhost:8080

User: airflow

Pass: airflow

Step 4: Output
Parquet files stored in shared/airflow_output/

Step 5: Custom Paths
Change input_path / output_path in the JSON config when triggering DAG

2. Prefect (Pure Python)
Step 1: Install Dependencies
bash
Copy code
cd prefect
pip install prefect pandas pyarrow
Step 2: Run Flow
bash
Copy code
python flow.py
Step 3: Output
Parquet files stored in prefect/prefect_output/

Step 4: Custom Paths
python
Copy code
from flow import etl_flow

etl_flow(
    input_path="../shared/synthetic_events.csv",
    output_path="./custom_output"
)
3. Dagster (Pure Python)
Step 1: Install Dependencies
bash
Copy code
cd dagster
pip install dagster pandas pyarrow
Step 2: Run Job
bash
Copy code
python etl_job.py
Step 3: Output
Parquet files stored in dagster/dagster_output/

Step 4: Custom Paths
Edit the run_config dict in etl_job.py:

python
Copy code
run_config = {
    "extract_op": {"config": {"input_path": "../shared/synthetic_events.csv"}},
    "load_op": {"config": {"output_path": "./custom_output"}}
}
✅ Verify Output Parity
Step 1: Navigate to Root
bash
Copy code
cd ..
Step 2: Run Parity Check
bash
Copy code
python check_parity.py
Step 3: Expected Output
ini
Copy code
Airflow == Prefect: True
Prefect == Dagster: True
📊 Framework Comparison
See COMPARISON.md for detailed analysis covering:

Setup experience

Developer DX (local dev, testing, debugging)

UI / observability

Parameterization, retries, backfills

Learning curve

Recommendation for startups / production

🎯 Key Results
All three pipelines produce identical Parquet output

Parameterization works (custom input/output paths)

Retries implemented on transform task

Backfill-style runs documented with screenshots