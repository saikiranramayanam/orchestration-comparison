# ETL Pipeline Comparison: Airflow vs Prefect vs Dagster

## 1. Setup and Installation

### Airflow

- Used the provided `airflow/docker-compose.yml` to run Airflow in Docker (webserver, scheduler, Postgres). [web:145]
- Started the stack from the `airflow` folder with `docker compose up`, then opened the UI at `http://localhost:8081`. [web:124]
- Placed the DAG file `etl_synthetic_events` in `airflow/dags/`, turned the DAG toggle ON in the UI, and triggered runs from the UI and CLI. [web:124]

### Prefect

- Created a Python 3.11 virtual environment `venv-prefect` in the project root and installed `prefect<3`, `pandas`, and `pyarrow` with `pip`. [web:107][web:110]
- Implemented `prefect/flow.py` with `@flow` and `@task` functions calling the shared `extract_csv`, `transform_events`, and `load_to_parquet` from `shared/etl_core.py`. [web:61][web:121]
- Ran the Prefect flow locally using `python flow.py`, which wrote partitioned Parquet files to `prefect/prefect_output/event_date=...`. [web:95]

### Dagster

- In the same virtual environment, installed `dagster` via `pip install dagster`. [web:124][web:125]
- Created `dagster/etl_job.py` defining a job `etl_job` composed of three ops (`extract_op`, `transform_op`, `load_op`) that reuse the same shared ETL functions and write to `dagster/dagster_output`. [web:127][web:135]
- Executed the job in-process with `etl_job.execute_in_process()`, confirming successful run and partitioned outputs. [web:137][web:134]

## 2. Pipeline Structure and Concepts

### Airflow

- Describes the ETL as a DAG (`etl_synthetic_events`) made of three PythonOperator tasks: `compute_paths` / `extract_transform` / `load_data`. [web:124]
- Task dependencies are defined in code using `compute_paths >> extract_transform >> load_data`, so Airflow controls order and retries, while the ETL logic lives in `shared/etl_core.py`. [web:83]
- Parameters like input and output paths are passed via DAG config / JSON when triggering, allowing the same DAG to write to default or custom folders. [web:148]

### Prefect

- Uses a `@flow` function `etl_flow` with three `@task` functions that directly call the shared `extract_csv`, `transform_events`, and `load_to_parquet`. [web:61][web:121]
- Data flows by returning Pandas DataFrames between tasks (`df -> transformed_df -> load`), so dependencies come from normal Python call order instead of a separate DAG definition. [web:65]
- Parameters (input and output paths) are normal function arguments to `etl_flow`, making it easy to change them when calling `etl_flow(...)` from Python. [web:112]

### Dagster

- Represents the ETL as a `@job` called `etl_job` composed of three `@op`s (`extract_op`, `transform_op`, `load_op`). [web:127][web:135]
- The ops form a graph where the output of `extract_op` feeds `transform_op`, and its output feeds `load_op`, with Dagster handling materialization and type checks. [web:135]
- For simplicity in this project, the job passes input and output paths via `run_config` in `etl_job.execute_in_process()`, while still reusing the same shared ETL functions. [web:134]

## 3. Features: Parameters, Retries, Backfills

### Parameters (input/output paths)

- Airflow: Paths are passed as JSON config when triggering the DAG, allowing runs that target either default folders or custom ones like `airflow_output_custom`. [web:124][web:148]
- Prefect: `etl_flow` takes `input_path` and `output_path` as normal Python arguments, so changing destinations is just changing the function call. [web:61][web:112]
- Dagster: Paths are supplied via `run_config` for `extract_op` and `load_op`, and can be changed without modifying the job code. [web:127][web:135]

### Retries

- Airflow: Retries are configured on the combined `extract_transform` task, with 2 retries and a 10‑second delay, and the task also simulates random failures to exercise this behavior. [web:83][web:37]
- Prefect: The `transform_task` is decorated with `@task(retries=2, retry_delay_seconds=10)`, so Prefect will automatically rerun that step on failure with a 10‑second delay. [web:66][web:109]
- Dagster: In this simple setup the job runs once per call; Dagster also supports per‑op retry policies, but this project focuses mainly on proving the ETL graph and output match the other tools. [web:135][web:137]

### Backfills / Historical runs

- Airflow: Backfill is simulated by triggering the DAG on the full synthetic CSV, producing `event_date=2025‑11‑01` to `2025‑11‑30` partitions; re‑running with the same input effectively backfills all dates again. [web:84][web:145]
- Prefect: Running `etl_flow` again with the same input file recreates the full November partition set, acting as a manual backfill for all days. [web:95]
- Dagster: Each execution of `etl_job` processes the full CSV and rewrites all November partitions, so repeated runs behave like backfills for the whole period. [web:127][web:134]

## 4. Developer Experience

### Airflow

- Setup felt heavier because it required Docker, multiple services, and waiting for the webserver and scheduler to start, but after that the UI made it easy to see DAGs and task states. [web:124][web:145]
- Writing the DAG required more boilerplate (DAG object, default args, PythonOperators), and dealing with JSON config for parameters was slightly confusing at first. [web:83][web:148]
- Debugging was helped by the Graph view and task logs in the UI, but container logs and configuration warnings sometimes added noise. [web:28][web:151]

### Prefect

- Installation and running were lightweight: just a virtual environment plus `pip install`, then the flow ran directly with `python flow.py` and printed structured logs to the terminal. [web:107][web:61]
- The code felt close to normal Python: `@flow` and `@task` decorators around regular functions, with parameters passed as standard arguments and DataFrames flowing through return values. [web:65][web:112]
- Retries were easy to express in one line on the transform task, and there was no separate UI required to get useful feedback for this project. [web:66][web:109]

### Dagster

- Installing Dagster was straightforward with `pip`, but getting the job configuration right required a few iterations and some understanding of how ops and jobs compose. [web:124][web:135]
- The `@job` and `@op` structure made the pipeline graph explicit, and `execute_in_process()` provided rich debug logs without starting the full Dagster web UI. [web:127][web:137]
- Compared to Prefect, Dagster felt a bit more opinionated and structured, but once the job was defined it ran reliably and produced the same outputs as the other tools. [web:134][web:152]

## 5. UI and Observability

### Airflow

- Provides a rich web UI at `http://localhost:8081` with DAG list, Graph view, Gantt charts, and per‑task logs; this made it easy to see which tasks succeeded or failed and to re‑run specific tasks. [web:28][web:124]
- The Graph view clearly showed the extract → transform → load dependencies and the success state (green) of each task for every DAG run. [web:28][web:83]

### Prefect

- For this project, runs were observed mainly through the CLI logs printed by `python flow.py`, which include flow and task names, start/end times, and retry behavior. [web:61][web:112]
- Prefect also has a cloud/hosted UI, but it was not required here; the lightweight local logging was sufficient to confirm that the ETL ran correctly and produced the expected partitions. [web:79][web:107]

### Dagster

- Instead of starting the Dagster web UI, the job was executed in‑process with `etl_job.execute_in_process()`, which produced detailed step‑level logs in the terminal (step start, outputs, success). [web:137][web:134]
- Dagster’s logging showed each op (`extract_op`, `transform_op`, `load_op`) as a separate step, which helped verify that data flowed through the graph and that the final load step wrote all partitions. [web:135][web:138]

## 6. Summary Table and Recommendation

### High-level comparison

| Aspect                | Airflow                                                  | Prefect                                                  | Dagster                                                  |
|-----------------------|----------------------------------------------------------|----------------------------------------------------------|----------------------------------------------------------|
| Setup                 | Heavier: Docker, multiple services, UI + scheduler. [web:124][web:145] | Light: Python venv + `pip install`, runs with `python`. [web:107][web:61] | Medium: Python venv + `pip install dagster`. [web:124][web:134] |
| Pipeline model        | DAG with Operators and explicit dependencies. [web:83]   | `@flow` + `@task` functions, dependencies via calls. [web:65] | Job + ops graph with strong structure. [web:135][web:127] |
| Parameters            | JSON config or DAG params at trigger time. [web:148]    | Normal Python arguments to the flow. [web:112]           | Paths passed via job run config. [web:135]               |
| Retries               | Configurable per task, plus custom logic in code. [web:37] | Simple decorator options on tasks. [web:66]             | Supports policies but not emphasized here. [web:135]     |
| Backfill pattern      | Natural fit via DAG runs over historical dates or full CSV re-runs. [web:84] | Manual re-runs over same input file. [web:95]           | Re-running job rewrites all partitions. [web:127]        |
| UI / observability    | Strong web UI with graphs and logs. [web:28]            | Good CLI logs, optional UI if needed. [web:61][web:79]  | Detailed step logs; optional Dagster UI. [web:137][web:134] |
| Best suited for       | Mature, scheduled data warehouse pipelines. [web:145]   | Pythonic, fast-iteration workflows and ML-ish tasks. [web:146][web:147] | Structured data platforms where graphs and assets matter. [web:134][web:152] |

### Personal recommendation

- For **a student or small team** wanting something close to normal Python and easy local runs, Prefect felt the most straightforward: minimal setup, simple decorators, and clear logs. [web:61][web:112]  
- For **classic enterprise-style scheduled ETL** with many teams and strong operational UIs, Airflow is still a solid choice, especially when you need its mature scheduler and ecosystem. [web:145][web:148]  
- For **future expansion into data platforms with rich graphs/assets**, Dagster is promising, but it introduces more structure and concepts, so it may feel heavier than Prefect for a first project. [web:134][web:152]
### Airflow backfill / historical run

Command:
docker compose exec airflow-webserver airflow dags trigger etl_synthetic_events -c "{\"input_path\": \"/opt/airflow/shared/synthetic_events.csv\", \"output_path\": \"/opt/airflow/shared/airflow_output_2025_12_15\"}"

Screenshot:

![Airflow historical run](./screenshots/airflow_backfill.png)

### Prefect backfill / historical run

Command used:

cd prefect
python flow.py

text

This reruns the same ETL flow and we treat this rerun as a historical/backfill run.

Screenshot:

![Prefect historical run](./screenshots/prefect_backfill.png)
text
### Dagster backfill / historical run

Command used:

cd dagster
python etl_job.py

text

We rerun the same `etl_job` pipeline and treat this rerun as a historical/backfill-style execution.

Screenshot:
![Dagster historical run](./screenshots/dagster_backfill.png)
