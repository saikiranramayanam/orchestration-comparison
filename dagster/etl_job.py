from pathlib import Path
import sys

from dagster import job, op, Field, String, Array

# --- Make repo root importable so `shared` works ---
CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent  # one level up from dagster/
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.etl_core import extract_csv, transform_events, load_to_parquet


@op(config_schema={"input_path": Field(String)})
def extract_op(context):
    input_path = context.op_config["input_path"]
    df = extract_csv(input_path)
    context.log.info(f"Extracted {len(df)} rows from {input_path}")
    return df


@op(config_schema={"blocked_countries": Field(Array(String))})  # <- fixed here
def transform_op(context, df):
    blocked = context.op_config["blocked_countries"]
    result = transform_events(df, blocked_countries=blocked)
    context.log.info(f"Transformed to {len(result)} user-day rows")
    return result


@op(config_schema={"output_path": Field(String)})
def load_op(context, df):
    output_path = context.op_config["output_path"]
    load_to_parquet(df, output_path)
    context.log.info(f"Wrote Parquet to {output_path}")


@job
def etl_job():
    raw = extract_op()
    transformed = transform_op(raw)
    load_op(transformed)


if __name__ == "__main__":
    run_config = {
        "ops": {
            "extract_op": {
                "config": {
                    "input_path": "../synthetic_events.csv",
                }
            },
            "transform_op": {
                "config": {
                    "blocked_countries": ["Russia", "China"],
                }
            },
            "load_op": {
                "config": {
                    "output_path": "./dagster_output",
                }
            },
        }
    }
    result = etl_job.execute_in_process(run_config=run_config)
    print("Dagster run success:", result.success)
