from etl_job import etl_job


if __name__ == "__main__":
    result = etl_job.execute_in_process()
    print("Dagster etl_job success:", result.success)
