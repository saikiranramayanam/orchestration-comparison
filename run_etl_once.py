import sys
from pathlib import Path

from shared.etl_core import extract_csv, transform_events, load_to_parquet


def main():
    """
    Small helper to run the ETL once from the command line.
    Usage:
        python run_etl_once.py <input_csv_path> <output_folder_path>
    """
    if len(sys.argv) != 3:
        print("Usage: python run_etl_once.py <input_csv_path> <output_folder_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_folder = sys.argv[2]

    # Convert to Path objects (nice for Windows paths)
    input_path = Path(input_path)
    output_folder = Path(output_folder)

    print(f"Reading CSV from: {input_path}")
    df = extract_csv(str(input_path))
    print(f"Rows in raw CSV: {len(df)}")

    # No countries blocked for now
    transformed = transform_events(df, blocked_countries=[])
    print(f"Rows after transform: {len(transformed)}")

    # Ensure output folder exists
    output_folder.mkdir(parents=True, exist_ok=True)

    print(f"Writing Parquet output to: {output_folder}")
    load_to_parquet(transformed, str(output_folder))

    print("ETL finished successfully.")


if __name__ == "__main__":
    main()
