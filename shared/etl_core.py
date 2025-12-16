import pandas as pd


def extract_csv(input_path: str) -> pd.DataFrame:
    """
    Read the CSV file and return a DataFrame.
    """
    df = pd.read_csv(input_path)
    return df


def transform_events(
    df: pd.DataFrame,
    blocked_countries: list[str],
) -> pd.DataFrame:
    """
    1) Remove events from blocked countries.
    2) Compute session duration per user.
    3) Count events per user per day.
    """
    # 1) Filter out blocked countries
    if blocked_countries:
        df = df[~df["country"].isin(blocked_countries)]

    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Add a date column (day part)
    df["event_date"] = df["timestamp"].dt.date

    # 2) Compute session duration per user (last - first event)
    session_df = (
        df.groupby("user_id")["timestamp"]
        .agg(["min", "max"])
        .reset_index()
    )
    session_df["session_duration_seconds"] = (
        session_df["max"] - session_df["min"]
    ).dt.total_seconds()

    # 3) Count events per user per day
    agg_df = (
        df.groupby(["user_id", "event_date"])
        .size()
        .reset_index(name="events_per_user_per_day")
    )

    # Join session duration into the daily aggregates
    result = agg_df.merge(
        session_df[["user_id", "session_duration_seconds"]],
        on="user_id",
        how="left",
    )

    return result


def load_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    """
    Write the DataFrame to Parquet, partitioned by event_date.
    """
    df.to_parquet(
        output_path,
        engine="pyarrow",
        partition_cols=["event_date"],
        index=False,
    )
