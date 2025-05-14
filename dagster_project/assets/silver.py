from dagster import asset
import polars as pl
from pathlib import Path
import numpy as np

@asset
def cleaned_ohlcv_aapl(raw_ohlcv_aapl: str) -> str:
    input_path = Path("data/bronze/AAPL.parquet")
    output_path = Path("data/silver/AAPL.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load raw data
    df = pl.read_parquet(str(input_path))

    # Ensure sorted by date
    df = df.sort("Date")

    # Drop rows with nulls
    df = df.drop_nulls()

    # Ensure date type is datetime
    df = df.with_columns([
        pl.col("Date").str.strptime(pl.Datetime, fmt="%Y-%m-%d")
        if df["Date"].dtype == pl.Utf8 else pl.col("Date")
    ])

    # Calculate daily log return
    df = df.with_columns([
        (pl.col("Close_AAPL") / pl.col("Close_AAPL").shift(1)).log().alias("daily_log_return")
    ])


    # Save output
    df.write_parquet(str(output_path))
    return str(output_path)