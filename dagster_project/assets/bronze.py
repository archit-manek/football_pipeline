from dagster import asset
import yfinance as yf
import polars as pl
from pathlib import Path
import pandas as pd

TICKER = "AAPL"
START_DATE = "2015-01-01"
END_DATE = "2025-01-01"
BRONZE_PATH = Path("data/bronze")

@asset
def raw_ohlcv_aapl() -> str:
    # Ensure data/bronze/ exists
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    # Fetch raw OHLCV data
    df = yf.download(TICKER, start=START_DATE, end=END_DATE)

    if df.empty:
        raise ValueError(f"No data for {TICKER} in range {START_DATE}–{END_DATE}")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(filter(None, col)).strip() for col in df.columns]

    # Convert to Polars
    df.reset_index(inplace=True)
    pl_df = pl.from_pandas(df)

    # Write to Parquet
    file_path = BRONZE_PATH / f"{TICKER}.parquet"
    pl_df.write_parquet(str(file_path))

    return str(file_path)