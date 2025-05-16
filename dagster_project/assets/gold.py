from dagster import asset
import polars as pl
from pathlib import Path

@asset
def momentum_signal_aapl(cleaned_ohlcv_aapl: str) -> str:
    input_path = Path(cleaned_ohlcv_aapl)
    output_path = Path("data/gold/AAPL_momentum.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pl.read_parquet(str(input_path)).sort("Date")

    # Calculate 20-day momentum
    df = df.with_columns([
        (pl.col("Close_AAPL") / pl.col("Close_AAPL").shift(20)).log().alias("momentum_20d")
    ])

    # Compute forward 10-day return
    df = df.with_columns([
        (pl.col("Close_AAPL").shift(-10) / pl.col("Close_AAPL")).log().alias("forward_10d_return")
    ])

    # Select candidates: top 10% momentum values
    df = df.with_columns([
        (pl.col("momentum_20d") > df.select("momentum_20d").quantile(0.90)[0, 0]).alias("is_candidate")
    ])

    df.write_parquet(str(output_path))
    return str(output_path)
