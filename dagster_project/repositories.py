from dagster import Definitions
from dagster_project.assets.bronze import raw_ohlcv_aapl
from dagster_project.assets.silver import cleaned_ohlcv_aapl

defs = Definitions(
    assets=[raw_ohlcv_aapl, cleaned_ohlcv_aapl]
)
