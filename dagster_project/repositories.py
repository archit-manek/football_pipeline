from dagster import Definitions
from dagster_project.assets.bronze import raw_ohlcv_aapl

defs = Definitions(
    assets=[raw_ohlcv_aapl]
)
