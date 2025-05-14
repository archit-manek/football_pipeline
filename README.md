# Momentum Pipeline Project (Dagster + Polars)

## Medallion Pipeline Architecture

### Bronze Layer: Raw Ingestion
- Fetches OHLCV data for AAPL (2015–2025) from Yahoo Finance
- Flattens multi-index columns if necessary
- Saves raw data to `data/bronze/` as Parquet

### Silver Layer: Clean + Enrich
- Reads Bronze parquet
- Fixes column names
- Converts dates to datetime
- Sorts chronologically
- Drops nulls
- Computes `daily_log_return`
- Saves clean data to `data/silver/`

### Gold Layer (Planned):
- Calculate 20-day rolling return (momentum)
- Rank tickers daily by momentum
- Select top 10% as signal candidates
- Compute forward 10-day return to evaluate signal quality

## Tools Used

| Tool       | Why It Was Used |
|------------|------------------|
| [Dagster](https://dagster.io) | Modern orchestration & asset tracking |
| [Polars](https://pola.rs)     | Fast, expressive DataFrame engine |
| [Parquet](https://parquet.apache.org/) | Efficient columnar storage format |
| [yfinance](https://github.com/ranaroussi/yfinance) | Pull historical OHLCV data |

---

## How to Run It

### 1. Clone and set up environment, run with Dagster UI:

```bash
git clone https://github.com/yourname/momentum-pipeline.git
cd momentum-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

dagster dev -f dagster_project/repositories.py