# utils.py
import polars as pl

def safe_from_dicts(rows, schema=None):
    if not rows:
        if schema:
            return pl.DataFrame(schema=schema)
        return pl.DataFrame()
    return pl.from_dicts(rows)