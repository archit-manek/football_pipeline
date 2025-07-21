import polars as pl

def enforce_schema(df, schema):
    for col, typ in schema.items():
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(typ))
        else:
            df = df.with_columns(pl.lit(None).cast(typ).alias(col))
    return df