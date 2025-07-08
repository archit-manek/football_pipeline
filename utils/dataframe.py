import polars as pl

def flatten_columns(df):
    """
    Recursively flatten all struct columns and list-of-struct columns in a DataFrame.
    """
    while True:
        # Find struct columns
        struct_cols = [col for col in df.columns if df.schema[col] == pl.Struct]
        # Find list-of-struct columns
        list_struct_cols = [
            col for col in df.columns
            if isinstance(df.schema[col], pl.List) and getattr(df.schema[col], 'inner', None) == pl.Struct
        ]
        if not struct_cols and not list_struct_cols:
            break

        # Flatten struct columns
        for col in struct_cols:
            fields = df.schema[col].fields
            df = df.with_columns([
                pl.col(col).struct.field(field.name).alias(f"{col}_{field.name}") for field in fields
            ]).drop(col)

        # Explode and flatten list-of-struct columns
        for col in list_struct_cols:
            df = df.explode(col)
            # After exploding, the column is now a struct, so flatten it
            fields = df.schema[col].fields
            df = df.with_columns([
                pl.col(col).struct.field(field.name).alias(f"{col}_{field.name}") for field in fields
            ]).drop(col)
    # Rename columns to replace '.' with '_'
    df = df.rename({col: col.replace('.', '_') for col in df.columns})
    return df