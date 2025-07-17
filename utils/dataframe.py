import polars as pl
from pathlib import Path
from typing import Dict, List

def is_source_newer(source_path: Path, output_path: Path) -> bool:
    """
    Check if source file is newer than output file.
    
    Args:
        source_path (Path): Path to the source file
        output_path (Path): Path to the output file
        
    Returns:
        bool: True if source is newer than output or output doesn't exist, False otherwise
    """
    if not output_path.exists():
        return True
    return source_path.stat().st_mtime > output_path.stat().st_mtime

def get_int_columns_from_schema(schema: Dict[str, pl.DataType]) -> List[str]:
    """
    Return all column names whose declared Polars dtype is pl.Int64.
    
    Args
    ----
    schema : dict[str, pl.DataType]
        Your explicit Polars schema mapping.
    
    Returns
    -------
    list[str]
        Column names declared as Int64.
    """
    return [name for name, dtype in schema.items() if dtype == pl.Int64]

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

def add_missing_columns(df: pl.DataFrame, expected_cols: set) -> pl.DataFrame:
    """
    Add missing columns to the DataFrame with null values.
    """
    missing = expected_cols - set(df.columns)
    for col in missing:
        df = df.with_columns([pl.lit(None).alias(col)])
    return df