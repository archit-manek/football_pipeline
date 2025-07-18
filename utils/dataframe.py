import polars as pl
from pathlib import Path

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

def apply_schema_flexibly(df: pl.DataFrame, target_schema: dict) -> pl.DataFrame:
    """
    Apply schema to dataframe flexibly, handling missing columns and type mismatches.
    """
    # Add missing columns with null values
    for col_name, col_type in target_schema.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))
    
    # Cast existing columns to target types, handling errors gracefully
    cast_expressions = []
    for col_name, col_type in target_schema.items():
        if col_name in df.columns:
            try:
                cast_expressions.append(pl.col(col_name).cast(col_type, strict=False).alias(col_name))
            except:
                # If casting fails, keep original column
                cast_expressions.append(pl.col(col_name))
    
    # Cast all at once
    if cast_expressions:
        df = df.with_columns(cast_expressions)
    
    # Select only columns that are in the target schema
    df = df.select([col for col in target_schema.keys() if col in df.columns])
    
    return df

def create_dataframe_safely(data, target_schema: dict) -> pl.DataFrame:
    """
    Create a DataFrame from JSON data safely, avoiding schema inference issues.
    Includes recursive struct flattening for nested JSON data.
    """
    try:
        # First try to create with inferred schema
        df = pl.DataFrame(data)
        # Flatten any nested structs using the comprehensive flatten_columns function
        df = flatten_columns(df)
        return apply_schema_flexibly(df, target_schema)
    except Exception as e:
        # If that fails, try with increased schema inference length
        try:
            df = pl.DataFrame(data, infer_schema_length=10000)
            df = flatten_columns(df)
            return apply_schema_flexibly(df, target_schema)
        except Exception as e2:
            # If that fails, create with all string columns first, then cast
            try:
                # Force all columns to be strings initially to avoid inference issues
                df = pl.DataFrame(data, infer_schema_length=0)  # This forces string inference
                df = flatten_columns(df)
                return apply_schema_flexibly(df, target_schema)
            except Exception as e3:
                # Last resort: create with very limited schema inference
                try:
                    df = pl.DataFrame(data, infer_schema_length=1)
                    df = flatten_columns(df)
                    return apply_schema_flexibly(df, target_schema)
                except Exception as e4:
                    # If all else fails, raise the original error with context
                    raise Exception(f"Failed to create DataFrame safely. Original error: {e}. Last error: {e4}")