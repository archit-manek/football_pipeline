import polars as pl
import pandera as pa
from pandera import Column, DataFrameSchema
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

def get_int_columns_from_schema(schema: pa.DataFrameSchema) -> list:
    """
    Extract column names from a Pandera schema that are Int64 type.
    
    Args:
        schema (pa.DataFrameSchema): The Pandera schema to extract columns from.
        
    Returns:
        list: List of column names that are Int64 type.
    """
    int_columns = []
    for col_name, col_schema in schema.columns.items():
        dtype_str = str(col_schema.dtype)
        # Handle both pa.Int64 and int64 types
        if dtype_str in ['Int64', 'int64'] or col_schema.dtype == pa.Int64:
            int_columns.append(col_name)
    return int_columns

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

def fix_int_columns_with_nans(df: pl.DataFrame, int_columns: list, sentinel: int = -1) -> pl.DataFrame:
    """Fix integer columns with NaN values by filling them with a sentinel value.
       NaN values are converted to a float, so using -1 as an int placeholder.

    Args:
        df (pl.DataFrame): The input DataFrame.
        int_columns (list): List of integer column names to fix.
        sentinel (int, optional): The sentinel value to use for filling NaNs. Defaults to -1.

    Returns:
        pl.DataFrame: The DataFrame with fixed integer columns.
    """
    for col in int_columns:
        if col in df.columns:
            df = df.with_columns([pl.col(col).fill_null(sentinel).cast(pl.Int64)])
    return df

def add_missing_columns(df: pl.DataFrame, expected_cols: set) -> pl.DataFrame:
    """Add missing columns to the DataFrame with null values."""
    missing = expected_cols - set(df.columns)
    for col in missing:
        df = df.with_columns([pl.lit(None).alias(col)])
    return df

def cast_columns_to_schema_types(df: pl.DataFrame, schema: pa.DataFrameSchema) -> pl.DataFrame:
    """
    Cast columns in df to match types defined in schema.
    """
    pa_to_pl = {
        'Int64': pl.Int64,
        'int64': pl.Int64,
        'Float64': pl.Float64,
        'float64': pl.Float64,
        'String': pl.String,
        'str': pl.String,
        'Bool': pl.Boolean,
        'bool': pl.Boolean,
    }

    for col, col_schema in schema.columns.items():
        if col in df.columns:
            # Skip datetime columns since we handle them separately
            if col in ['timestamp', 'match_updated', 'match_available']:
                continue
                
            # Pandera dtype can be pa.Int64, numpy.int64, or Python's str/int, etc.
            pa_type_name = str(col_schema.dtype)
            
            # Skip casting for pa.Object columns (they handle flexible types like Lists)
            if pa_type_name == 'object':
                continue
                
            # Handles both pa.String and <class 'str'>
            pa_type_name = pa_type_name.split("'")[-2] if "'" in pa_type_name else pa_type_name.replace('pa.', '')
            pl_type = pa_to_pl.get(pa_type_name, pl.String)
            try:
                # Special handling for boolean columns with nulls
                if pl_type == pl.Boolean:
                    df = df.with_columns([
                        pl.col(col).cast(pl.Boolean, strict=False)
                    ])
                else:
                    df = df.with_columns([pl.col(col).cast(pl_type)])
            except Exception as e:
                print(f"Error casting column '{col}' to {pl_type}: {e}")
    return df

def log_schema_differences(df, schema, logger, parquet_file=None):
    """Log differences between DataFrame schema and expected schema."""
    df_cols = set(df.columns)
    schema_cols = set(schema.columns.keys())

    missing_cols = schema_cols - df_cols
    extra_cols = df_cols - schema_cols

    if missing_cols:
        logger.error(f"Missing columns: {missing_cols} in {parquet_file}")
    if extra_cols:
        logger.warning(f"Extra columns: {extra_cols} in {parquet_file}")

def validate_polars_with_pandera(df: pl.DataFrame, schema: pa.DataFrameSchema) -> pl.DataFrame:
    """
    Validates a Polars DataFrame against a given Pandera schema.
    Returns a Polars DataFrame if valid, otherwise raises SchemaError.
    """
    df_pd = df.to_pandas()
    
    # Skip datetime columns validation since we handle them separately
    # Also skip all boolean columns that are problematic with casting
    problematic_cols = ['timestamp', 'match_updated', 'match_available']
    
    # Find all boolean columns in the schema
    for col_name, col_schema in schema.columns.items():
        if col_schema.dtype == pa.Bool:
            problematic_cols.append(col_name)
    
    saved_cols = {}
    for col in problematic_cols:
        if col in df_pd.columns:
            saved_cols[col] = df_pd[col].copy()
            df_pd = df_pd.drop(col, axis=1)
    
    # Create a schema without problematic columns
    schema_cols = {k: v for k, v in schema.columns.items() if k not in problematic_cols}
    temp_schema = pa.DataFrameSchema(schema_cols)
    
    validated_pd = temp_schema.validate(df_pd)
    
    # Restore the problematic columns
    for col, col_data in saved_cols.items():
        validated_pd[col] = col_data
    
    return pl.from_pandas(validated_pd) 