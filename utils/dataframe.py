import polars as pl
import pandas as pd
from pathlib import Path
import json

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

def serialize_all_lists(data):
    for event in data:
        for k, v in event.items():
            if isinstance(v, list):
                event[k] = json.dumps(v, ensure_ascii=False)
    return data

def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert dot notation to underscore notation for silver layer standardization.
    Example: 'player.name' -> 'player_name', 'team.stats.goals' -> 'team_stats_goals'
    """
    rename_map = {col: col.replace('.', '_') for col in df.columns}
    return df.rename(rename_map)

def apply_schema_flexibly(df: pl.DataFrame, target_schema: dict, logger=None) -> pl.DataFrame:
    """
    Apply schema to dataframe flexibly, with error logging for failed casts.
    """
    failed_casts = []
    # Add missing columns
    for col_name, col_type in target_schema.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))
    
    # Cast with logging
    cast_expressions = []
    for col_name, col_type in target_schema.items():
        if col_name in df.columns:
            try:
                cast_expressions.append(pl.col(col_name).cast(col_type, strict=False).alias(col_name))
            except Exception:
                failed_casts.append(col_name)
                cast_expressions.append(pl.col(col_name))  # Keep as-is

    if cast_expressions:
        df = df.with_columns(cast_expressions)
    
    # Select only columns in schema
    df = df.select([col for col in target_schema.keys() if col in df.columns])

    if logger and failed_casts:
        logger.warning(f"Columns failed to cast to target schema: {failed_casts}")

    return df

def create_dataframe_safely(data, target_schema: dict, logger=None) -> pl.DataFrame:
    """
    Create a DataFrame from JSON data safely.
    Uses pandas json_normalize for flattening, then standardizes column names to underscores.
    """
    try:
        # Primary approach: Use pandas json_normalize for flattening (with dots)
        df_pd = pd.json_normalize(data)  # Use default dot separator
        df = pl.from_pandas(df_pd)
        
        if logger:
            logger.info(f"Flattened JSON with pandas: {len(df)} rows, {len(df.columns)} columns")
        
        # Standardize column names: convert dots to underscores for consistent schema application
        df = normalize_column_names(df)
        
        if logger:
            logger.info(f"Normalized column names to underscores")
        
        return apply_schema_flexibly(df, target_schema, logger)
    except Exception as e:
        if logger:
            logger.warning(f"Pandas flattening failed: {e} | Attempting simple Polars fallback")
        try:
            # Simple fallback: Polars with string-first approach
            df = pl.DataFrame(data, infer_schema_length=0)  # Forces string inference
            
            # Apply same normalization to fallback
            df = normalize_column_names(df)
            
            if logger:
                logger.info(f"Created DataFrame with Polars string-first approach: {len(df)} rows, {len(df.columns)} columns")
            return apply_schema_flexibly(df, target_schema, logger)
        except Exception as e2:
            if logger:
                logger.error(f"Both pandas and Polars failed. Data is likely too complex for bronze layer processing")
            raise Exception(f"DataFrame creation failed. Consider processing this data in the silver layer. Pandas error: {e}, Polars error: {e2}")

