import polars as pl
import pandas as pd
from utils.dataframe import normalize_column_names

def apply_schema_flexibly(df: pl.DataFrame, target_schema: dict, logger=None) -> pl.DataFrame:
    """
    Apply schema to dataframe flexibly, with error logging for failed casts.

    Args:
        df (pl.DataFrame): The dataframe to apply the schema to.
        target_schema (dict): The target schema to apply.
        logger (Logger, optional): The logger to use. Defaults to None.

    Returns:
        pl.DataFrame: The dataframe with the schema applied.
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

    Args:
        data (dict): The JSON object to create a dataframe from.
        target_schema (dict): The target schema to apply.
        logger (Logger, optional): The logger to use. Defaults to None.

    Returns:
        pl.DataFrame: The dataframe with the schema applied.
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
