import polars as pl
import pandas as pd
from pathlib import Path
import json

from football_pipeline.utils.io import is_source_newer
from football_pipeline.utils.logging import NullLogger

def serialize_all_lists(data, logger=None, log_every=100000, description=""):
    """
    Serialize all lists in a JSON object to JSON strings.

    Args:
        data (dict): The JSON object to serialize.
        logger (Logger, optional): The logger to use. Defaults to None.
        log_every (int, optional): The frequency of logging. Defaults to 100000.
        description (str, optional): The description of the data. Defaults to "".

    Returns:
        dict: The serialized JSON object.
    """
    for i, event in enumerate(data):
        for k, v in event.items():
            if isinstance(v, list):
                event[k] = json.dumps(v, ensure_ascii=False)
        if logger and (i + 1) % log_every == 0:
            logger.info(f"Serialized lists for {i + 1} {description}...")
    return data

def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert dot notation to underscore notation for silver layer standardization.
    Example: 'player.name' -> 'player_name', 'team.stats.goals' -> 'team_stats_goals'

    Args:
        df (pl.DataFrame): The dataframe to normalize.

    Returns:
        pl.DataFrame: The normalized dataframe.
    """
    rename_map = {col: col.replace('.', '_') for col in df.columns}
    return df.rename(rename_map)

## PARQUET INGESTION FUNCTIONS ##

def ingest_json_to_parquet(
    input_file: Path,
    output_file: Path,
    logger=None,
    description: str = "",
    serialize_lists: bool = True,
    overwrite: bool = False,
):
    """
    Ingest a single JSON file (list or dict) to Parquet.

    Args:
        input_file (Path): The path to the input JSON file.
        output_file (Path): The path to the output Parquet file.
        logger (Logger, optional): The logger to use. Defaults to None.
        description (str, optional): The description of the data. Defaults to "".
        serialize_lists (bool, optional): Whether to serialize lists to JSON strings. Defaults to True.
        overwrite (bool, optional): Whether to overwrite the output file if it already exists. Defaults to False.
    """
    if logger is None:
        logger = NullLogger()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not is_source_newer(input_file, output_file) and not overwrite:
        logger.info(f"{description.title()} file {output_file} is up to date, skipping.")
        return
    if not input_file.exists():
        logger.warning(f"{description.title()} file {input_file} not found, skipping.")
        return
    try:
        with open(input_file, "r") as f:
            data = json.load(f)
        # Make sure data is a list of records
        if isinstance(data, dict):
            data = [data]
        if serialize_lists:
            data = serialize_all_lists(data, logger=logger, description=description)
        df_pd = pd.json_normalize(data)
        df = pl.from_pandas(df_pd)
        df = normalize_column_names(df)
        df.write_parquet(output_file, compression="snappy")
        logger.info(f"Successfully processed {len(df)} {description} records to {output_file}")
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in {description} file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing {description} data: {e}")
        raise

def ingest_json_batch_to_parquet(
    input_dir: Path,
    output_dir: Path,
    logger=None,
    description: str = "",
    file_pattern: str = "*.json",
    serialize_lists: bool = True,
    output_prefix: str = "",
    log_frequency: int = 50
):
    """
    Ingest all JSON files in a directory to Parquet files (one per input).

    Args:
        input_dir (Path): The path to the input directory.
        output_dir (Path): The path to the output directory.
        logger (Logger, optional): The logger to use. Defaults to None.
        description (str, optional): The description of the data. Defaults to "".
        file_pattern (str, optional): The pattern to match the input files. Defaults to "*.json".
        serialize_lists (bool, optional): Whether to serialize lists to JSON strings. Defaults to True.
        output_prefix (str, optional): The prefix to add to the output filename. Defaults to "".
        log_frequency (int, optional): The frequency of logging. Defaults to 50.
    """
    if logger is None:
        logger = NullLogger()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_files = list(input_dir.glob(file_pattern))
    logger.info(f"Found {len(json_files)} {description} files to process.")
    processed_count = 0
    skipped_count = 0
    error_count = 0
    for json_file in json_files:
        # Construct output filename
        if output_prefix:
            output_filename = f"{output_prefix}_{json_file.stem}.parquet"
        else:
            output_filename = f"{json_file.stem}.parquet"
        output_file = output_dir / output_filename

        try:
            ingest_json_to_parquet(
                input_file=json_file,
                output_file=output_file,
                logger=NullLogger(),
                description=f"{description} {json_file.stem}",
                serialize_lists=serialize_lists,
            )
            processed_count += 1
            if processed_count % log_frequency == 0:
                logger.info(f"Processed {processed_count} {description} files so far.")
        except Exception as e:
            logger.error(f"Failed to process {json_file}: {e}")
            error_count += 1

    summary_msg = f"{description.title()} batch ingest complete: {processed_count} processed, {error_count} errors"
    logger.info(summary_msg)
    return processed_count, skipped_count, error_count

## CSV INGESTION FUNCTIONS ##

def ingest_csv_to_parquet(
    input_file: Path,
    output_file: Path,
    logger=None,
    description: str = "",
    overwrite: bool = False,
):
    """
    Ingest a single CSV file and write it to Parquet.

    Args:
        input_file (Path): The path to the input CSV file.
        output_file (Path): The path to the output Parquet file.
        logger (Logger, optional): The logger to use. Defaults to None.
        description (str, optional): The description of the data. Defaults to "".
        overwrite (bool, optional): Whether to overwrite the output file if it already exists. Defaults to False.
    """
    if logger is None:
        logger = NullLogger()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not is_source_newer(input_file, output_file) and not overwrite:
        logger.info(f"{description.title()} file {output_file} is up to date, skipping.")
        return
    if not input_file.exists():
        logger.warning(f"{description.title()} file {input_file} not found, skipping.")
        return
    try:
        df_pd = pd.read_csv(input_file)
        df = pl.from_pandas(df_pd)
        df = df.rename({col: col.replace('.', '_') for col in df.columns})
        df.write_parquet(output_file, compression="snappy")
        logger.info(f"Successfully processed {len(df)} {description} records to {output_file}")
    except Exception as e:
        logger.error(f"Error processing {description} data: {e}")
        raise

def ingest_csv_batch_to_parquet(
    input_dir: Path,
    output_dir: Path,
    logger=None,
    description: str = "csv",
    file_pattern: str = "*.csv",
    overwrite: bool = False,
    log_frequency: int = 10,
):
    """
    Ingest all CSV files in a directory to Parquet files (one per CSV).

    Args:
        input_dir (Path): The path to the input directory.
        output_dir (Path): The path to the output directory.
        logger (Logger, optional): The logger to use. Defaults to None.
        description (str, optional): The description of the data. Defaults to "".
        file_pattern (str, optional): The pattern to match the input files. Defaults to "*.csv".
        overwrite (bool, optional): Whether to overwrite the output file if it already exists. Defaults to False.
        log_frequency (int, optional): The frequency of logging. Defaults to 10.
    """
    if logger is None:
        logger = NullLogger()
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_files = list(input_dir.glob(file_pattern))
    if not csv_files:
        logger.warning(f"No CSV files found in {input_dir}")
        return
    processed_count = 0
    error_count = 0
    for i, csv_file in enumerate(csv_files, 1):
        output_file = output_dir / csv_file.with_suffix('.parquet').name
        try:
            ingest_csv_to_parquet(
                input_file=csv_file,
                output_file=output_file,
                logger=logger,
                description=f"{description} {csv_file.stem}",
                overwrite=overwrite,
            )
            processed_count += 1
            if processed_count % log_frequency == 0:
                logger.info(f"Processed {processed_count} CSV files so far.")
        except Exception as e:
            logger.error(f"Error processing {csv_file}: {e}")
            error_count += 1

    logger.info(f"{description.title()} batch ingest complete: {processed_count} CSV files processed, {error_count} errors")
