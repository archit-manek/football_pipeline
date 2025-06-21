from pathlib import Path
import json
import pandas as pd

RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def get_valid_match_ids():
    """
    Get all valid match IDs from the matches JSON files.

    Returns:
        df_matches: DataFrame containing match data with normalized fields.
        valid_ids: Set of valid match IDs.
    """
    matches = []
    for match_file in RAW_DIR.glob("matches/**/*.json"):
        with open(match_file) as f:
            matches += json.load(f)

    # Normalize only the top-level fields (including match_id)
    df_matches = pd.json_normalize(matches)

    return df_matches, set(df_matches["match_id"])

def load_events(valid_match_ids):
    """
    Load events from JSON files, filtering by valid match IDs.

    Args:
        valid_match_ids: Set of valid match IDs to filter events.

    Returns:
        df: DataFrame containing all events with match_id included.
    """
    event_rows = []
    for path in (RAW_DIR / "events").glob("*.json"):
        match_id = int(path.stem)
        if match_id not in valid_match_ids:
            continue
        with open(path) as f:
            events = json.load(f)
            for ev in events:
                ev["match_id"] = match_id
                event_rows.append(ev)
    return pd.json_normalize(event_rows)

def load_lineups(valid_match_ids):
    """
    Load lineups from JSON files, filtering by valid match IDs.

    Args:
        valid_match_ids: Set of valid match IDs to filter events.

    Returns:
        df: DataFrame containing all lineups with match_id included.
    """
    lineup_rows = []
    for path in (RAW_DIR / "lineups").glob("*.json"):
        match_id = int(path.stem)
        if match_id not in valid_match_ids:
            continue
        with open(path) as f:
            teams = json.load(f)
            for team in teams:
                if "team" not in team or "lineup" not in team:
                    continue

                team_id = team["team"]["id"]
                for player in team["lineup"]:
                    row = {
                        **player,
                        "match_id": match_id,
                        "team_id": team_id
                    }
                    lineup_rows.append(row)
    return pd.json_normalize(lineup_rows)

def write_parquet(df: pd.DataFrame, name: str, overwrite: bool = False):
    """
    Write a DataFrame to a Parquet file in the bronze layer.

    Args:
        df (pd.DataFrame): DataFrame to write.
        name (str): Name of the Parquet file (without extension).
        overwrite (bool, optional): Whether to overwrite existing file. Defaults to False.
    """
    path = Path(f"data/bronze/{name}.parquet")
    df.to_parquet(path, index=False)
    print(f"✅ {name}.parquet written.")

def bronze_ingest():
    """
    Ingests raw data into the bronze layer by loading matches, events, and lineups.
    """
    # TODO: Add a check to see if data is loaded correctly
    # TODO: Ingest three-sixty data if using freeze frames later
    print("→ Loading matches")
    df_matches, valid_ids = get_valid_match_ids()
    write_parquet(df_matches, "matches")

    print("→ Loading events")
    df_events = load_events(valid_ids)
    write_parquet(df_events, "events")

    print("→ Loading lineups")
    df_lineups = load_lineups(valid_ids)
    write_parquet(df_lineups, "lineups")
