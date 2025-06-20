import os
import shutil
import json
from pathlib import Path
import pandas as pd

# Paths
SRC_DIR = Path("/Users/architmanek/Desktop/DataEngineering/open-data/data")
DST_DIR = Path("data/bronze")

# Folders to copy
FOLDERS = ["matches", "events", "lineups", "three-sixty"]

def copy_files():
    # Copy competitions.json
    shutil.copy(SRC_DIR / "competitions.json", DST_DIR / "competitions.json")

    # Copy each folder
    for folder in FOLDERS:
        src_folder = SRC_DIR / folder
        dst_folder = DST_DIR / folder
        if not dst_folder.exists():
            dst_folder.mkdir(parents=True)

        for file in src_folder.glob("**/*.json"):
            dest_path = dst_folder / file.name
            shutil.copy(file, dest_path)

    print("Bronze layer populated.")

def build_match_index():
    matches_dir = DST_DIR / "matches"
    rows = []
    for match_file in matches_dir.glob("*.json"):
        with open(match_file, "r") as f:
            matches = json.load(f)
            for m in matches:
                rows.append({
                    "match_id": m["match_id"],
                    "competition": m["competition"]["competition_name"],
                    "season": m["season"]["season_name"],
                    "match_date": m["match_date"],
                    "home_team": m["home_team"]["home_team_name"],
                    "away_team": m["away_team"]["away_team_name"]
                })

    
    df = pd.DataFrame(rows)
    df.to_csv(DST_DIR / "match_index.csv", index=False)
    print(f"match_index.csv created with {len(df)} matches.")

if __name__ == "__main__":
    copy_files()
    build_match_index()
