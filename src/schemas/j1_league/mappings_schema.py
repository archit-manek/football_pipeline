import polars as pl

# J1 League mapping schemas - based on CSV mapping files

# Matches mapping schema
J1_LEAGUE_MATCHES_MAPPING_SCHEMA = {
    "statsbomb_id": pl.Int64,
    "wyscout_id": pl.Int64,
}

# Teams mapping schema
J1_LEAGUE_TEAMS_MAPPING_SCHEMA = {
    "statsbomb_id": pl.Int64,
    "wyscout_id": pl.Int64,
}

# Players mapping schema
J1_LEAGUE_PLAYERS_MAPPING_SCHEMA = {
    "statsbomb_id": pl.Int64,
    "wyscout_id": pl.Int64,
} 