import polars as pl

# J1 League physical data schema - based on HUDL structure
J1_LEAGUE_PHYSICAL_SCHEMA = {
    "matchId": pl.String,
    "label": pl.String,
    "dateutc": pl.String,
    "teamId": pl.String,
    "teamName": pl.String,
    "playerid": pl.String,
    "player": pl.String,
    "metric": pl.String,
    "phase": pl.String,
    "value": pl.Float64,
} 