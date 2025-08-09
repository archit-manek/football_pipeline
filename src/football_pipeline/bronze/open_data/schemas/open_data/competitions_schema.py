import polars as pl

COMPETITIONS_SCHEMA = {
    "competition_id":           pl.Int64,
    "season_id":                pl.Int64,
    "country_name":             pl.Utf8,
    "competition_name":         pl.Utf8,
    "competition_gender":       pl.Utf8,
    "competition_youth":        pl.Boolean,
    "competition_international":pl.Boolean,
    "season_name":              pl.Utf8,
    "match_updated":            pl.Utf8,
    "match_updated_360":        pl.Utf8,
    "match_available_360":      pl.Utf8,
    "match_available":          pl.Utf8,
}
