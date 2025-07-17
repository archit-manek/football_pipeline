import polars as pl

SCHEMA_360 = {
    "event_uuid": pl.String,
    "visible_area": pl.List,
    "freeze_frame_teammate": pl.Boolean,
    "freeze_frame_actor": pl.Boolean,
    "freeze_frame_keeper": pl.Boolean,
    "freeze_frame_location": pl.String,
}