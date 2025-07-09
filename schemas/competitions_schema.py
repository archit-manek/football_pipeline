import pandera as pa
from pandera import DataFrameSchema, Column

competitions_schema = DataFrameSchema({
    "competition_id": Column(pa.Int64, nullable=True),
    "season_id": Column(pa.Int64, nullable=True),
    "country_name": Column(pa.String, nullable=True),
    "competition_name": Column(pa.String, nullable=True),
    "competition_gender": Column(pa.String, nullable=True),
    "competition_youth": Column(pa.Bool, nullable=True),
    "competition_international": Column(pa.Bool, nullable=True),
    "season_name": Column(pa.String, nullable=True),
    "match_updated": Column(pa.Object, nullable=True),  # Datetime after processing
    "match_updated_360": Column(pa.String, nullable=True),
    "match_available_360": Column(pa.String, nullable=True),
    "match_available": Column(pa.Object, nullable=True),  # Datetime after processing
})