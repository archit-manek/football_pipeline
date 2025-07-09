import pandera as pa
from pandera import DataFrameSchema, Column

lineups_schema = DataFrameSchema({
    "player_id": Column(pa.Int64, nullable=True),
    "player_name": Column(pa.String, nullable=True),
    "player_nickname": Column(pa.String, nullable=True),
    "jersey_number": Column(pa.Int64, nullable=True),
    "match_id": Column(pa.Int64, nullable=True),
    "team_id": Column(pa.Int64, nullable=True),
    "country_id": Column(pa.Int64, nullable=True),
    "country_name": Column(pa.String, nullable=True),
    "cards": Column(pa.Object, nullable=True),  # List type for cards (can be empty list or list of card objects)
    "cards_time": Column(pa.String, nullable=True),
    "cards_card_type": Column(pa.String, nullable=True),
    "cards_reason": Column(pa.String, nullable=True),
    "cards_period": Column(pa.Int64, nullable=True),
    "positions_position_id": Column(pa.Int64, nullable=True),
    "positions_position": Column(pa.String, nullable=True),
    "positions_from": Column(pa.String, nullable=True),
    "positions_to": Column(pa.String, nullable=True),
    "positions_from_period": Column(pa.Int64, nullable=True),
    "positions_to_period": Column(pa.Int64, nullable=True),
    "positions_start_reason": Column(pa.String, nullable=True),
    "positions_end_reason": Column(pa.String, nullable=True),
})