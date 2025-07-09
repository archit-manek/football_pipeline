from pandera import DataFrameSchema, Column
import pandera as pa

_360_schema = DataFrameSchema({
    "event_uuid": Column(pa.String, nullable=True),
    "visible_area": Column(pa.Object, nullable=True),  # List type
    "freeze_frame_teammate": Column(pa.Bool, nullable=True),
    "freeze_frame_actor": Column(pa.Bool, nullable=True),
    "freeze_frame_keeper": Column(pa.Bool, nullable=True),
    "freeze_frame_location": Column(pa.Object, nullable=True),  # List type
})