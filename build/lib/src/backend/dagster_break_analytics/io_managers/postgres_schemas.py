from sqlalchemy.types import String, Float, DateTime

# Define dtypes for each asset/table
ASSET_DTYPES = {
    "raw_buoy_data": {
        "station_id": String,
        "datetime": DateTime,
        "latitude": Float,
        "longitude": Float,
        "wave_height": Float,
        "wave_period": Float,
        "wave_direction": Float,
        "avg_wave_period": Float,
        "_partition_key": String,
        "_updated_at": DateTime,
        "_asset_partition": String,
    },
}
