from pydantic import BaseModel


# Response model for consistency data
class ConsistencyData(BaseModel):
    station_id: str
    latitude: float
    longitude: float
    consistency_label: str
    consistency_score: float
