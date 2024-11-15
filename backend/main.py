from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from sqlalchemy.orm import Session

from backend.config.settings import PROJECT_NAME, CORS_ORIGINS
from backend.config.database import get_wave_consistency_db

# Initialize the app
app = FastAPI(title=PROJECT_NAME)

# CORS settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Response model for consistency data
class ConsistencyData(BaseModel):
    station_id: str
    latitude: float
    longitude: float
    consistency_label: str
    consistency_score: float


@app.get("/consistency", response_model=list[ConsistencyData])
async def get_consistency(
    start_month: str = Query(..., description="Start month in YYYY-MM format"),
    end_month: str = Query(..., description="End month in YYYY-MM format"),
    db: Session = Depends(get_wave_consistency_db),
):
    """Retrieve consistency data for the specified month range."""
    # Validate month format and convert to datetime
    try:
        start_month_dt = pd.to_datetime(start_month, format="%Y-%m")
        end_month_dt = pd.to_datetime(end_month, format="%Y-%m")
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid month format. Use YYYY-MM."
        )

    # Ensure the start month is before the end month
    if start_month_dt > end_month_dt:
        raise HTTPException(
            status_code=400, detail="Start month must be before end month."
        )

    # Query the wave consistency database for the specified month range
    query = f"""
        SELECT station_id, month, latitude, longitude, consistency_label, consistency_score
        FROM wave_consistency_trends
        WHERE month >= '{start_month_dt.strftime('%Y-%m-01')}'
        AND month <= '{end_month_dt.strftime('%Y-%m-01')}'
        AND consistency_score IS NOT NULL
        AND consistency_score != 'NaN'
        AND consistency_score != 'Infinity'
        AND consistency_score != '-Infinity'
    """

    df = pd.read_sql(query, db.bind)

    if df.empty:
        raise HTTPException(
            status_code=404, detail="No data found for the given month range."
        )

    # Clean the dataframe before converting to response
    df = df.replace([float("inf"), float("-inf")], None)  # Replace infinity with None
    df = df.dropna(subset=["consistency_score"])  # Remove rows with NaN scores

    # Format and return the response
    return [
        ConsistencyData(
            station_id=row["station_id"],
            latitude=float(row["latitude"]),  # Ensure proper float conversion
            longitude=float(row["longitude"]),  # Ensure proper float conversion
            consistency_label=str(row["consistency_label"]),  # Ensure string conversion
            consistency_score=float(
                row["consistency_score"]
            ),  # Ensure proper float conversion
        )
        for _, row in df.iterrows()
    ]
