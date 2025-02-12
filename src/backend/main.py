from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from src.backend.config.database import get_wave_analytics_db
from src.backend.config.settings import PROJECT_NAME, CORS_ORIGINS
from src.backend.models import WaveDataModel, StationModel, WaveData, StationData

load_dotenv()

# Initialize the app
app = FastAPI(
    title=PROJECT_NAME,
    description="API for Wave Consistency Analysis",
    version="1.0.0",
)

# CORS settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint that provides API information"""
    return {
        "name": PROJECT_NAME,
        "version": "1.0.0",
        "description": "Wave Consistency Analysis API",
        "docs_url": "/docs",
        "endpoints": {
            "wave_data": "/wave-data",
            "consistency": "/consistency",
        },
    }


@app.get("/api/v1/wave-data", response_model=list[WaveData])
async def get_wave_data(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    db: Session = Depends(get_wave_analytics_db),
):
    """Retrieve wave measurements from the localized_wave_data table."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail="Invalid date format. Use YYYY-MM-DD."
        ) from exc

    stmt = (
        select(WaveDataModel)
        .where(WaveDataModel.datetime.between(start_dt, end_dt))
        .order_by(WaveDataModel.datetime.desc())
        .limit(100)
    )

    result = db.execute(stmt).scalars().all()
    if not result:
        raise HTTPException(
            status_code=404, detail="No data found for the given date range."
        )

    return result


@app.get("/api/v1/stations", response_model=list[StationData])
async def get_stations(db: Session = Depends(get_wave_analytics_db)):
    """Retrieve all available stations."""
    stmt = select(StationModel)
    result = db.execute(stmt).scalars().all()

    if not result:
        raise HTTPException(status_code=404, detail="No stations found.")

    return result


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "1.0.0"}
