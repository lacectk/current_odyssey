from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from pydantic import BaseModel, Field
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

API_VERSION = "1.0.0"


class ErrorResponse(BaseModel):
    """Base model for API error responses"""

    detail: str = Field(..., description="Human-readable error message")
    error_code: str = Field(..., description="Machine-readable error code")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    path: Optional[str] = Field(None, description="API endpoint path")
    additional_info: Optional[dict] = Field(
        None, description="Additional error context"
    )


class ValidationErrorResponse(ErrorResponse):
    """Model for validation errors"""

    invalid_fields: dict[str, str] = Field(
        ..., description="Dictionary of field names and their validation errors"
    )


class DatabaseErrorResponse(ErrorResponse):
    """Model for database-related errors"""

    retry_after: Optional[int] = Field(
        None, description="Seconds to wait before retrying the request"
    )


@app.get("/")
async def root():
    """Root endpoint that provides API information"""
    return {
        "name": PROJECT_NAME,
        "version": API_VERSION,
        "description": "Wave Consistency Analysis API",
        "docs_url": "/docs",
        "endpoints": {
            "wave_data": "/wave-data",
            "consistency": "/consistency",
        },
    }


@app.get(
    "/api/v1/wave-data",
    response_model=list[WaveData],
    responses={
        404: {"model": ErrorResponse, "description": "No data found"},
        400: {
            "model": ValidationErrorResponse,
            "description": "Invalid request parameters",
        },
        500: {"model": DatabaseErrorResponse, "description": "Internal server error"},
    },
)
async def get_wave_data(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    db: Session = Depends(get_wave_analytics_db),
):
    """
    Retrieve wave measurements for a specified date range.

    Raises:
        400: Invalid date format or range
        404: No data found
        500: Database error
    """
    try:
        start_dt, end_dt = validate_date_range(start_date, end_date)

        stmt = select(WaveDataModel).where(
            WaveDataModel.datetime.between(start_dt, end_dt)
        )

        if station_id:
            stmt = stmt.where(WaveDataModel.station_id == station_id)

        result = db.execute(stmt).scalars().all()

        if not result:
            raise HTTPException(
                status_code=404,
                detail=ErrorResponse(
                    detail=f"No wave data found between {start_date} and {end_date}",
                    error_code="DATA_NOT_FOUND",
                ).model_dump(),
            )

        return result

    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=500,
            detail=DatabaseErrorResponse(
                detail="Database error occurred",
                error_code="DATABASE_ERROR",
                retry_after=30,
                additional_info={"error_type": e.__class__.__name__},
            ).model_dump(),
        ) from e


@app.get("/api/v1/stations", response_model=list[StationData])
async def get_stations(db: Session = Depends(get_wave_analytics_db)):
    """Retrieve all available stations."""
    try:
        stmt = select(StationModel)
        result = db.execute(stmt).scalars().all()

        if not result:
            raise HTTPException(
                status_code=404,
                detail=ErrorResponse(
                    detail="No stations found", error_code="STATIONS_NOT_FOUND"
                ).model_dump(),
            )

        return result

    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=500,
            detail=DatabaseErrorResponse(
                detail="Database error occurred",
                error_code="DATABASE_ERROR",
                retry_after=30,
                additional_info={"error_type": e.__class__.__name__},
            ).model_dump(),
        ) from e

    except Exception as e:  # Handle unexpected exceptions
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                detail="An unexpected error occurred",
                error_code="INTERNAL_ERROR",
                additional_info={"error_type": e.__class__.__name__},
            ).model_dump(),
        ) from e


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": API_VERSION}


def validate_date_range(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if end_dt < start_dt:
            raise ValueError("End date must be after start date")

        max_range = timedelta(days=31)
        if (end_dt - start_dt) > max_range:
            raise ValueError(f"Date range cannot exceed {max_range.days} days")

        # Ensure dates aren't in the future
        if end_dt > datetime.now(timezone.utc) or start_dt > datetime.now(timezone.utc):
            raise ValueError("End date cannot be in the future")

        return start_dt, end_dt
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=ValidationErrorResponse(
                detail=str(exc),
                error_code="INVALID_DATE_RANGE",
                invalid_fields={
                    "format": "YYYY-MM-DD",
                    "example": datetime.now().strftime("%Y-%m-%d"),
                },
            ).model_dump(),
        ) from exc


# Example middleware to add request path to error responses
@app.middleware("http")
async def add_error_context(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except HTTPException as exc:
        if isinstance(exc.detail, dict) and "path" not in exc.detail:
            exc.detail["path"] = request.url.path
        raise
