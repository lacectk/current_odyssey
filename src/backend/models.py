from datetime import datetime
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, Float, DateTime
from pydantic import BaseModel, ConfigDict
from typing import Optional


class Base(DeclarativeBase):
    """Custom SQLAlchemy base class with schema enforcement."""

    __table_args__ = {"schema": "raw_data"}


class WaveDataModel(Base):
    """SQLAlchemy model for wave measurements with timestamps and soft delete."""

    __tablename__ = "localized_wave_data"

    station_id = Column(String, primary_key=True)
    datetime = Column(DateTime, primary_key=True, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    wave_height = Column(Float, nullable=True, name="wave_height_wvht")
    dominant_period = Column(Float, nullable=True, name="dominant_period_dpd")
    average_period = Column(Float, nullable=True, name="average_period_apd")
    mean_wave_direction = Column(Float, nullable=True, name="mean_wave_direction_mwd")


class StationModel(Base):
    """SQLAlchemy model for stations with timestamps."""

    __tablename__ = "stations"

    station_id = Column(String, primary_key=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)


# 🔹 Pydantic Model for API Validation - Wave Data
class WaveData(BaseModel):
    """Pydantic model for wave measurements."""

    station_id: str
    datetime: datetime
    latitude: float
    longitude: float
    wave_height: Optional[float] = None
    dominant_period: Optional[float] = None
    average_period: Optional[float] = None
    mean_wave_direction: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class StationData(BaseModel):
    """Pydantic model for stations."""

    station_id: str
    latitude: float
    longitude: float

    model_config = ConfigDict(from_attributes=True)
