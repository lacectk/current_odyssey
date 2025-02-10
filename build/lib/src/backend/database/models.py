from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Float, DateTime, MetaData, text
from datetime import datetime

# Create base class for declarative models with schema
Base = declarative_base(metadata=MetaData(schema="raw_data"))


class Station(Base):
    __tablename__ = "stations"

    station_id = Column(String(10), primary_key=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(
        DateTime, server_default=text("CURRENT_TIMESTAMP"), onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<Station(id={self.station_id}, lat={self.latitude}, lon={self.longitude})>"
