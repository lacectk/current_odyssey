from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.backend.config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

# Database URLs
WAVE_ANALYTICS_DB_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/wave_analytics"
)

# Create engines
wave_analytics_engine = create_engine(WAVE_ANALYTICS_DB_URL)

# SessionLocal classes
WaveAnalyticsSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=wave_analytics_engine
)


def get_wave_analytics_db():
    db = WaveAnalyticsSessionLocal()
    try:
        yield db
    finally:
        db.close()
