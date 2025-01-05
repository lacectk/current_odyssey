from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.backend.config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

# Database URLs
WAVE_CONSISTENCY_DB_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/wave_consistency"
)
LOCALIZED_WAVE_DB_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/localized_wave_data"
)

# Create engines
wave_consistency_engine = create_engine(WAVE_CONSISTENCY_DB_URL)
localized_wave_engine = create_engine(LOCALIZED_WAVE_DB_URL)

# SessionLocal classes
WaveConsistencySessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=wave_consistency_engine
)
LocalizedWaveSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=localized_wave_engine
)


def get_wave_consistency_db():
    db = WaveConsistencySessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_localized_wave_db():
    db = LocalizedWaveSessionLocal()
    try:
        yield db
    finally:
        db.close()
