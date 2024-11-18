from dotenv import load_dotenv
import os

load_dotenv()

# API Settings
API_V1_STR = "/api/v1"
PROJECT_NAME = "Wave Consistency API"

# CORS Settings
CORS_ORIGINS = [
    "http://localhost:3000",  # React default port
    "http://localhost:8000",  # FastAPI default port
]


# Environment Settings
class Environment:
    DEV = "development"
    PROD = "production"
    TEST = "testing"


# Current environment
ENVIRONMENT = os.getenv("ENVIRONMENT", Environment.DEV)

# Database Settings
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
