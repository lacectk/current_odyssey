"""Configuration settings for the Wave Consistency API."""

import os
from dotenv import load_dotenv

load_dotenv()

# API Settings
API_V1_STR = "/api/v1"
PROJECT_NAME = "Wave Consistency API"

# CORS Settings
CORS_ORIGINS = [
    "http://localhost:3000",  # React dev server
    "http://localhost:8000",  # FastAPI dev server
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

EMAIL_NOTIFICATION = {
    "smtp_server": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("SMTP_PORT")),
    "sender_email": os.getenv("SENDER_EMAIL"),
    "sender_password": os.getenv("SENDER_PASSWORD"),
    "recipient_emails": os.getenv("RECIPIENT_EMAILS", "").split(","),
}
