# backend/settings.py
from pydantic import BaseSettings

class Settings(BaseSettings):
    MAIL_USERNAME: str
    MAIL_PASSWORD: str
    MAIL_FROM: str
    MAIL_PORT: int = 587
    MAIL_SERVER: str = "smtp.gmail.com"
    MAIL_TLS: bool = True
    MAIL_SSL: bool = False
    USE_CREDENTIALS: bool = True

    GOOGLE_APPLICATION_CREDENTIALS: str
    DOWNLOAD_FOLDER: str

    API_HOST: str = "127.0.0.1"
    API_PORT: int = 8000
    KAFKA_BROKER: str = "localhost:9092"
    REDIS_URL: str = "redis://localhost:6379"

    class Config:
        env_file = "../.env"   # points to the root .env

settings = Settings()
