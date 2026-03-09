"""Configuration management using environment variables."""

import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    port: int = 8000
    database_path: str = "/app/data/sensors-python.db"
    api_token: str  # Required - no default
    log_level: str = "INFO"
    log_format: str = "json"
    seed_data_path: str = "/app/data/sensors.json"
    rabbitmq_url: str = "amqp://iot_service:iot_secret@rabbitmq:5672/"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
