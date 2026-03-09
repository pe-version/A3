"""Configuration management using environment variables."""

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    port: int = 8001
    database_path: str = "/app/data/alerts-python.db"
    api_token: str  # Required - no default
    log_level: str = "INFO"
    log_format: str = "json"
    seed_data_path: str = "/app/data/alert_rules.json"
    sensor_service_url: str = "http://python-service:8000"
    rabbitmq_url: str = "amqp://iot_service:iot_secret@rabbitmq:5672/"
    cb_fail_max: int = 5
    cb_reset_timeout: int = 30

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
