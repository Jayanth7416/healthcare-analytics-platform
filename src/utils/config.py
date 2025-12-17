"""Application Configuration"""

from pydantic_settings import BaseSettings
from typing import List, Optional
from functools import lru_cache


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables

    All sensitive values should be provided via environment
    variables or AWS Secrets Manager in production.
    """

    # Application
    APP_NAME: str = "Healthcare Analytics Platform"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "development"

    # API
    API_KEY_HEADER: str = "X-API-Key"
    ALLOWED_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8080"]

    # AWS
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None

    # Kinesis
    KINESIS_STREAM_NAME: str = "healthcare-events"
    KINESIS_DLQ_STREAM_NAME: str = "healthcare-events-dlq"
    ENABLE_KINESIS_CONSUMER: bool = False

    # KMS
    KMS_KEY_ID: str = "alias/healthcare-analytics-key"

    # Database
    DATABASE_URL: str = "postgresql://localhost:5432/healthcare_analytics"
    REDSHIFT_HOST: Optional[str] = None
    REDSHIFT_PORT: int = 5439
    REDSHIFT_DB: str = "analytics"
    REDSHIFT_USER: Optional[str] = None
    REDSHIFT_PASSWORD: Optional[str] = None

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Encryption (for local development only)
    ENCRYPTION_SECRET: str = "dev-secret-key-change-in-production"
    ENCRYPTION_SALT: str = "dev-salt-change-in-production"

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


settings = get_settings()
