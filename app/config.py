from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, loaded exclusively from environment variables.

    No secrets are hardcoded. See `.env.example` for the supported keys.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # SQLAlchemy URL for THIS service's own database. It must never point at the
    # legacy monolith database.
    database_url: str = "postgresql+psycopg://user_service:user_service@localhost:5433/user_service"

    app_env: str = "development"
    log_level: str = "INFO"
    service_name: str = "user-service"

    # Graceful shutdown window (seconds) advertised to uvicorn / orchestrators.
    graceful_shutdown_seconds: int = 20

    # --- CDC consumer (app/cdc) ------------------------------------------
    # Runs as a separate process from the FastAPI server; see app/cdc/__main__.py.
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_users_topic: str = "legacy.public.users"
    kafka_consumer_group: str = "user-service-cdc"
    kafka_auto_offset_reset: str = "earliest"
    cdc_metrics_port: int = 9200


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
