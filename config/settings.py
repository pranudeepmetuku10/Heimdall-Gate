"""Process-wide configuration.

All runtime configuration is loaded from environment variables (and, optionally,
a `.env` file in the project root). The `Settings` object is created exactly
once per process via `get_settings()` and is immutable afterwards. Tests pass
their own `Settings` instance into the components that need it rather than
mutating environment variables.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )

    # --- Yelp ---
    yelp_api_key: str = Field(default="", alias="YELP_API_KEY")
    yelp_base_url: str = Field(
        default="https://api.yelp.com/v3",
        alias="YELP_BASE_URL",
    )
    yelp_request_timeout_sec: float = Field(
        default=10.0, alias="YELP_REQUEST_TIMEOUT_SEC"
    )

    # --- Ingestion targeting ---
    cities: list[str] = Field(
        default_factory=lambda: ["Boston", "Seattle", "Austin"],
        alias="HEIMDALL_CITIES",
    )
    terms: list[str] = Field(
        default_factory=lambda: ["restaurants"], alias="HEIMDALL_TERMS"
    )
    page_size: int = Field(default=50, ge=1, le=50, alias="HEIMDALL_PAGE_SIZE")
    max_pages_per_city: int = Field(
        default=4, ge=1, le=20, alias="HEIMDALL_MAX_PAGES_PER_CITY"
    )
    poll_interval_sec: int = Field(
        default=900, ge=60, alias="HEIMDALL_POLL_INTERVAL_SEC"
    )
    api_call_budget: int = Field(
        default=2000, ge=1, alias="HEIMDALL_API_CALL_BUDGET"
    )

    # --- Kafka ---
    kafka_bootstrap_servers: str = Field(
        default="localhost:9094", alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    kafka_topic_raw: str = Field(
        default="heimdall.raw.business", alias="KAFKA_TOPIC_RAW"
    )
    kafka_topic_dlq: str = Field(
        default="heimdall.dlq.ingest", alias="KAFKA_TOPIC_DLQ"
    )
    kafka_security_protocol: Literal[
        "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"
    ] = Field(default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] = Field(
        default="PLAIN", alias="KAFKA_SASL_MECHANISM"
    )
    kafka_sasl_username: str = Field(default="", alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: str = Field(default="", alias="KAFKA_SASL_PASSWORD")
    kafka_linger_ms: int = Field(default=20, ge=0, alias="KAFKA_LINGER_MS")
    kafka_batch_size: int = Field(default=65536, ge=1024, alias="KAFKA_BATCH_SIZE")
    kafka_compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = Field(
        default="lz4", alias="KAFKA_COMPRESSION_TYPE"
    )
    kafka_enable_idempotence: bool = Field(
        default=True, alias="KAFKA_ENABLE_IDEMPOTENCE"
    )
    kafka_acks: Literal["0", "1", "all"] = Field(default="all", alias="KAFKA_ACKS")
    kafka_max_in_flight: int = Field(default=5, ge=1, alias="KAFKA_MAX_IN_FLIGHT")

    # --- File sink (volume-source mode) ---
    file_sink_enabled: bool = Field(
        default=False, alias="HEIMDALL_FILE_SINK_ENABLED"
    )
    file_sink_path: str = Field(
        default="./out/raw_drop", alias="HEIMDALL_FILE_SINK_PATH"
    )

    # --- Logging ---
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", alias="HEIMDALL_LOG_LEVEL"
    )
    log_format: Literal["json", "console"] = Field(
        default="console", alias="HEIMDALL_LOG_FORMAT"
    )

    # --- Identity ---
    producer_id: str = Field(
        default="heimdall-ingest-local", alias="HEIMDALL_PRODUCER_ID"
    )

    @field_validator("cities", "terms", mode="before")
    @classmethod
    def _split_csv(cls, v: object) -> object:
        if isinstance(v, str):
            return [piece.strip() for piece in v.split(",") if piece.strip()]
        return v

    @property
    def kafka_enabled(self) -> bool:
        return bool(self.kafka_bootstrap_servers)

    def require_yelp_key(self) -> None:
        if not self.yelp_api_key:
            raise RuntimeError(
                "YELP_API_KEY is not set. Get a key at "
                "https://docs.developer.yelp.com/docs/fusion-intro and "
                "populate .env."
            )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached process-wide settings.

    Test code should construct `Settings(...)` directly instead of calling this.
    """
    return Settings()
