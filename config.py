from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://maritime:maritime_pass@localhost:5432/maritime_awareness"

    @field_validator("database_url", mode="before")
    @classmethod
    def normalise_db_url(cls, v: str) -> str:
        """Accept Railway/Supabase postgres:// URLs and convert to asyncpg dialect."""
        if v.startswith("postgres://"):
            v = "postgresql+asyncpg://" + v[len("postgres://"):]
        elif v.startswith("postgresql://") and "+asyncpg" not in v:
            v = "postgresql+asyncpg://" + v[len("postgresql://"):]
        # Strip pgbouncer parameter — asyncpg handles it natively
        v = v.replace("?pgbouncer=true", "").replace("&pgbouncer=true", "")
        return v
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = False
    sql_echo: bool = False

    # Anthropic
    anthropic_api_key: str = ""

    # AISStream — register free at https://aisstream.io
    aisstream_api_key: str = ""
    aisstream_url: str = "wss://stream.aisstream.io/v0/stream"

    # Seconds between DB flush cycles while messages are accumulating
    ais_batch_interval: int = 5

    # Max records to buffer before forcing an early flush
    ais_batch_max: int = 500

    # Detection engine
    detection_interval_seconds: int = 300   # run detectors every 5 minutes

    # Dark vessel thresholds
    dark_vessel_silence_minutes:     int = 60  # silent for this long → dark (1 hour)
    dark_vessel_active_window_hours: int = 6  # vessel must have track points within this window
    dark_vessel_min_active_points:   int = 10 # minimum recent track points to prove active transmission

    # Loitering thresholds
    loitering_window_hours:          float = 2.0
    loitering_min_duration_minutes:  float = 30.0
    loitering_max_displacement_nm:   float = 1.0
    loitering_max_avg_sog:           float = 0.5

    class Config:
        env_file = ".env"


settings = Settings()
