from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://maritime:maritime_pass@localhost:5432/maritime_awareness"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = False

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
    dark_vessel_silence_minutes: int   = 120   # silent for this long → dark
    dark_vessel_lookback_hours:  int   = 24    # only flag if seen within this window

    # Loitering thresholds
    loitering_window_hours:          float = 3.0
    loitering_min_duration_minutes:  float = 60.0
    loitering_max_displacement_nm:   float = 2.0
    loitering_max_avg_sog:           float = 1.5

    class Config:
        env_file = ".env"


settings = Settings()
