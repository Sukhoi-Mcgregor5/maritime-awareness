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

    class Config:
        env_file = ".env"


settings = Settings()
