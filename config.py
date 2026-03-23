from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://maritime:maritime_pass@localhost:5432/maritime_awareness"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = False

    # AISHub — register free at https://www.aishub.net/join-us
    aishub_username: str = ""
    aishub_url: str = "http://data.aishub.net/ws.php"

    # Polling interval in seconds
    ais_poll_interval: int = 60

    class Config:
        env_file = ".env"


settings = Settings()
