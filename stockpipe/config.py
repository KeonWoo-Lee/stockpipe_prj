from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "STOCKPIPE_", "env_file": ".env"}

    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "stockpipe"
    db_password: str = "stockpipe"
    db_name: str = "stockpipe"
    LOOKBACK_DAYS: int = 365

    STOCK_CODES: dict = {
        "047040": "대우건설",
        "397030": "에이프릴바이오",
        "005930": "삼성전자",
    }

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
