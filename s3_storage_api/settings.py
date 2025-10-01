from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="")

    constellation_api_key: str = "constellation_api_key"

    bittensor_network: str = Field(
        "finney",
        alias="BT_NETWORK",
        description="Name of the Bittensor network (finney/mainnet)",
    )

    netuid: int = Field(13, alias="NET_UID", description="Subnet uid")

    allowed_clock_skew_ms: int = 8000  # replay window

    metagraph_sync_interval: int = 60

    min_validator_stake: int = 20_000

    redis_url: str = "redis://localhost:6379/0"

    disable_rate_limiting: bool = False


    s3_bucket : str = "data-universe-storage"
    s3_region: str = "nyc3"
    s3_endpoint: str = Field("https://localhost:9000", env="S3_ENDPOINT")

    aws_access_key: str = Field(default="aws_access_key", alias="DO_SPACES_KEY")
    aws_secret_key: str = Field(default="aws_secret_key", alias="DO_SPACES_SECRET")

    api_host: str = Field("0.0.0.0", alias='HOST')
    api_port: int = Field(8051, alias='PORT')

    postgres_dsn: str = "postgresql+asyncpg://user:pass@localhost:5432/mydb"

    metrics_api_key: str = "metrics_api_key"


settings = ApiSettings()
