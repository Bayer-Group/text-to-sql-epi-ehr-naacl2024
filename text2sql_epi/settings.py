from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings for API server."""

    model_config = SettingsConfigDict(env_file=(".env", ".env.local"), extra="ignore")

    SNOWFLAKE_USER: str
    SNOWFLAKE_PASSWORD: str
    SNOWFLAKE_ACCOUNT_IDENTIFIER: str
    SNOWFLAKE_WAREHOUSE: str
    SNOWFLAKE_DATABASE: str
    SNOWFLAKE_TIMEOUT: int = 120
    OPENAI_API_KEY: str
    OPENAI_API_VERSION: str
    OPENAI_API_BASE: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str
    AZURE_TENANT_ID: str


settings = Settings()
