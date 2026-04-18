from functools import lru_cache
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Make env loading stable regardless of current working directory.
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parents[1] / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_user: str = Field(default="carwash", alias="POSTGRES_USER")
    postgres_password: str = Field(default="carwash", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="carwash", alias="POSTGRES_DB")
    database_url: str | None = Field(default=None, alias="DATABASE_URL")
    # e.g. require | verify-full. If unset and host looks like Azure PostgreSQL, "require" is applied automatically.
    postgres_sslmode: Optional[str] = Field(default=None, alias="POSTGRES_SSLMODE")

    # Required — no in-code defaults; set ADMIN_ID or ADMIN_USERNAME plus ADMIN_PASSWORD in .env / environment.
    admin_username: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("ADMIN_USERNAME", "ADMIN_ID"),
    )
    admin_password: str = Field(..., min_length=1, alias="ADMIN_PASSWORD")

    jwt_secret_key: str = Field(default="dev-secret-change-in-production", alias="JWT_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    access_token_expire_minutes: int = Field(default=60 * 24, alias="ACCESS_TOKEN_EXPIRE_MINUTES")

    cors_origins: str = Field(
        default="http://localhost:5173,http://localhost:5174",
        alias="CORS_ORIGINS",
    )
    # When true, allow any http(s)://localhost or 127.0.0.1 with any port (USER app may not be 5173).
    cors_allow_localhost_regex: bool = Field(default=True, alias="CORS_ALLOW_LOCALHOST_REGEX")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    def _is_azure_postgres_host(self) -> bool:
        h = (self.postgres_host or "").lower()
        u = (self.database_url or "").lower()
        return "database.azure.com" in h or "database.azure.com" in u

    def _effective_sslmode(self) -> str | None:
        if self.postgres_sslmode and self.postgres_sslmode.strip():
            return self.postgres_sslmode.strip()
        if self._is_azure_postgres_host():
            return "require"
        return None

    @staticmethod
    def _append_sslmode(url: str, sslmode: str | None) -> str:
        if not sslmode or "sslmode=" in url.lower():
            return url
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}sslmode={quote_plus(sslmode, safe='')}"

    def sqlalchemy_database_uri(self) -> str:
        if self.database_url:
            url = self.database_url
        else:
            # User/password must be URL-encoded (e.g. passwords containing "@").
            user = quote_plus(self.postgres_user, safe="")
            password = quote_plus(self.postgres_password, safe="")
            url = (
                f"postgresql+psycopg2://{user}:{password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )
        return self._append_sslmode(url, self._effective_sslmode())

    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
