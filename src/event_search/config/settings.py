from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class AzureSettings(BaseModel):
    container_url: str
    sas_token: SecretStr
    folder_name: str = ""

    @field_validator("container_url")
    @classmethod
    def validate_container_url(cls, value: str) -> str:
        value = value.rstrip("/")
        parsed = urlsplit(value)

        if parsed.scheme != "https":
            raise ValueError("Azure container URL must use HTTPS")

        if not parsed.netloc:
            raise ValueError("Invalid Azure container URL")

        if not parsed.path.strip("/"):
            raise ValueError("Azure container URL must contain a container name")

        if parsed.query:
            raise ValueError("container_url must not contain SAS parameters; configure sas_token separately")

        return value

    @model_validator(mode="after")
    def validate_sas_token(self) -> "AzureSettings":
        token = self.sas_token.get_secret_value().lstrip("?")

        params = parse_qs(
            token,
            keep_blank_values=True,
        )

        resource = params.get("sr", [None])[0]

        if resource != "c":
            raise ValueError("Azure SAS must target a container: sr=c")

        permissions = set(params.get("sp", [""])[0])

        missing = {"r", "l"} - permissions

        if missing:
            raise ValueError("Azure SAS must contain read and list permissions: sp=rl")

        protocol = params.get("spr", [None])[0]

        if protocol != "https":
            raise ValueError("Azure SAS must restrict access to HTTPS: spr=https")

        if not params.get("se"):
            raise ValueError("Azure SAS must contain expiration parameter: se")

        if not params.get("sig"):
            raise ValueError("Azure SAS must contain signature parameter: sig")

        return self


class CacheSettings(BaseModel):
    parquet_dir: Path = Path(".cache/parquet")
    database_path: Path = Path(".cache/event_search.duckdb")
    temp_dir: Path = Path(".cache/tmp")


class SearchSettings(BaseModel):
    default_limit: int = Field(
        default=100,
        ge=1,
    )

    max_limit: int = Field(
        default=10_000,
        ge=1,
    )


class SyncSettings(BaseModel):
    concurrency: int = 20
    target_parquet_size_mb: int = 128


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="EVENT_SEARCH_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    azure: AzureSettings
    cache: CacheSettings = Field(default_factory=CacheSettings)
    search: SearchSettings = Field(default_factory=SearchSettings)
    sync: SyncSettings
