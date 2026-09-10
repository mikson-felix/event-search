from pathlib import Path

from pydantic import BaseModel, SecretStr
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class AzureSettings(BaseModel):
    container_url: str
    sas_token: SecretStr
    folder_name: str = ""


class CacheSettings(BaseModel):
    parquet_dir: Path = Path(".cache/parquet")

    database_path: Path = Path(".cache/event_search.sqlite")

    temp_dir: Path = Path(".cache/tmp")


class SearchSettings(BaseModel):
    default_limit: int = 100
    max_limit: int = 10_000


class SyncSettings(BaseModel):
    concurrency: int = 4
    target_parquet_size_mb: int = 128


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EVENT_SEARCH_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    azure: AzureSettings

    cache: CacheSettings = CacheSettings()
    search: SearchSettings = SearchSettings()
    sync: SyncSettings = SyncSettings()
