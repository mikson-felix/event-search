from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import event_search.bootstrap as bootstrap
from event_search.application.search_service import SearchService
from event_search.application.sync_service import SyncService
from event_search.application.time import (
    BlobPartitionResolver,
    TimeRangeResolver,
)
from event_search.bootstrap import Application


class FakeSecret:
    def __init__(
        self,
        value: str,
    ) -> None:
        self._value = value

    def get_secret_value(
        self,
    ) -> str:
        return self._value


@pytest.fixture
def fake_settings(
    tmp_path: Path,
) -> SimpleNamespace:
    return SimpleNamespace(
        azure=SimpleNamespace(
            container_url="https://example.blob.core.windows.net/events",
            sas_token=FakeSecret("test-sas-token"),
            folder_name="test-folder",
        ),
        cache=SimpleNamespace(
            database_path=tmp_path / "event-search.duckdb",
            parquet_dir=tmp_path / "parquet",
            temp_dir=tmp_path / "tmp",
        ),
    )


def test_build_application_creates_application(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(
        bootstrap,
        "Settings",
        lambda: fake_settings,
    )

    blob_source = MagicMock()
    manifest = MagicMock()
    result_store = MagicMock()
    materializer = MagicMock()
    query_engine = MagicMock()
    details_reader = MagicMock()
    renderer = MagicMock()

    monkeypatch.setattr(
        bootstrap,
        "AzureBlobSource",
        lambda **kwargs: blob_source,
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBManifestRepository",
        lambda database_path: manifest,
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBSearchResultStore",
        lambda database_path: result_store,
    )

    monkeypatch.setattr(
        bootstrap,
        "ParquetMaterializer",
        lambda parquet_root: materializer,
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBQueryEngine",
        lambda parquet_root: query_engine,
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBParquetEventDetailsReader",
        lambda: details_reader,
    )

    monkeypatch.setattr(
        bootstrap,
        "ConsoleRenderer",
        lambda: renderer,
    )

    application = bootstrap.build_application()

    assert isinstance(
        application,
        Application,
    )

    assert application.settings is fake_settings
    assert application.manifest is manifest
    assert application.renderer is renderer

    assert isinstance(
        application.time_range_resolver,
        TimeRangeResolver,
    )

    assert isinstance(
        application.partition_resolver,
        BlobPartitionResolver,
    )

    assert isinstance(
        application.sync_service,
        SyncService,
    )

    assert isinstance(
        application.search_service,
        SearchService,
    )


def test_build_application_creates_azure_blob_source_from_settings(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(
        bootstrap,
        "Settings",
        lambda: fake_settings,
    )

    azure_blob_source = MagicMock(
        return_value=MagicMock(),
    )

    monkeypatch.setattr(
        bootstrap,
        "AzureBlobSource",
        azure_blob_source,
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBManifestRepository",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBSearchResultStore",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ParquetMaterializer",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBQueryEngine",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBParquetEventDetailsReader",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ConsoleRenderer",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    bootstrap.build_application()

    azure_blob_source.assert_called_once_with(
        container_url=fake_settings.azure.container_url,
        sas_token="test-sas-token",
        folder_name="test-folder",
    )


def test_build_application_uses_cache_paths_from_settings(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(
        bootstrap,
        "Settings",
        lambda: fake_settings,
    )

    manifest_factory = MagicMock(
        return_value=MagicMock(),
    )

    result_store_factory = MagicMock(
        return_value=MagicMock(),
    )

    materializer_factory = MagicMock(
        return_value=MagicMock(),
    )

    query_engine_factory = MagicMock(
        return_value=MagicMock(),
    )

    monkeypatch.setattr(
        bootstrap,
        "AzureBlobSource",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBManifestRepository",
        manifest_factory,
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBSearchResultStore",
        result_store_factory,
    )

    monkeypatch.setattr(
        bootstrap,
        "ParquetMaterializer",
        materializer_factory,
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBQueryEngine",
        query_engine_factory,
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBParquetEventDetailsReader",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ConsoleRenderer",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    bootstrap.build_application()

    manifest_factory.assert_called_once_with(
        fake_settings.cache.database_path,
    )

    result_store_factory.assert_called_once_with(
        fake_settings.cache.database_path,
    )

    materializer_factory.assert_called_once_with(
        fake_settings.cache.parquet_dir,
    )

    query_engine_factory.assert_called_once_with(
        fake_settings.cache.parquet_dir,
    )


def test_build_application_wires_sync_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(
        bootstrap,
        "Settings",
        lambda: fake_settings,
    )

    blob_source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    monkeypatch.setattr(
        bootstrap,
        "AzureBlobSource",
        MagicMock(
            return_value=blob_source,
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBManifestRepository",
        MagicMock(
            return_value=manifest,
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBSearchResultStore",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ParquetMaterializer",
        MagicMock(
            return_value=materializer,
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBQueryEngine",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBParquetEventDetailsReader",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ConsoleRenderer",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    sync_service_factory = MagicMock(
        return_value=MagicMock(),
    )

    monkeypatch.setattr(
        bootstrap,
        "SyncService",
        sync_service_factory,
    )

    application = bootstrap.build_application()

    sync_service_factory.assert_called_once_with(
        source=blob_source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=fake_settings.cache.temp_dir,
    )

    assert application.sync_service is sync_service_factory.return_value


def test_build_application_wires_search_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(
        bootstrap,
        "Settings",
        lambda: fake_settings,
    )

    query_engine = MagicMock()
    result_store = MagicMock()
    details_reader = MagicMock()

    monkeypatch.setattr(
        bootstrap,
        "AzureBlobSource",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBManifestRepository",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBSearchResultStore",
        MagicMock(
            return_value=result_store,
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ParquetMaterializer",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBQueryEngine",
        MagicMock(
            return_value=query_engine,
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBParquetEventDetailsReader",
        MagicMock(
            return_value=details_reader,
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ConsoleRenderer",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    search_service_factory = MagicMock(
        return_value=MagicMock(),
    )

    monkeypatch.setattr(
        bootstrap,
        "SearchService",
        search_service_factory,
    )

    application = bootstrap.build_application()

    search_service_factory.assert_called_once_with(
        query_engine=query_engine,
        result_store=result_store,
        details_reader=details_reader,
    )

    assert application.search_service is search_service_factory.return_value


def test_build_application_creates_time_components(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(
        bootstrap,
        "Settings",
        lambda: fake_settings,
    )

    monkeypatch.setattr(
        bootstrap,
        "AzureBlobSource",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBManifestRepository",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBSearchResultStore",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ParquetMaterializer",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBQueryEngine",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "DuckDBParquetEventDetailsReader",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    monkeypatch.setattr(
        bootstrap,
        "ConsoleRenderer",
        MagicMock(
            return_value=MagicMock(),
        ),
    )

    timezone_provider = MagicMock()
    time_range_resolver = MagicMock()
    partition_resolver = MagicMock()

    timezone_provider_factory = MagicMock(
        return_value=timezone_provider,
    )

    time_range_resolver_factory = MagicMock(
        return_value=time_range_resolver,
    )

    partition_resolver_factory = MagicMock(
        return_value=partition_resolver,
    )

    monkeypatch.setattr(
        bootstrap,
        "TimezoneProvider",
        timezone_provider_factory,
    )

    monkeypatch.setattr(
        bootstrap,
        "TimeRangeResolver",
        time_range_resolver_factory,
    )

    monkeypatch.setattr(
        bootstrap,
        "BlobPartitionResolver",
        partition_resolver_factory,
    )

    application = bootstrap.build_application()

    timezone_provider_factory.assert_called_once_with()

    time_range_resolver_factory.assert_called_once_with(
        timezone_provider,
    )

    partition_resolver_factory.assert_called_once_with()

    assert application.time_range_resolver is time_range_resolver

    assert application.partition_resolver is partition_resolver
