from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import event_search.bootstrap as bootstrap_module
from event_search.bootstrap import (
    Application,
    build_application,
)


@pytest.fixture
def sas_token() -> MagicMock:
    token = MagicMock()
    token.get_secret_value.return_value = "test-sas-token"

    return token


@pytest.fixture
def fake_settings(
    sas_token: MagicMock,
) -> SimpleNamespace:
    return SimpleNamespace(
        azure=SimpleNamespace(
            container_url=("https://example.blob.core.windows.net/events"),
            sas_token=sas_token,
            folder_name="activity-logs",
        ),
        cache=SimpleNamespace(
            parquet_dir=Path(".cache/parquet"),
            database_path=Path(".cache/event_search.duckdb"),
            temp_dir=Path(".cache/tmp"),
        ),
        search=SimpleNamespace(
            default_limit=100,
            max_limit=10_000,
        ),
        sync=SimpleNamespace(
            concurrency=4,
            target_parquet_size_mb=128,
        ),
    )


@pytest.fixture
def dependencies(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> SimpleNamespace:
    settings = MagicMock(
        return_value=fake_settings,
    )

    azure_blob_source = MagicMock()
    manifest = MagicMock()
    search_result_store = MagicMock()
    materializer = MagicMock()
    query_engine = MagicMock()
    details_reader = MagicMock()

    sync_service = MagicMock()
    search_service = MagicMock()

    timezone_provider = MagicMock()
    time_range_resolver = MagicMock()
    partition_resolver = MagicMock()

    renderer = MagicMock()

    monkeypatch.setattr(
        bootstrap_module,
        "Settings",
        settings,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "AzureBlobSource",
        azure_blob_source,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "DuckDBManifestRepository",
        manifest,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "DuckDBSearchResultStore",
        search_result_store,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "ParquetMaterializer",
        materializer,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "DuckDBQueryEngine",
        query_engine,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "DuckDBParquetEventDetailsReader",
        details_reader,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "SyncService",
        sync_service,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "SearchService",
        search_service,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "TimezoneProvider",
        timezone_provider,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "TimeRangeResolver",
        time_range_resolver,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "BlobPartitionResolver",
        partition_resolver,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "ConsoleRenderer",
        renderer,
    )

    return SimpleNamespace(
        settings=settings,
        azure_blob_source=azure_blob_source,
        manifest=manifest,
        search_result_store=search_result_store,
        materializer=materializer,
        query_engine=query_engine,
        details_reader=details_reader,
        sync_service=sync_service,
        search_service=search_service,
        timezone_provider=timezone_provider,
        time_range_resolver=time_range_resolver,
        partition_resolver=partition_resolver,
        renderer=renderer,
    )


def test_build_application_creates_application(
    dependencies: SimpleNamespace,
    fake_settings: SimpleNamespace,
) -> None:
    app = build_application()

    assert isinstance(
        app,
        Application,
    )

    assert app.settings is fake_settings

    assert app.sync_service is (dependencies.sync_service.return_value)

    assert app.search_service is (dependencies.search_service.return_value)

    assert app.manifest is (dependencies.manifest.return_value)

    assert app.renderer is (dependencies.renderer.return_value)


def test_build_application_creates_azure_blob_source_from_settings(
    dependencies: SimpleNamespace,
    fake_settings: SimpleNamespace,
) -> None:
    build_application()

    dependencies.azure_blob_source.assert_called_once_with(
        container_url=(fake_settings.azure.container_url),
        sas_token="test-sas-token",
        folder_name=(fake_settings.azure.folder_name),
    )

    fake_settings.azure.sas_token.get_secret_value.assert_called_once_with()


def test_build_application_uses_cache_paths_from_settings(
    dependencies: SimpleNamespace,
    fake_settings: SimpleNamespace,
) -> None:
    build_application()

    dependencies.manifest.assert_called_once_with(
        fake_settings.cache.database_path,
    )

    dependencies.search_result_store.assert_called_once_with(
        fake_settings.cache.database_path,
    )

    dependencies.materializer.assert_called_once_with(
        fake_settings.cache.parquet_dir,
    )

    dependencies.query_engine.assert_called_once_with(
        fake_settings.cache.parquet_dir,
    )

    dependencies.details_reader.assert_called_once_with()


def test_build_application_wires_sync_service(
    dependencies: SimpleNamespace,
    fake_settings: SimpleNamespace,
) -> None:
    build_application()

    dependencies.sync_service.assert_called_once_with(
        source=(dependencies.azure_blob_source.return_value),
        manifest=(dependencies.manifest.return_value),
        materializer=(dependencies.materializer.return_value),
        temp_dir=(fake_settings.cache.temp_dir),
        concurrency=(fake_settings.sync.concurrency),
    )


def test_build_application_wires_search_service(
    dependencies: SimpleNamespace,
) -> None:
    build_application()

    dependencies.search_service.assert_called_once_with(
        query_engine=(dependencies.query_engine.return_value),
        result_store=(dependencies.search_result_store.return_value),
        details_reader=(dependencies.details_reader.return_value),
    )


def test_build_application_creates_time_components(
    dependencies: SimpleNamespace,
) -> None:
    app = build_application()

    dependencies.timezone_provider.assert_called_once_with()

    dependencies.time_range_resolver.assert_called_once_with(
        dependencies.timezone_provider.return_value,
    )

    dependencies.partition_resolver.assert_called_once_with()

    assert app.time_range_resolver is (dependencies.time_range_resolver.return_value)

    assert app.partition_resolver is (dependencies.partition_resolver.return_value)


def test_build_application_creates_renderer(
    dependencies: SimpleNamespace,
) -> None:
    app = build_application()

    dependencies.renderer.assert_called_once_with()

    assert app.renderer is (dependencies.renderer.return_value)
