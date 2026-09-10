from dataclasses import dataclass

from event_search.application.search_service import SearchService
from event_search.application.sync_service import SyncService
from event_search.application.time import (
    BlobPartitionResolver,
    TimeRangeResolver,
    TimezoneProvider,
)
from event_search.config import Settings
from event_search.infrastructure.azure.blob_source import AzureBlobSource
from event_search.infrastructure.cache.manifest import (
    DuckDBManifestRepository,
)
from event_search.infrastructure.cache.materializer import (
    ParquetMaterializer,
)
from event_search.infrastructure.cache.search_result_store import (
    DuckDBSearchResultStore,
)
from event_search.infrastructure.query.duckdb_engine import (
    DuckDBQueryEngine,
)
from event_search.infrastructure.query.parquet_event_reader import (
    DuckDBParquetEventDetailsReader,
)
from event_search.presentation.console import ConsoleRenderer


@dataclass(frozen=True)
class Application:
    settings: Settings
    time_range_resolver: TimeRangeResolver
    partition_resolver: BlobPartitionResolver
    sync_service: SyncService
    search_service: SearchService
    manifest: DuckDBManifestRepository
    renderer: ConsoleRenderer


def build_application() -> Application:
    settings = Settings()

    blob_source = AzureBlobSource(
        container_url=(settings.azure.container_url),
        sas_token=(settings.azure.sas_token.get_secret_value()),
        folder_name=settings.azure.folder_name,
    )

    manifest = DuckDBManifestRepository(settings.cache.database_path)

    search_result_store = DuckDBSearchResultStore(settings.cache.database_path)

    materializer = ParquetMaterializer(settings.cache.parquet_dir)

    query_engine = DuckDBQueryEngine(settings.cache.parquet_dir)

    details_reader = DuckDBParquetEventDetailsReader()

    sync_service = SyncService(
        source=blob_source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=settings.cache.temp_dir,
        concurrency=settings.sync.concurrency,
    )

    search_service = SearchService(
        query_engine=query_engine,
        result_store=search_result_store,
        details_reader=details_reader,
    )

    return Application(
        settings=settings,
        time_range_resolver=TimeRangeResolver(TimezoneProvider()),
        partition_resolver=BlobPartitionResolver(),
        sync_service=sync_service,
        search_service=search_service,
        manifest=manifest,
        renderer=ConsoleRenderer(),
    )
