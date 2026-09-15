from concurrent.futures import (
    ThreadPoolExecutor,
)
from dataclasses import dataclass

from loguru import logger

from event_search.domain.models import (
    BlobObject,
    SyncResult,
)
from event_search.domain.ports import (
    BlobSource,
    ManifestRepository,
    Materializer,
)


@dataclass(frozen=True)
class _PartitionSyncResult:
    discovered: int
    materialized: int
    skipped: int


class SyncService:
    def __init__(
        self,
        *,
        source: BlobSource,
        manifest: ManifestRepository,
        materializer: Materializer,
        concurrency: int,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be greater than zero")

        self._source = source
        self._manifest = manifest
        self._materializer = materializer
        self._concurrency = concurrency

    def sync(
        self,
        partitions: list[str],
    ) -> SyncResult:
        if not partitions:
            return SyncResult(
                discovered=0,
                materialized=0,
                skipped=0,
            )

        with ThreadPoolExecutor(
            max_workers=self._concurrency,
        ) as executor:
            results = list(
                executor.map(
                    self._sync_partition,
                    partitions,
                )
            )

        return SyncResult(
            discovered=sum(result.discovered for result in results),
            materialized=sum(result.materialized for result in results),
            skipped=sum(result.skipped for result in results),
        )

    def _sync_partition(
        self,
        partition: str,
    ) -> _PartitionSyncResult:
        logger.debug(
            "Sync partition started: partition={}",
            partition,
        )
        blobs = self._source.list_blobs(partition)
        missing_blobs = self._manifest.filter_missing(blobs)
        skipped = len(blobs) - len(missing_blobs)
        logger.debug(
            ("Sync partition state: partition={}, discovered={}, cached={}, missing={}"),
            partition,
            len(blobs),
            skipped,
            len(missing_blobs),
        )

        if not missing_blobs:
            logger.debug(
                "Sync partition fully cached: partition={}",
                partition,
            )

            return _PartitionSyncResult(
                discovered=len(blobs),
                materialized=0,
                skipped=skipped,
            )

        downloaded = self._download_all(missing_blobs)

        sources = list(
            zip(
                downloaded,
                missing_blobs,
                strict=True,
            )
        )

        results = self._materializer.materialize(
            partition=partition,
            sources=sources,
        )

        for result in results:
            self._manifest.save(result)

        logger.debug(
            ("Partition materialized: partition={}, source_blobs={}, parquet_files={}"),
            partition,
            len(missing_blobs),
            len(results),
        )

        return _PartitionSyncResult(
            discovered=len(blobs),
            materialized=len(missing_blobs),
            skipped=skipped,
        )

    def _download_all(
        self,
        blobs: list[BlobObject],
    ) -> list[bytes]:
        max_workers = min(len(blobs), self._concurrency)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(
                executor.map(
                    self._source.download,
                    blobs,
                )
            )
