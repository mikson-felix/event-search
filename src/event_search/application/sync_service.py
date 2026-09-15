from collections.abc import Callable, Iterator
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    as_completed,
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
        download_concurrency: int,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be greater than zero")

        if download_concurrency < 1:
            raise ValueError("download_concurrency must be greater than zero")

        self._source = source
        self._manifest = manifest
        self._materializer = materializer
        self._concurrency = concurrency
        self._download_concurrency = download_concurrency

    def sync(
        self,
        partitions: list[str],
        *,
        on_partition_synced: Callable[[], None] | None = None,
    ) -> SyncResult:
        if not partitions:
            return SyncResult(
                discovered=0,
                materialized=0,
                skipped=0,
            )

        discovered = 0
        materialized = 0
        skipped = 0

        with ThreadPoolExecutor(
            max_workers=self._concurrency,
        ) as executor:
            futures = [executor.submit(self._sync_partition, partition) for partition in partitions]

            for future in as_completed(futures):
                result = future.result()

                discovered += result.discovered
                materialized += result.materialized
                skipped += result.skipped

                if on_partition_synced is not None:
                    on_partition_synced()

        return SyncResult(
            discovered=discovered,
            materialized=materialized,
            skipped=skipped,
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

        sources = self._download_pipeline(missing_blobs)

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

    def _download_pipeline(
        self,
        blobs: list[BlobObject],
    ) -> Iterator[tuple[bytes, BlobObject]]:
        # Downloads are submitted here, eagerly, so they start regardless of
        # whether/how fast the returned iterator is consumed. Only the
        # blocking wait for each result is deferred to iteration, which lets
        # a materializer parse earlier blobs while later ones are still
        # downloading in the background.
        max_workers = min(len(blobs), self._download_concurrency)

        executor = ThreadPoolExecutor(max_workers=max_workers)

        futures = [executor.submit(self._source.download, blob) for blob in blobs]

        return self._iter_downloaded(
            executor=executor,
            futures=futures,
            blobs=blobs,
        )

    @staticmethod
    def _iter_downloaded(
        *,
        executor: ThreadPoolExecutor,
        futures: list[Future[bytes]],
        blobs: list[BlobObject],
    ) -> Iterator[tuple[bytes, BlobObject]]:
        try:
            for future, blob in zip(futures, blobs, strict=True):
                yield future.result(), blob
        finally:
            executor.shutdown(wait=True)
