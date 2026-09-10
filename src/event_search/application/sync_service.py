from concurrent.futures import (
    ThreadPoolExecutor,
)
from dataclasses import dataclass
from pathlib import Path

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
        temp_dir: Path,
        concurrency: int,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be greater than zero")

        self._source = source
        self._manifest = manifest
        self._materializer = materializer
        self._temp_dir = temp_dir.resolve()
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
        blobs = self._source.list_blobs(partition)

        missing_blobs = [blob for blob in blobs if not self._manifest.is_materialized(blob)]

        skipped = len(blobs) - len(missing_blobs)

        if not missing_blobs:
            return _PartitionSyncResult(
                discovered=len(blobs),
                materialized=0,
                skipped=skipped,
            )

        sources: list[tuple[Path, BlobObject]] = []

        try:
            for blob in missing_blobs:
                temp_file = self._temp_dir / blob.partition / blob.file_name

                self._source.download(
                    blob,
                    temp_file,
                )

                sources.append(
                    (
                        temp_file,
                        blob,
                    )
                )

            results = self._materializer.materialize(
                partition=partition,
                sources=sources,
            )

            for result in results:
                self._manifest.save(result)

        finally:
            for source_file, _ in sources:
                source_file.unlink(missing_ok=True)

        return _PartitionSyncResult(
            discovered=len(blobs),
            materialized=len(missing_blobs),
            skipped=skipped,
        )
