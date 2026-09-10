from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from event_search.domain.models import SyncResult
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
        blobs = self._source.list_blobs(
            partition,
        )

        materialized = 0
        skipped = 0

        for blob in blobs:
            if self._manifest.is_materialized(blob):
                skipped += 1
                continue

            temp_file = self._temp_dir / blob.partition / blob.file_name

            try:
                self._source.download(
                    blob,
                    temp_file,
                )

                result = self._materializer.materialize(
                    source_file=temp_file,
                    blob=blob,
                )

                self._manifest.save(
                    blob=blob,
                    result=result,
                )

                materialized += 1

            finally:
                temp_file.unlink(
                    missing_ok=True,
                )

        return _PartitionSyncResult(
            discovered=len(blobs),
            materialized=materialized,
            skipped=skipped,
        )
