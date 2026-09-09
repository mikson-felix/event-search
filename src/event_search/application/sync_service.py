from pathlib import Path

from event_search.domain.models import SyncResult
from event_search.domain.ports import (
    BlobSource,
    ManifestRepository,
    Materializer,
)


class SyncService:
    def __init__(
        self,
        *,
        source: BlobSource,
        manifest: ManifestRepository,
        materializer: Materializer,
        temp_dir: Path,
    ) -> None:
        self._source = source
        self._manifest = manifest
        self._materializer = materializer
        self._temp_dir = temp_dir.resolve()

    def sync(
        self,
        partitions: list[str],
    ) -> SyncResult:
        blobs = self._source.list_blobs(partitions)

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
                temp_file.unlink(missing_ok=True)

        return SyncResult(
            discovered=len(blobs),
            materialized=materialized,
            skipped=skipped,
        )
