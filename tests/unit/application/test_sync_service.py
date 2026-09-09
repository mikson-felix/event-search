from __future__ import annotations

from pathlib import Path

from event_search.application.sync_service import SyncService
from event_search.domain.models import (
    BlobObject,
    MaterializationResult,
)


class FakeBlobSource:
    def __init__(
        self,
        blobs: list[BlobObject],
    ) -> None:
        self.blobs = blobs
        self.list_calls: list[list[str]] = []
        self.download_calls: list[tuple[BlobObject, Path]] = []

    def list_blobs(
        self,
        partitions: list[str],
    ) -> list[BlobObject]:
        self.list_calls.append(partitions)

        return [blob for blob in self.blobs if blob.partition in partitions]

    def download(
        self,
        blob: BlobObject,
        destination: Path,
    ) -> None:
        self.download_calls.append(
            (
                blob,
                destination,
            )
        )

        destination.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        destination.write_bytes(b"test-data")


class FakeManifest:
    def __init__(
        self,
        materialized: set[str] | None = None,
    ) -> None:
        self.materialized = materialized or set()

        self.saved: list[
            tuple[
                BlobObject,
                MaterializationResult,
            ]
        ] = []

    def is_materialized(
        self,
        blob: BlobObject,
    ) -> bool:
        return blob.name in self.materialized

    def save(
        self,
        *,
        blob: BlobObject,
        result: MaterializationResult,
    ) -> None:
        self.saved.append(
            (
                blob,
                result,
            )
        )


class FakeMaterializer:
    def __init__(
        self,
        parquet_dir: Path,
    ) -> None:
        self.parquet_dir = parquet_dir

        self.calls: list[
            tuple[
                Path,
                BlobObject,
            ]
        ] = []

    def materialize(
        self,
        *,
        source_file: Path,
        blob: BlobObject,
    ) -> MaterializationResult:
        self.calls.append(
            (
                source_file,
                blob,
            )
        )

        parquet_path = self.parquet_dir / blob.partition / f"{Path(blob.file_name).stem}.parquet"

        parquet_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        parquet_path.touch()

        return MaterializationResult(
            path=parquet_path,
            events_count=1,
        )


def make_blob(
    name: str,
) -> BlobObject:
    path = Path(name)

    return BlobObject(
        name=name,
        partition=str(path.parent),
        file_name=path.name,
    )


def test_sync_materializes_only_missing_blobs(
    tmp_path: Path,
) -> None:
    partition = "2026/09/09/08"

    blob_a = make_blob(
        f"{partition}/a.ndjson",
    )

    blob_b = make_blob(
        f"{partition}/b.ndjson",
    )

    blob_c = make_blob(
        f"{partition}/c.ndjson",
    )

    source = FakeBlobSource(
        [
            blob_a,
            blob_b,
            blob_c,
        ]
    )

    manifest = FakeManifest(
        materialized={
            blob_a.name,
            blob_c.name,
        }
    )

    materializer = FakeMaterializer(
        tmp_path / "parquet",
    )

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path / "tmp",
    )

    result = service.sync(
        [
            partition,
        ]
    )

    assert result.discovered == 3
    assert result.materialized == 1
    assert result.skipped == 2

    assert source.list_calls == [
        [
            partition,
        ]
    ]

    assert len(source.download_calls) == 1

    downloaded_blob, downloaded_path = source.download_calls[0]

    assert downloaded_blob == blob_b

    assert downloaded_path == (tmp_path / "tmp" / partition / "b.ndjson").resolve()

    assert len(materializer.calls) == 1

    source_file, materialized_blob = materializer.calls[0]

    assert materialized_blob == blob_b

    assert source_file == (tmp_path / "tmp" / partition / "b.ndjson").resolve()

    assert len(manifest.saved) == 1

    saved_blob, saved_result = manifest.saved[0]

    assert saved_blob == blob_b
    assert saved_result.events_count == 1

    assert not downloaded_path.exists()


def test_sync_does_nothing_for_empty_partition(
    tmp_path: Path,
) -> None:
    source = FakeBlobSource([])

    manifest = FakeManifest()

    materializer = FakeMaterializer(
        tmp_path / "parquet",
    )

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path / "tmp",
    )

    result = service.sync(
        [
            "2026/09/09/08",
        ]
    )

    assert result.discovered == 0
    assert result.materialized == 0
    assert result.skipped == 0

    assert source.download_calls == []
    assert materializer.calls == []
    assert manifest.saved == []


def test_temp_file_is_removed_after_materialization(
    tmp_path: Path,
) -> None:
    partition = "2026/09/09/08"

    blob = make_blob(
        f"{partition}/events.ndjson",
    )

    source = FakeBlobSource(
        [
            blob,
        ]
    )

    manifest = FakeManifest()

    materializer = FakeMaterializer(
        tmp_path / "parquet",
    )

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path / "tmp",
    )

    service.sync(
        [
            partition,
        ]
    )

    temp_file = (tmp_path / "tmp" / partition / "events.ndjson").resolve()

    assert not temp_file.exists()


def test_skipped_blob_is_not_downloaded(
    tmp_path: Path,
) -> None:
    partition = "2026/09/09/08"

    blob = make_blob(
        f"{partition}/events.ndjson",
    )

    source = FakeBlobSource(
        [
            blob,
        ]
    )

    manifest = FakeManifest(
        materialized={
            blob.name,
        }
    )

    materializer = FakeMaterializer(
        tmp_path / "parquet",
    )

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path / "tmp",
    )

    result = service.sync(
        [
            partition,
        ]
    )

    assert result.discovered == 1
    assert result.materialized == 0
    assert result.skipped == 1

    assert source.download_calls == []
    assert materializer.calls == []
    assert manifest.saved == []
