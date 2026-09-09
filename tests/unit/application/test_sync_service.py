import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from event_search.application.sync_service import (
    SyncService,
)
from event_search.domain.models import (
    BlobObject,
    MaterializationResult,
)


def make_blob(
    *,
    file_name: str,
    partition: str = "2026/09/10/08",
) -> BlobObject:
    return BlobObject(
        name=f"activity-logs/{partition}/{file_name}",
        partition=partition,
        file_name=file_name,
    )


def make_service(
    *,
    tmp_path: Path,
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    concurrency: int = 2,
) -> SyncService:
    return SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path / "tmp",
        concurrency=concurrency,
    )


def test_rejects_invalid_concurrency(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    with pytest.raises(
        ValueError,
        match="concurrency",
    ):
        SyncService(
            source=source,
            manifest=manifest,
            materializer=materializer,
            temp_dir=tmp_path,
            concurrency=0,
        )


def test_sync_returns_empty_result_for_no_partitions(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
    )

    result = service.sync([])

    assert result.discovered == 0
    assert result.materialized == 0
    assert result.skipped == 0

    source.list_blobs.assert_not_called()
    materializer.materialize.assert_not_called()


def test_sync_materializes_only_missing_blobs(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    cached_blob = make_blob(file_name="cached.ndjson")
    missing_blob = make_blob(file_name="missing.ndjson")

    source.list_blobs.return_value = [
        cached_blob,
        missing_blob,
    ]

    manifest.is_materialized.side_effect = lambda blob: blob == cached_blob

    output_file = tmp_path / "parquet" / "part-test.parquet"

    materializer.materialize.return_value = [
        MaterializationResult(
            path=output_file,
            events_count=10,
            blobs=(missing_blob,),
        )
    ]

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
    )

    result = service.sync(["2026/09/10/08"])

    assert result.discovered == 2
    assert result.materialized == 1
    assert result.skipped == 1

    source.download.assert_called_once()

    materializer.materialize.assert_called_once()

    call = materializer.materialize.call_args

    assert call.kwargs["partition"] == "2026/09/10/08"

    sources = call.kwargs["sources"]

    assert len(sources) == 1
    assert sources[0][1] == missing_blob

    manifest.save.assert_called_once_with(materializer.materialize.return_value[0])


def test_sync_skips_materializer_when_all_blobs_cached(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    blobs = [
        make_blob(file_name="first.ndjson"),
        make_blob(file_name="second.ndjson"),
    ]

    source.list_blobs.return_value = blobs
    manifest.is_materialized.return_value = True

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
    )

    result = service.sync(["2026/09/10/08"])

    assert result.discovered == 2
    assert result.materialized == 0
    assert result.skipped == 2

    source.download.assert_not_called()
    materializer.materialize.assert_not_called()
    manifest.save.assert_not_called()


def test_temp_files_are_removed_after_materialization(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    blob = make_blob(file_name="events.ndjson")

    source.list_blobs.return_value = [blob]

    manifest.is_materialized.return_value = False

    def download(
        _blob: BlobObject,
        target: Path,
    ) -> None:
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        target.write_text(
            "{}",
            encoding="utf-8",
        )

    source.download.side_effect = download

    materializer.materialize.return_value = [
        MaterializationResult(
            path=(tmp_path / "part.parquet"),
            events_count=1,
            blobs=(blob,),
        )
    ]

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
    )

    service.sync(["2026/09/10/08"])

    expected_temp = tmp_path / "tmp" / blob.partition / blob.file_name

    assert not expected_temp.exists()


def test_temp_files_are_removed_when_materializer_fails(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    blob = make_blob(file_name="events.ndjson")

    source.list_blobs.return_value = [blob]
    manifest.is_materialized.return_value = False

    def download(
        _blob: BlobObject,
        target: Path,
    ) -> None:
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        target.write_text(
            "{}",
            encoding="utf-8",
        )

    source.download.side_effect = download

    materializer.materialize.side_effect = RuntimeError("failed")

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
    )

    with pytest.raises(
        RuntimeError,
        match="failed",
    ):
        service.sync(["2026/09/10/08"])

    expected_temp = tmp_path / "tmp" / blob.partition / blob.file_name

    assert not expected_temp.exists()


def test_sync_saves_all_materialization_results(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    first_blob = make_blob(file_name="first.ndjson")
    second_blob = make_blob(file_name="second.ndjson")

    source.list_blobs.return_value = [
        first_blob,
        second_blob,
    ]

    manifest.is_materialized.return_value = False

    first_result = MaterializationResult(
        path=tmp_path / "first.parquet",
        events_count=10,
        blobs=(first_blob,),
    )

    second_result = MaterializationResult(
        path=tmp_path / "second.parquet",
        events_count=20,
        blobs=(second_blob,),
    )

    materializer.materialize.return_value = [
        first_result,
        second_result,
    ]

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
    )

    result = service.sync(["2026/09/10/08"])

    assert result.discovered == 2
    assert result.materialized == 2
    assert result.skipped == 0

    assert manifest.save.call_count == 2

    manifest.save.assert_any_call(first_result)

    manifest.save.assert_any_call(second_result)


def test_sync_aggregates_results_from_multiple_partitions(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    first_partition = "2026/09/10/08"
    second_partition = "2026/09/10/09"

    first_blob = make_blob(
        file_name="first.ndjson",
        partition=first_partition,
    )

    second_blob = make_blob(
        file_name="second.ndjson",
        partition=second_partition,
    )

    source.list_blobs.side_effect = lambda partition: [first_blob] if partition == first_partition else [second_blob]

    manifest.is_materialized.return_value = False

    def materialize(
        *,
        partition: str,
        sources: list,
    ):
        blobs = tuple(blob for _, blob in sources)

        return [
            MaterializationResult(
                path=(tmp_path / (partition.replace("/", "-") + ".parquet")),
                events_count=len(blobs),
                blobs=blobs,
            )
        ]

    materializer.materialize.side_effect = materialize

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
    )

    result = service.sync(
        [
            first_partition,
            second_partition,
        ]
    )

    assert result.discovered == 2
    assert result.materialized == 2
    assert result.skipped == 0

    assert source.list_blobs.call_count == 2
    assert materializer.materialize.call_count == 2
    assert manifest.save.call_count == 2


def test_downloads_missing_blobs_concurrently(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    blobs = [make_blob(file_name=f"blob-{index}.ndjson") for index in range(4)]

    source.list_blobs.return_value = blobs
    manifest.is_materialized.return_value = False
    materializer.materialize.return_value = []

    barrier = threading.Barrier(len(blobs))

    def download(
        blob: BlobObject,
        target: Path,
    ) -> None:
        # Only satisfied if all four downloads are in flight at once; a
        # sequential implementation would time out and raise here.
        barrier.wait(timeout=2)

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{}", encoding="utf-8")

    source.download.side_effect = download

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
        concurrency=len(blobs),
    )

    service.sync(["2026/09/10/08"])

    assert source.download.call_count == len(blobs)


def test_download_order_matches_missing_blobs_regardless_of_completion_order(
    tmp_path: Path,
) -> None:
    source = MagicMock()
    manifest = MagicMock()
    materializer = MagicMock()

    blobs = [make_blob(file_name=f"blob-{index}.ndjson") for index in range(3)]

    source.list_blobs.return_value = blobs
    manifest.is_materialized.return_value = False
    materializer.materialize.return_value = []

    def download(
        blob: BlobObject,
        target: Path,
    ) -> None:
        # First blob finishes last, proving result order does not depend on completion order.
        if blob is blobs[0]:
            time.sleep(0.05)

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{}", encoding="utf-8")

    source.download.side_effect = download

    service = make_service(
        tmp_path=tmp_path,
        source=source,
        manifest=manifest,
        materializer=materializer,
        concurrency=len(blobs),
    )

    service.sync(["2026/09/10/08"])

    sources = materializer.materialize.call_args.kwargs["sources"]

    assert [blob for _, blob in sources] == blobs
