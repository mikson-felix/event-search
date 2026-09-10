from pathlib import Path
from unittest.mock import MagicMock

import pytest

from event_search.application.sync_service import SyncService
from event_search.domain.models import (
    BlobObject,
    MaterializationResult,
)


@pytest.fixture
def source() -> MagicMock:
    return MagicMock()


@pytest.fixture
def manifest() -> MagicMock:
    return MagicMock()


@pytest.fixture
def materializer() -> MagicMock:
    return MagicMock()


def make_blob(
    *,
    partition: str,
    file_name: str,
) -> BlobObject:
    year, month, day, hour = partition.split("/")

    return BlobObject(
        name=(f"activity-logs/year={year}/month={month}/day={day}/hour={hour}/{file_name}"),
        partition=partition,
        file_name=file_name,
    )


def test_rejects_non_positive_concurrency(
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match="concurrency must be greater than zero",
    ):
        SyncService(
            source=source,
            manifest=manifest,
            materializer=materializer,
            temp_dir=tmp_path,
            concurrency=0,
        )


def test_sync_does_nothing_for_empty_partition(
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    tmp_path: Path,
) -> None:
    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path,
        concurrency=1,
    )

    result = service.sync([])

    assert result.discovered == 0
    assert result.materialized == 0
    assert result.skipped == 0

    source.list_blobs.assert_not_called()
    source.download.assert_not_called()

    manifest.is_materialized.assert_not_called()
    manifest.save.assert_not_called()

    materializer.materialize.assert_not_called()


def test_sync_materializes_only_missing_blobs(
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    tmp_path: Path,
) -> None:
    partition = "2026/09/10/08"

    cached_blob = make_blob(
        partition=partition,
        file_name="cached.ndjson",
    )

    missing_blob = make_blob(
        partition=partition,
        file_name="missing.ndjson",
    )

    source.list_blobs.return_value = [
        cached_blob,
        missing_blob,
    ]

    manifest.is_materialized.side_effect = lambda blob: blob == cached_blob

    materialization_result = MagicMock(
        spec=MaterializationResult,
    )

    materializer.materialize.return_value = materialization_result

    def download(
        blob: BlobObject,
        target: Path,
    ) -> None:
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        target.write_text(
            '{"event_id":"event-001"}\n',
            encoding="utf-8",
        )

    source.download.side_effect = download

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path,
        concurrency=1,
    )

    result = service.sync(
        [
            partition,
        ]
    )

    assert result.discovered == 2
    assert result.materialized == 1
    assert result.skipped == 1

    source.list_blobs.assert_called_once_with(
        partition,
    )

    source.download.assert_called_once()

    downloaded_blob = source.download.call_args.args[0]
    downloaded_target = source.download.call_args.args[1]

    assert downloaded_blob == missing_blob
    assert downloaded_target == (tmp_path.resolve() / partition / "missing.ndjson")

    materializer.materialize.assert_called_once_with(
        source_file=(tmp_path.resolve() / partition / "missing.ndjson"),
        blob=missing_blob,
    )

    manifest.save.assert_called_once_with(
        blob=missing_blob,
        result=materialization_result,
    )


def test_temp_file_is_removed_after_materialization(
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    tmp_path: Path,
) -> None:
    partition = "2026/09/10/08"

    blob = make_blob(
        partition=partition,
        file_name="events.ndjson",
    )

    source.list_blobs.return_value = [
        blob,
    ]

    manifest.is_materialized.return_value = False

    materializer.materialize.return_value = MagicMock(
        spec=MaterializationResult,
    )

    target = tmp_path.resolve() / partition / "events.ndjson"

    def download(
        blob: BlobObject,
        target: Path,
    ) -> None:
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        target.write_text(
            '{"event_id":"event-001"}\n',
            encoding="utf-8",
        )

    source.download.side_effect = download

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path,
        concurrency=1,
    )

    service.sync(
        [
            partition,
        ]
    )

    assert not target.exists()


def test_temp_file_is_removed_when_materialization_fails(
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    tmp_path: Path,
) -> None:
    partition = "2026/09/10/08"

    blob = make_blob(
        partition=partition,
        file_name="events.ndjson",
    )

    source.list_blobs.return_value = [
        blob,
    ]

    manifest.is_materialized.return_value = False

    target = tmp_path.resolve() / partition / "events.ndjson"

    def download(
        blob: BlobObject,
        target: Path,
    ) -> None:
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        target.write_text(
            '{"event_id":"event-001"}\n',
            encoding="utf-8",
        )

    source.download.side_effect = download

    materializer.materialize.side_effect = RuntimeError("materialization failed")

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path,
        concurrency=1,
    )

    with pytest.raises(
        RuntimeError,
        match="materialization failed",
    ):
        service.sync(
            [
                partition,
            ]
        )

    assert not target.exists()

    manifest.save.assert_not_called()


def test_skipped_blob_is_not_downloaded(
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    tmp_path: Path,
) -> None:
    partition = "2026/09/10/08"

    blob = make_blob(
        partition=partition,
        file_name="events.ndjson",
    )

    source.list_blobs.return_value = [
        blob,
    ]

    manifest.is_materialized.return_value = True

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path,
        concurrency=1,
    )

    result = service.sync(
        [
            partition,
        ]
    )

    assert result.discovered == 1
    assert result.materialized == 0
    assert result.skipped == 1

    source.download.assert_not_called()
    materializer.materialize.assert_not_called()
    manifest.save.assert_not_called()


def test_sync_queries_each_partition(
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    tmp_path: Path,
) -> None:
    source.list_blobs.return_value = []

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path,
        concurrency=2,
    )

    result = service.sync(
        [
            "2026/09/10/08",
            "2026/09/10/09",
        ]
    )

    assert result.discovered == 0
    assert result.materialized == 0
    assert result.skipped == 0

    assert source.list_blobs.call_count == 2

    source.list_blobs.assert_any_call("2026/09/10/08")

    source.list_blobs.assert_any_call("2026/09/10/09")


def test_sync_aggregates_results_from_multiple_partitions(
    source: MagicMock,
    manifest: MagicMock,
    materializer: MagicMock,
    tmp_path: Path,
) -> None:
    partition_08 = "2026/09/10/08"
    partition_09 = "2026/09/10/09"

    blob_08 = make_blob(
        partition=partition_08,
        file_name="a.ndjson",
    )

    blob_09 = make_blob(
        partition=partition_09,
        file_name="b.ndjson",
    )

    def list_blobs(
        partition: str,
    ) -> list[BlobObject]:
        if partition == partition_08:
            return [
                blob_08,
            ]

        if partition == partition_09:
            return [
                blob_09,
            ]

        return []

    source.list_blobs.side_effect = list_blobs

    manifest.is_materialized.side_effect = lambda blob: blob == blob_09

    materializer.materialize.return_value = MagicMock(
        spec=MaterializationResult,
    )

    def download(
        blob: BlobObject,
        target: Path,
    ) -> None:
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        target.write_text(
            "{}\n",
            encoding="utf-8",
        )

    source.download.side_effect = download

    service = SyncService(
        source=source,
        manifest=manifest,
        materializer=materializer,
        temp_dir=tmp_path,
        concurrency=2,
    )

    result = service.sync(
        [
            partition_08,
            partition_09,
        ]
    )

    assert result.discovered == 2
    assert result.materialized == 1
    assert result.skipped == 1

    assert source.list_blobs.call_count == 2

    source.download.assert_called_once()
    materializer.materialize.assert_called_once()
    manifest.save.assert_called_once()
