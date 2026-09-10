from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import event_search.infrastructure.azure.blob_source as blob_source_module
from event_search.domain.models import BlobObject
from event_search.infrastructure.azure.blob_source import AzureBlobSource


@pytest.fixture
def container_client(
    monkeypatch: pytest.MonkeyPatch,
) -> MagicMock:
    client = MagicMock()

    monkeypatch.setattr(
        blob_source_module.ContainerClient,
        "from_container_url",
        MagicMock(
            return_value=client,
        ),
    )

    return client


def test_initializes_container_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = MagicMock(
        return_value=MagicMock(),
    )

    monkeypatch.setattr(
        blob_source_module.ContainerClient,
        "from_container_url",
        factory,
    )

    AzureBlobSource(
        container_url="https://example.blob.core.windows.net/events",
        sas_token="?test-token",
        folder_name="/archive/",
    )

    factory.assert_called_once_with(
        container_url="https://example.blob.core.windows.net/events",
        credential="test-token",
    )


@pytest.mark.parametrize(
    ("folder_name", "partition", "expected"),
    [
        (
            "archive",
            "2026/09/10/08",
            "archive/year=2026/month=09/day=10/hour=08/",
        ),
        (
            "/archive/",
            "/2026/09/10/08/",
            "archive/year=2026/month=09/day=10/hour=08/",
        ),
        (
            "",
            "2026/09/10/08",
            "year=2026/month=09/day=10/hour=08/",
        ),
    ],
)
def test_build_prefix(
    container_client: MagicMock,
    folder_name: str,
    partition: str,
    expected: str,
) -> None:
    source = AzureBlobSource(
        container_url="https://example.blob.core.windows.net/events",
        sas_token="test-token",
        folder_name=folder_name,
    )

    result = source._build_prefix(
        partition,
    )

    assert result == expected


def test_list_blobs_uses_folder_name_in_prefix(
    container_client: MagicMock,
) -> None:
    container_client.list_blobs.return_value = [
        SimpleNamespace(
            name=("archive/year=2026/month=09/day=10/hour=08/b.ndjson"),
        ),
        SimpleNamespace(
            name=("archive/year=2026/month=09/day=10/hour=08/a.ndjson"),
        ),
    ]

    source = AzureBlobSource(
        container_url="https://example.blob.core.windows.net/events",
        sas_token="test-token",
        folder_name="archive",
    )

    result = source.list_blobs(
        [
            "2026/09/10/08",
        ]
    )

    container_client.list_blobs.assert_called_once_with(
        name_starts_with=("archive/year=2026/month=09/day=10/hour=08/"),
    )

    assert [blob.name for blob in result] == [
        ("archive/year=2026/month=09/day=10/hour=08/a.ndjson"),
        ("archive/year=2026/month=09/day=10/hour=08/b.ndjson"),
    ]

    assert result[0].partition == "2026/09/10/08"
    assert result[0].file_name == "a.ndjson"


def test_list_blobs_ignores_non_ndjson_files(
    container_client: MagicMock,
) -> None:
    container_client.list_blobs.return_value = [
        SimpleNamespace(
            name=("archive/year=2026/month=09/day=10/hour=08/events.ndjson"),
        ),
        SimpleNamespace(
            name=("archive/year=2026/month=09/day=10/hour=08/readme.txt"),
        ),
        SimpleNamespace(
            name=("archive/year=2026/month=09/day=10/hour=08/data.json"),
        ),
    ]

    source = AzureBlobSource(
        container_url="https://example.blob.core.windows.net/events",
        sas_token="test-token",
        folder_name="archive",
    )

    result = source.list_blobs(
        [
            "2026/09/10/08",
        ]
    )

    assert len(result) == 1

    assert result[0].name == ("archive/year=2026/month=09/day=10/hour=08/events.ndjson")


def test_list_blobs_queries_each_partition(
    container_client: MagicMock,
) -> None:
    container_client.list_blobs.side_effect = [
        [
            SimpleNamespace(
                name=("archive/year=2026/month=09/day=10/hour=08/a.ndjson"),
            ),
        ],
        [
            SimpleNamespace(
                name=("archive/year=2026/month=09/day=10/hour=09/b.ndjson"),
            ),
        ],
    ]

    source = AzureBlobSource(
        container_url="https://example.blob.core.windows.net/events",
        sas_token="test-token",
        folder_name="archive",
    )

    result = source.list_blobs(
        [
            "2026/09/10/08",
            "2026/09/10/09",
        ]
    )

    assert container_client.list_blobs.call_count == 2

    container_client.list_blobs.assert_any_call(
        name_starts_with=("archive/year=2026/month=09/day=10/hour=08/"),
    )

    container_client.list_blobs.assert_any_call(
        name_starts_with=("archive/year=2026/month=09/day=10/hour=09/"),
    )

    assert [blob.partition for blob in result] == [
        "2026/09/10/08",
        "2026/09/10/09",
    ]


def test_list_blobs_without_folder_name(
    container_client: MagicMock,
) -> None:
    container_client.list_blobs.return_value = []

    source = AzureBlobSource(
        container_url="https://example.blob.core.windows.net/events",
        sas_token="test-token",
        folder_name="",
    )

    result = source.list_blobs(
        [
            "2026/09/10/08",
        ]
    )

    assert result == []

    container_client.list_blobs.assert_called_once_with(
        name_starts_with=("year=2026/month=09/day=10/hour=08/"),
    )


def test_download_writes_blob_to_target(
    tmp_path: Path,
    container_client: MagicMock,
) -> None:
    source = AzureBlobSource(
        container_url="https://example.blob.core.windows.net/events",
        sas_token="test-token",
        folder_name="archive",
    )

    blob = BlobObject(
        name=("archive/year=2026/month=09/day=10/hour=08/events.ndjson"),
        partition="2026/09/10/08",
        file_name="events.ndjson",
    )

    blob_client = MagicMock()
    downloader = MagicMock()

    container_client.get_blob_client.return_value = blob_client
    blob_client.download_blob.return_value = downloader

    def write_content(stream) -> None:
        stream.write(b'{"event_id":"event-001"}\n')

    downloader.readinto.side_effect = write_content

    target = tmp_path / "nested" / "directory" / "events.ndjson"

    source.download(
        blob,
        target,
    )

    container_client.get_blob_client.assert_called_once_with(
        blob.name,
    )

    blob_client.download_blob.assert_called_once_with()

    downloader.readinto.assert_called_once()

    assert target.exists()

    assert target.read_bytes() == (b'{"event_id":"event-001"}\n')
