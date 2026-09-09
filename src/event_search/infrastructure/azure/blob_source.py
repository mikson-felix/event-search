from pathlib import Path

from azure.storage.blob import ContainerClient
from loguru import logger

from event_search.domain.models import BlobObject


class AzureBlobSource:
    def __init__(
        self,
        *,
        container_url: str,
        sas_token: str,
        folder_name: str,
    ) -> None:
        self._client = ContainerClient.from_container_url(
            container_url=container_url,
            credential=sas_token.lstrip("?"),
        )
        self._folder_name = folder_name.strip("/")

    def list_blobs(self, partition: str) -> list[BlobObject]:
        prefix = self._build_prefix(partition=partition)
        logger.debug(
            "Listing Azure blobs: partition={}, prefix={}",
            partition,
            prefix,
        )
        blobs = self._client.list_blobs(
            name_starts_with=prefix,
        )

        result: list[BlobObject] = []

        for blob in blobs:
            if not blob.name.endswith(".ndjson"):
                continue

            file_name = blob.name.rsplit("/", maxsplit=1)[-1]

            result.append(
                BlobObject(
                    name=blob.name,
                    partition=partition,
                    file_name=file_name,
                )
            )

        result = sorted(result, key=lambda blob: blob.name)
        logger.debug(
            "Azure blobs discovered: partition={}, count={}",
            partition,
            len(result),
        )
        return result

    def download(
        self,
        blob: BlobObject,
        target: Path,
    ) -> None:
        logger.debug(
            "Downloading Azure blob: blob={}, target={}",
            blob.name,
            target,
        )
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        blob_client = self._client.get_blob_client(blob.name)

        downloader = blob_client.download_blob()

        with target.open("wb") as stream:
            downloader.readinto(stream)

        logger.debug(
            "Azure blob downloaded: blob={}, size_bytes={}",
            blob.name,
            target.stat().st_size,
        )

    def _build_prefix(self, partition: str) -> str:
        year, month, day, hour = partition.strip("/").split("/")
        partition_path = f"year={year}/month={month}/day={day}/hour={hour}"
        parts = [self._folder_name, partition_path]
        return "/".join(part for part in parts if part) + "/"
