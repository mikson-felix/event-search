from pathlib import Path

from azure.storage.blob import ContainerClient

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

    def list_blobs(
        self,
        partitions: list[str],
    ) -> list[BlobObject]:
        result: list[BlobObject] = []

        for partition in partitions:
            prefix = self._build_prefix(partition=partition)

            blobs = self._client.list_blobs(name_starts_with=prefix)

            for blob in blobs:
                if not blob.name.endswith(".ndjson"):
                    continue

                file_name = blob.name.rsplit(
                    "/",
                    maxsplit=1,
                )[-1]

                result.append(
                    BlobObject(
                        name=blob.name,
                        partition=partition,
                        file_name=file_name,
                    )
                )

        return sorted(
            result,
            key=lambda blob: blob.name,
        )

    def download(
        self,
        blob: BlobObject,
        target: Path,
    ) -> None:
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        blob_client = self._client.get_blob_client(blob.name)

        downloader = blob_client.download_blob()

        with target.open("wb") as stream:
            downloader.readinto(stream)

    def _build_prefix(self, partition: str) -> str:
        year, month, day, hour = partition.strip("/").split("/")
        partition_path = f"year={year}/month={month}/day={day}/hour={hour}"
        parts = [self._folder_name, partition_path]

        return "/".join(part for part in parts if part) + "/"
