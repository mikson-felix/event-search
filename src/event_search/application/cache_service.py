import shutil
from pathlib import Path


class CacheService:
    def __init__(
        self,
        *,
        parquet_dir: Path,
        database_path: Path,
    ) -> None:
        self._parquet_dir = parquet_dir
        self._database_path = database_path

    def clean(self) -> None:
        shutil.rmtree(self._parquet_dir, ignore_errors=True)

        for path in self._database_files():
            path.unlink(missing_ok=True)

    def _database_files(self) -> list[Path]:
        # SQLite's WAL mode keeps uncheckpointed writes in "-wal"/"-shm"
        # sidecar files next to the main database file; remove those too so
        # no stale cache data survives.
        return [
            self._database_path,
            self._database_path.with_name(f"{self._database_path.name}-wal"),
            self._database_path.with_name(f"{self._database_path.name}-shm"),
        ]
