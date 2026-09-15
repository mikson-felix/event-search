from pathlib import Path

from event_search.application.cache_service import CacheService


def make_service(
    tmp_path: Path,
) -> tuple[CacheService, Path, Path]:
    parquet_dir = tmp_path / "parquet"
    database_path = tmp_path / "event_search.sqlite"

    return (
        CacheService(
            parquet_dir=parquet_dir,
            database_path=database_path,
        ),
        parquet_dir,
        database_path,
    )


def test_clean_removes_parquet_directory(
    tmp_path: Path,
) -> None:
    service, parquet_dir, _ = make_service(tmp_path)

    (parquet_dir / "2026/09/10/08").mkdir(parents=True)

    (parquet_dir / "2026/09/10/08" / "part-1.parquet").write_bytes(b"data")

    service.clean()

    assert not parquet_dir.exists()


def test_clean_removes_database_file_and_wal_shm_sidecars(
    tmp_path: Path,
) -> None:
    service, _, database_path = make_service(tmp_path)

    database_path.write_bytes(b"db")

    wal_path = database_path.with_name(f"{database_path.name}-wal")
    shm_path = database_path.with_name(f"{database_path.name}-shm")

    wal_path.write_bytes(b"wal")
    shm_path.write_bytes(b"shm")

    service.clean()

    assert not database_path.exists()
    assert not wal_path.exists()
    assert not shm_path.exists()


def test_clean_does_not_raise_when_nothing_exists(
    tmp_path: Path,
) -> None:
    service, parquet_dir, database_path = make_service(tmp_path)

    assert not parquet_dir.exists()
    assert not database_path.exists()

    service.clean()
