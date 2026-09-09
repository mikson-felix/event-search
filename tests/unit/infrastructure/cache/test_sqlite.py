import sqlite3
from pathlib import Path

from event_search.infrastructure.cache.sqlite import (
    SQLiteConnectionFactory,
)


def test_creates_database_parent_directory(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "nested" / "cache" / "event_search.sqlite"

    SQLiteConnectionFactory(database_path)

    assert database_path.parent.exists()
    assert database_path.exists()


def test_enables_wal_mode(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "event_search.sqlite"

    factory = SQLiteConnectionFactory(database_path)

    with factory.connect() as connection:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]

    assert journal_mode.lower() == "wal"


def test_enables_foreign_keys(
    tmp_path: Path,
) -> None:
    factory = SQLiteConnectionFactory(tmp_path / "event_search.sqlite")

    with factory.connect() as connection:
        foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]

    assert foreign_keys == 1


def test_configures_busy_timeout(
    tmp_path: Path,
) -> None:
    factory = SQLiteConnectionFactory(tmp_path / "event_search.sqlite")

    with factory.connect() as connection:
        busy_timeout = connection.execute("PRAGMA busy_timeout").fetchone()[0]

    assert busy_timeout == 30_000


def test_returns_sqlite_connection(
    tmp_path: Path,
) -> None:
    factory = SQLiteConnectionFactory(tmp_path / "event_search.sqlite")

    connection = factory.connect()

    try:
        assert isinstance(
            connection,
            sqlite3.Connection,
        )
    finally:
        connection.close()
