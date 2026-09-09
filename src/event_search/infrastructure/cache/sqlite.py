import sqlite3
from pathlib import Path


class SQLiteConnectionFactory:
    def __init__(
        self,
        database_path: Path,
    ) -> None:
        self._database_path = database_path.resolve()

        self._database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._initialize()

    def connect(
        self,
    ) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database_path,
            timeout=30,
        )

        connection.execute("PRAGMA foreign_keys = ON")

        connection.execute("PRAGMA busy_timeout = 30000")

        return connection

    def _initialize(
        self,
    ) -> None:
        with sqlite3.connect(
            self._database_path,
            timeout=30,
        ) as connection:
            connection.execute("PRAGMA journal_mode = WAL")

            connection.execute("PRAGMA synchronous = NORMAL")

            connection.execute("PRAGMA foreign_keys = ON")

            connection.execute("PRAGMA busy_timeout = 30000")
