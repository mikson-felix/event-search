from collections.abc import Callable, Iterator
from contextlib import contextmanager

from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)
from rich.status import Status
from rich.table import Table
from rich.text import Text

from event_search.domain.models import (
    CacheStatus,
    EventDetails,
    SearchSummary,
    SyncResult,
    TimeRange,
)


class ConsoleRenderer:
    def __init__(
        self,
        console: Console | None = None,
    ) -> None:
        self._console = console or Console()

    @staticmethod
    def _shorten(
        value: str | None,
        *,
        length: int = 20,
    ) -> str:
        if not value:
            return "-"

        if len(value) <= length:
            return value

        return f"{value[: length - 1]}…"

    def render_range(
        self,
        value: TimeRange,
    ) -> None:
        self._console.print(
            f"[dim]Local:[/] {value.local_from:%Y-%m-%d %H:%M:%S %Z} → {value.local_to:%Y-%m-%d %H:%M:%S %Z}"
        )

        self._console.print(
            f"[dim]UTC:[/]   {value.utc_from:%Y-%m-%d %H:%M:%S %Z} → {value.utc_to:%Y-%m-%d %H:%M:%S %Z}"
        )

        self._console.print()

    @contextmanager
    def track_partitions(
        self,
        total: int,
    ) -> Iterator[Callable[[], None]]:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=self._console,
        ) as progress:
            task_id = progress.add_task(
                "Syncing partitions",
                total=total,
            )

            yield lambda: progress.advance(task_id)

    def status(
        self,
        message: str,
    ) -> Status:
        return self._console.status(message)

    def render_results(
        self,
        results: list[SearchSummary],
    ) -> None:
        table = Table(
            title=f"Events ({len(results)})",
            show_lines=False,
            expand=False,
        )

        table.add_column(
            "ID",
            style="bold cyan",
            no_wrap=True,
            width=36,
        )

        table.add_column(
            "User ID",
            no_wrap=True,
        )

        table.add_column(
            "Organization ID",
            no_wrap=True,
        )

        table.add_column(
            "Event",
            ratio=1,
        )

        table.add_column(
            "Category",
            ratio=1,
        )

        table.add_column(
            "Timestamp (UTC)",
            no_wrap=True,
        )

        for result in results:
            table.add_row(
                result.event_id,
                result.user_id or "-",
                self._shorten(result.organization_id, length=20),
                self._shorten(result.event_name, length=20),
                self._shorten(result.category, length=10),
                result.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            )

        self._console.print(table)

    def render_event(
        self,
        result: EventDetails,
    ) -> None:
        metadata = Text()

        metadata.append(
            "ID:     ",
            style="bold",
        )
        metadata.append(result.event_id)

        metadata.append(
            "\nSource: ",
            style="bold",
        )
        metadata.append(f"{result.blob_partition}/{result.blob_name}")

        metadata.append(
            "\nLine:   ",
            style="bold",
        )
        metadata.append(str(result.source_line))

        self._console.print(
            Panel(
                metadata,
                title="Event",
                title_align="left",
            )
        )

        self._console.print(JSON(result.raw_json))

    def render_status(
        self,
        status: CacheStatus,
    ) -> None:
        table = Table(title="Local cache")

        table.add_column("Metric")

        table.add_column(
            "Value",
            justify="right",
        )

        table.add_row(
            "Cached blobs",
            f"{status.blobs_count:,}",
        )

        table.add_row(
            "Cached events",
            f"{status.events_count:,}",
        )

        table.add_row(
            "Parquet size",
            self._format_size(status.parquet_size_bytes),
        )

        table.add_row(
            "Last materialization",
            (status.last_materialized_at.isoformat() if status.last_materialized_at else "-"),
        )

        self._console.print(table)

    def render_sync(
        self,
        result: SyncResult,
    ) -> None:
        self._console.print(
            "Discovered: "
            f"[bold]{result.discovered}[/], "
            "materialized: "
            f"[green]{result.materialized}[/], "
            "cached: "
            f"[cyan]{result.skipped}[/]"
        )

    def render_clean(
        self,
    ) -> None:
        self._console.print("[green]Local cache cleared.[/] Parquet files and the SQLite database were removed.")

    @staticmethod
    def _format_size(
        value: int,
    ) -> str:
        size = float(value)

        for unit in (
            "B",
            "KB",
            "MB",
            "GB",
            "TB",
        ):
            if size < 1024:
                return f"{size:.1f} {unit}"

            size /= 1024

        return f"{size:.1f} PB"
