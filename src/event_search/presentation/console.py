from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
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

    def render_results(
        self,
        results: list[SearchSummary],
    ) -> None:
        table = Table(
            title=f"Events ({len(results)})",
            show_lines=False,
        )

        table.add_column(
            "ID",
            style="bold cyan",
            no_wrap=True,
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
        )

        table.add_column(
            "Category",
        )

        table.add_column(
            "Timestamp",
            no_wrap=True,
        )

        table.add_column(
            "Path",
            style="dim",
            no_wrap=True,
        )

        table.add_column(
            "Blob",
            overflow="ellipsis",
        )

        for result in results:
            table.add_row(
                result.event_id,
                result.user_id or "-",
                result.organization_id or "-",
                result.event_name or "-",
                result.category or "-",
                result.timestamp.isoformat(),
                result.locator.blob_partition,
                result.locator.blob_name,
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
