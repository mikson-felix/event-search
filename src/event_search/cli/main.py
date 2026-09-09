import click

from event_search import __version__
from event_search.bootstrap import build_application
from event_search.cli.commands import (
    search_command,
    show_command,
    status_command,
    sync_command,
)


@click.group(
    help=(
        "Local CLI for searching immutable NDJSON event archives "
        "stored in Azure Blob Storage.\n\n"
        "The application resolves local time ranges to UTC partitions, "
        "synchronizes missing blobs into a local Parquet cache and "
        "queries the cache using DuckDB."
    ),
)
@click.version_option(
    version=__version__,
    prog_name="event-search",
)
@click.pass_context
def cli(
    context: click.Context,
) -> None:
    context.obj = build_application()


cli.add_command(search_command)
cli.add_command(show_command)
cli.add_command(sync_command)
cli.add_command(status_command)
