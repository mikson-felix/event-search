import click

from event_search.bootstrap import Application
from event_search.domain.errors import InvalidTimeRangeError


@click.command(
    "sync",
    help=(
        "Synchronize immutable NDJSON blobs into the local "
        "Parquet cache.\n\n"
        "Only blobs belonging to the selected UTC partitions are "
        "inspected. Already materialized blobs are skipped.\n\n"
        "If no time range is specified, the last hour is used."
    ),
)
@click.option(
    "--time",
    "shortcut",
    type=str,
    metavar="RANGE",
    help=(
        "Relative time range. Examples: "
        "'last 15 minutes', 'last hour', "
        "'last 2 hours', 'last 2 days', "
        "'today', 'yesterday', 'current hour'."
    ),
)
@click.option(
    "--from",
    "date_from",
    type=str,
    metavar="DATETIME",
    help=(
        "Start of the synchronization interval. Datetime without timezone is interpreted using the local OS timezone."
    ),
)
@click.option(
    "--to",
    "date_to",
    type=str,
    metavar="DATETIME",
    help=("End of the synchronization interval. Requires --from. If omitted, current time is used."),
)
@click.pass_obj
def sync_command(
    app: Application,
    shortcut: str | None,
    date_from: str | None,
    date_to: str | None,
) -> None:
    try:
        time_range = app.time_range_resolver.resolve(
            shortcut=shortcut,
            date_from=date_from,
            date_to=date_to,
        )

    except InvalidTimeRangeError as exc:
        raise click.ClickException(str(exc)) from exc

    partitions = app.partition_resolver.resolve(time_range)

    app.renderer.render_range(time_range)

    result = app.sync_service.sync(partitions)

    app.renderer.render_sync(result)
