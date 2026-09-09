import click

from event_search.bootstrap import Application
from event_search.domain.errors import InvalidTimeRangeError
from event_search.domain.models import SearchFilters


@click.command(
    "search",
    help=(
        "Search events in a selected time range.\n\n"
        "Missing immutable NDJSON blobs are downloaded from Azure "
        "Blob Storage and materialized into the local Parquet cache "
        "before the query is executed.\n\n"
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
        "'last 15 minutes', 'last 30 minutes', "
        "'last hour', 'last 2 hours', "
        "'last 2 days', 'today', 'yesterday', "
        "'current hour'."
    ),
)
@click.option(
    "--from",
    "date_from",
    type=str,
    metavar="DATETIME",
    help=("Start of the search interval. Datetime without timezone is interpreted using the local OS timezone."),
)
@click.option(
    "--to",
    "date_to",
    type=str,
    metavar="DATETIME",
    help=("End of the search interval. Requires --from. If omitted, current time is used."),
)
@click.option(
    "--event-id",
    type=str,
    metavar="TEXT",
    help="Filter by exact event ID.",
)
@click.option(
    "--user-id",
    type=str,
    metavar="TEXT",
    help="Filter by exact user ID.",
)
@click.option(
    "--organization-id",
    type=str,
    metavar="TEXT",
    help="Filter by exact organization ID.",
)
@click.option(
    "--event-name",
    type=str,
    metavar="TEXT",
    help="Filter by exact event name.",
)
@click.option(
    "--category",
    type=str,
    metavar="TEXT",
    help="Filter by exact event category.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    metavar="INTEGER",
    default=None,
    help=("Maximum number of returned events. Uses configured default when omitted."),
)
@click.pass_obj
def search_command(
    app: Application,
    shortcut: str | None,
    date_from: str | None,
    date_to: str | None,
    event_id: str | None,
    user_id: str | None,
    organization_id: str | None,
    event_name: str | None,
    category: str | None,
    limit: int | None,
) -> None:
    try:
        time_range = app.time_range_resolver.resolve(
            shortcut=shortcut,
            date_from=date_from,
            date_to=date_to,
        )

    except InvalidTimeRangeError as exc:
        raise click.ClickException(str(exc)) from exc

    resolved_limit = limit if limit is not None else app.settings.search.default_limit

    max_limit = app.settings.search.max_limit

    if resolved_limit > max_limit:
        raise click.BadParameter(
            f"Maximum limit is {max_limit}",
            param_hint="--limit",
        )

    partitions = app.partition_resolver.resolve(time_range)

    app.renderer.render_range(time_range)

    app.sync_service.sync(partitions)

    filters = SearchFilters(
        event_id=event_id,
        user_id=user_id,
        organization_id=organization_id,
        event_name=event_name,
        category=category,
    )

    results = app.search_service.search(
        filters=filters,
        time_range=time_range,
        partitions=partitions,
        limit=resolved_limit,
    )

    app.renderer.render_results(results)
