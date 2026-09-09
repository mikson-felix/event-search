import click
from click.shell_completion import CompletionItem

from event_search.bootstrap import Application, build_application


def complete_event_id(
    context: click.Context,
    param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    del param

    app: Application | None = context.obj

    if app is None:
        # Click's shell completion resolves the context chain without
        # invoking group callbacks, so `cli()` never sets `context.obj`
        # here. Build the application directly instead.
        app = build_application()

    event_ids = app.search_service.complete_event_ids(
        prefix=incomplete,
        limit=20,
    )

    return [CompletionItem(event_id) for event_id in event_ids]


@click.command(
    "show",
    help=(
        "Show the raw JSON representation of an event from the "
        "latest search result set.\n\n"
        "EVENT_ID supports shell autocomplete. Completion candidates "
        "are taken only from the latest search results. Azure Blob "
        "Storage and the complete local Parquet cache are not searched."
    ),
)
@click.argument(
    "event_id",
    metavar="EVENT_ID",
    shell_complete=complete_event_id,
)
@click.pass_obj
def show_command(
    app: Application,
    event_id: str,
) -> None:
    result = app.search_service.get_from_last_search(event_id)

    if result is None:
        raise click.ClickException(
            f"Event '{event_id}' is not available in the latest search results. Run `event-search search ...` first."
        )

    app.renderer.render_event(result)
