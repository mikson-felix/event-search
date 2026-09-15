import click

from event_search.bootstrap import Application


@click.command(
    "clean",
    help=(
        "Remove the local Parquet cache and the local SQLite database.\n\n"
        "This deletes all materialized data. Nothing is removed from Azure "
        "Blob Storage; the next `sync` or `search` rebuilds the cache from "
        "scratch."
    ),
)
@click.option(
    "--yes",
    is_flag=True,
    default=False,
    help="Skip the confirmation prompt.",
)
@click.pass_obj
def clean_command(
    app: Application,
    yes: bool,
) -> None:
    if not yes:
        click.confirm(
            "This will delete the local Parquet cache and SQLite database. Continue?",
            abort=True,
        )

    app.cache_service.clean()

    app.renderer.render_clean()
