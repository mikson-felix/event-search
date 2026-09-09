import click

from event_search.bootstrap import Application


@click.command(
    "status",
    help=(
        "Show statistics about the local materialized cache.\n\n"
        "This command reads only local DuckDB metadata and Parquet "
        "files. Azure Blob Storage is not accessed."
    ),
)
@click.pass_obj
def status_command(
    app: Application,
) -> None:
    status = app.manifest.get_status()

    app.renderer.render_status(status)
