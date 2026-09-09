from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from click.testing import CliRunner

from event_search.cli.commands.status import status_command


def make_application() -> SimpleNamespace:
    return SimpleNamespace(
        manifest=MagicMock(),
        renderer=MagicMock(),
    )


def test_status_reads_manifest_and_renders_status() -> None:
    runner = CliRunner()

    app = make_application()

    cache_status = MagicMock()

    app.manifest.get_status.return_value = cache_status

    result = runner.invoke(
        status_command,
        [],
        obj=app,
    )

    assert result.exit_code == 0

    app.manifest.get_status.assert_called_once_with()

    app.renderer.render_status.assert_called_once_with(
        cache_status,
    )


def test_status_does_not_use_other_application_services() -> None:
    runner = CliRunner()

    manifest = MagicMock()
    renderer = MagicMock()

    cache_status = MagicMock()
    manifest.get_status.return_value = cache_status

    app = SimpleNamespace(
        manifest=manifest,
        renderer=renderer,
        search_service=MagicMock(),
        sync_service=MagicMock(),
        time_range_resolver=MagicMock(),
        partition_resolver=MagicMock(),
    )

    result = runner.invoke(
        status_command,
        [],
        obj=app,
    )

    assert result.exit_code == 0

    manifest.get_status.assert_called_once_with()
    renderer.render_status.assert_called_once_with(
        cache_status,
    )

    app.search_service.assert_not_called()
    app.sync_service.assert_not_called()
    app.time_range_resolver.assert_not_called()
    app.partition_resolver.assert_not_called()


def test_status_help() -> None:
    runner = CliRunner()

    result = runner.invoke(
        status_command,
        [
            "--help",
        ],
    )

    assert result.exit_code == 0

    output = " ".join(result.output.split())

    assert "Show statistics about the local materialized cache" in output

    assert "Azure Blob Storage is not accessed" in output
