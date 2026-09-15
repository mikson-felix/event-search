from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from click.testing import CliRunner

from event_search.cli.commands.clean import clean_command


def make_application() -> SimpleNamespace:
    return SimpleNamespace(
        cache_service=MagicMock(),
        renderer=MagicMock(),
    )


def test_clean_proceeds_when_confirmation_is_accepted() -> None:
    runner = CliRunner()

    app = make_application()

    result = runner.invoke(
        clean_command,
        [],
        input="y\n",
        obj=app,
    )

    assert result.exit_code == 0

    app.cache_service.clean.assert_called_once_with()

    app.renderer.render_clean.assert_called_once_with()


def test_clean_aborts_when_confirmation_is_declined() -> None:
    runner = CliRunner()

    app = make_application()

    result = runner.invoke(
        clean_command,
        [],
        input="n\n",
        obj=app,
    )

    assert result.exit_code == 1

    app.cache_service.clean.assert_not_called()
    app.renderer.render_clean.assert_not_called()


def test_clean_skips_prompt_with_yes_flag() -> None:
    runner = CliRunner()

    app = make_application()

    result = runner.invoke(
        clean_command,
        ["--yes"],
        obj=app,
    )

    assert result.exit_code == 0

    app.cache_service.clean.assert_called_once_with()

    app.renderer.render_clean.assert_called_once_with()


def test_clean_help() -> None:
    runner = CliRunner()

    result = runner.invoke(
        clean_command,
        ["--help"],
    )

    assert result.exit_code == 0

    output = " ".join(result.output.split())

    assert "Remove the local Parquet cache and the local SQLite database" in output
    assert "--yes" in output
