from unittest.mock import MagicMock

from click.testing import CliRunner

import event_search.cli.main as main_module
from event_search import __version__
from event_search.cli.main import cli


def test_root_help(
    monkeypatch,
) -> None:
    build_application = MagicMock()

    monkeypatch.setattr(
        main_module,
        "build_application",
        build_application,
    )

    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "--help",
        ],
    )

    assert result.exit_code == 0

    assert "Local CLI for searching immutable NDJSON event archives" in result.output

    assert "search" in result.output
    assert "show" in result.output
    assert "sync" in result.output
    assert "status" in result.output

    build_application.assert_not_called()


def test_version(
    monkeypatch,
) -> None:
    build_application = MagicMock()

    monkeypatch.setattr(
        main_module,
        "build_application",
        build_application,
    )

    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "--version",
        ],
    )

    assert result.exit_code == 0

    assert f"event-search, version {__version__}" in result.output

    build_application.assert_not_called()


def test_search_help(
    monkeypatch,
) -> None:
    application = MagicMock()

    monkeypatch.setattr(
        main_module,
        "build_application",
        MagicMock(
            return_value=application,
        ),
    )

    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "search",
            "--help",
        ],
    )

    assert result.exit_code == 0

    assert "Usage:" in result.output
    assert "search" in result.output


def test_sync_help(
    monkeypatch,
) -> None:
    application = MagicMock()

    monkeypatch.setattr(
        main_module,
        "build_application",
        MagicMock(
            return_value=application,
        ),
    )

    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "sync",
            "--help",
        ],
    )

    assert result.exit_code == 0

    assert "Usage:" in result.output
    assert "sync" in result.output


def test_show_help(
    monkeypatch,
) -> None:
    application = MagicMock()

    monkeypatch.setattr(
        main_module,
        "build_application",
        MagicMock(
            return_value=application,
        ),
    )

    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "show",
            "--help",
        ],
    )

    assert result.exit_code == 0

    assert "Usage:" in result.output
    assert "EVENT_ID" in result.output


def test_status_help(
    monkeypatch,
) -> None:
    application = MagicMock()

    monkeypatch.setattr(
        main_module,
        "build_application",
        MagicMock(
            return_value=application,
        ),
    )

    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "status",
            "--help",
        ],
    )

    assert result.exit_code == 0

    assert "Usage:" in result.output
    assert "status" in result.output
