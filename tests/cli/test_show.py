from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import click
import pytest
from click.shell_completion import CompletionItem
from click.testing import CliRunner

from event_search.cli.commands.show import (
    complete_event_id,
    show_command,
)


def make_context(
    *,
    application: object | None = None,
) -> click.Context:
    command = click.Command(
        name="show",
    )

    context = click.Context(
        command=command,
    )

    context.obj = application

    return context


def test_completion_returns_latest_search_ids() -> None:
    search_service = MagicMock()

    search_service.complete_event_ids.return_value = [
        "event-001",
        "event-002",
    ]

    application = SimpleNamespace(
        search_service=search_service,
    )

    context = make_context(
        application=application,
    )

    parameter = click.Argument(
        [
            "event_id",
        ],
    )

    result = complete_event_id(
        context,
        parameter,
        "event-",
    )

    assert all(isinstance(item, CompletionItem) for item in result)

    assert [item.value for item in result] == [
        "event-001",
        "event-002",
    ]

    search_service.complete_event_ids.assert_called_once_with(
        prefix="event-",
        limit=20,
    )


def test_completion_passes_prefix_to_search_service() -> None:
    search_service = MagicMock()

    search_service.complete_event_ids.return_value = [
        "abc-123",
    ]

    application = SimpleNamespace(
        search_service=search_service,
    )

    context = make_context(
        application=application,
    )

    parameter = click.Argument(
        [
            "event_id",
        ],
    )

    result = complete_event_id(
        context,
        parameter,
        "abc",
    )

    assert [item.value for item in result] == [
        "abc-123",
    ]

    search_service.complete_event_ids.assert_called_once_with(
        prefix="abc",
        limit=20,
    )


def test_completion_returns_empty_list_when_nothing_matches() -> None:
    search_service = MagicMock()

    search_service.complete_event_ids.return_value = []

    application = SimpleNamespace(
        search_service=search_service,
    )

    context = make_context(
        application=application,
    )

    parameter = click.Argument(
        [
            "event_id",
        ],
    )

    result = complete_event_id(
        context,
        parameter,
        "missing",
    )

    assert result == []

    search_service.complete_event_ids.assert_called_once_with(
        prefix="missing",
        limit=20,
    )


def test_completion_builds_application_when_context_has_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    search_service = MagicMock()

    search_service.complete_event_ids.return_value = [
        "event-001",
    ]

    application = SimpleNamespace(
        search_service=search_service,
    )

    monkeypatch.setattr(
        "event_search.cli.commands.show.build_application",
        MagicMock(return_value=application),
    )

    context = make_context(
        application=None,
    )

    parameter = click.Argument(
        [
            "event_id",
        ],
    )

    result = complete_event_id(
        context,
        parameter,
        "event-",
    )

    assert [item.value for item in result] == [
        "event-001",
    ]

    search_service.complete_event_ids.assert_called_once_with(
        prefix="event-",
        limit=20,
    )


def test_show_command_renders_event() -> None:
    runner = CliRunner()

    event = MagicMock()

    search_service = MagicMock()
    search_service.get_from_last_search.return_value = event

    renderer = MagicMock()

    application = SimpleNamespace(
        search_service=search_service,
        renderer=renderer,
    )

    result = runner.invoke(
        show_command,
        [
            "event-001",
        ],
        obj=application,
    )

    assert result.exit_code == 0

    search_service.get_from_last_search.assert_called_once_with(
        "event-001",
    )

    renderer.render_event.assert_called_once_with(
        event,
    )


def test_show_command_fails_when_event_is_not_in_latest_search() -> None:
    runner = CliRunner()

    search_service = MagicMock()
    search_service.get_from_last_search.return_value = None

    renderer = MagicMock()

    application = SimpleNamespace(
        search_service=search_service,
        renderer=renderer,
    )

    result = runner.invoke(
        show_command,
        [
            "missing-event",
        ],
        obj=application,
    )

    assert result.exit_code == 1

    assert "Event 'missing-event' is not available in the latest search results." in result.output

    assert "Run `event-search search ...` first." in result.output

    search_service.get_from_last_search.assert_called_once_with(
        "missing-event",
    )

    renderer.render_event.assert_not_called()


def test_show_command_requires_event_id() -> None:
    runner = CliRunner()

    application = SimpleNamespace(
        search_service=MagicMock(),
        renderer=MagicMock(),
    )

    result = runner.invoke(
        show_command,
        [],
        obj=application,
    )

    assert result.exit_code == 2

    assert "Missing argument 'EVENT_ID'" in result.output


def test_show_help() -> None:
    runner = CliRunner()

    result = runner.invoke(
        show_command,
        [
            "--help",
        ],
    )

    assert result.exit_code == 0

    assert "EVENT_ID" in result.output
    assert "latest search result" in result.output
    assert "shell autocomplete" in result.output
