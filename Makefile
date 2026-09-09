.PHONY: install fmt lint test test-cov check


install:
	uv sync
	uv run pre-commit install


fmt:
	uv run ruff check . --fix
	uv run ruff format .


lint:
	uv run ruff check .
	uv run ruff format --check .


test:
	uv run pytest


test-cov:
	uv run pytest \
		--cov=event_search \
		--cov-report=term-missing \
		--cov-report=html


check: lint test