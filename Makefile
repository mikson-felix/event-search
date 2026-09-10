.PHONY: install install-completion fmt lint test test-cov check

install:
	uv sync
	uv run pre-commit install
	$(MAKE) install-completion

install-completion:
	@SHELL_NAME=$$(basename "$$SHELL"); \
	if [ "$$SHELL_NAME" = "zsh" ]; then \
		mkdir -p "$$HOME/.zfunc"; \
		_EVENT_SEARCH_COMPLETE=zsh_source uv run event-search \
			> "$$HOME/.zfunc/_event-search"; \
		echo "Installed zsh completion: $$HOME/.zfunc/_event-search"; \
	elif [ "$$SHELL_NAME" = "bash" ]; then \
		mkdir -p "$$HOME/.local/share/bash-completion/completions"; \
		_EVENT_SEARCH_COMPLETE=bash_source uv run event-search \
			> "$$HOME/.local/share/bash-completion/completions/event-search"; \
		echo "Installed bash completion"; \
	elif [ "$$SHELL_NAME" = "fish" ]; then \
		mkdir -p "$$HOME/.config/fish/completions"; \
		_EVENT_SEARCH_COMPLETE=fish_source uv run event-search \
			> "$$HOME/.config/fish/completions/event-search.fish"; \
		echo "Installed fish completion"; \
	else \
		echo "Shell completion skipped: unsupported shell '$$SHELL_NAME'"; \
	fi

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