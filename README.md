# Event Search

<p align="center">
  <strong>Fast local search over immutable NDJSON event archives stored in Azure Blob Storage.</strong>
</p>

<p align="center">
  Azure Blob Storage → Parquet Cache → DuckDB → Rich CLI
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/uv-package%20manager-DE5FE9" alt="uv">
  <img src="https://img.shields.io/badge/Azure-Blob%20Storage-0078D4?logo=microsoftazure&logoColor=white" alt="Azure Blob Storage">
  <img src="https://img.shields.io/badge/DuckDB-local%20analytics-FFF000?logo=duckdb&logoColor=black" alt="DuckDB">
  <img src="https://img.shields.io/badge/Apache%20Parquet-cache-50ABF1?logo=apacheparquet&logoColor=white" alt="Apache Parquet">
  <img src="https://img.shields.io/badge/Ruff-lint%20%26%20format-D7FF64?logo=ruff&logoColor=black" alt="Ruff">
  <img src="https://img.shields.io/badge/pre--commit-enabled-FAB040?logo=pre-commit&logoColor=black" alt="pre-commit">
</p>

---

## Overview

**Event Search** is a local CLI application for searching event archives stored as immutable NDJSON files in Azure Blob Storage.

Instead of repeatedly downloading and parsing remote JSON files, the application materializes blobs into a local **Apache Parquet** cache and uses **DuckDB** for fast analytical queries.

```text
                         ┌─────────────────────────┐
                         │   Azure Blob Storage    │
                         │                         │
                         │   immutable *.ndjson    │
                         └────────────┬────────────┘
                                      │
                                      │ list / download
                                      ▼
                              ┌───────────────┐
                              │  SyncService  │
                              └───────┬───────┘
                                      │
                           materialize│missing blobs
                                      ▼
                         ┌─────────────────────────┐
                         │   Local Parquet Cache   │
                         │                         │
                         │ .cache/parquet/...      │
                         └────────────┬────────────┘
                                      │
                                      │ query
                                      ▼
                              ┌───────────────┐
                              │    DuckDB     │
                              └───────┬───────┘
                                      │
                         ┌────────────┴────────────┐
                         │                         │
                         ▼                         ▼
                  ┌─────────────┐       ┌──────────────────────┐
                  │ Rich Table  │       │ latest_search_results│
                  └─────────────┘       └──────────┬───────────┘
                                                   │
                                      ┌────────────┴────────────┐
                                      │                         │
                                      ▼                         ▼
                                show EVENT_ID             shell <TAB>
                                      │
                                      ▼
                               raw event JSON
```

The local cache is disposable. Azure Blob Storage remains the source of truth.

---

## Features

* Search immutable NDJSON event archives stored in Azure Blob Storage
* Automatic local timezone detection
* Local time → UTC partition resolution
* Relative time expressions such as `last hour`, `today`, and `last 2 days`
* Automatic cache-aside synchronization
* Local Parquet cache
* DuckDB-powered event filtering
* Exact filtering by common event fields
* Rich terminal output
* Raw JSON event inspection
* `show EVENT_ID` shell autocomplete
* Local cache statistics
* SAS-based Azure authentication
* Ruff linting and formatting
* pre-commit integration
* uv-based dependency management

---

## Technology Stack

| Technology             | Purpose                                          |
| ---------------------- | ------------------------------------------------ |
| **Python 3.12+**       | Application runtime                              |
| **Azure Blob Storage** | Source of immutable NDJSON event archives        |
| **azure-storage-blob** | Azure Blob Storage client                        |
| **Apache Parquet**     | Local columnar cache format                      |
| **PyArrow**            | NDJSON → Parquet materialization                 |
| **DuckDB**             | Local analytical query engine and metadata store |
| **orjson**             | Fast JSON parsing                                |
| **Click**              | CLI framework                                    |
| **Rich**               | Terminal tables and JSON rendering               |
| **Pydantic Settings**  | Environment-based configuration                  |
| **tzlocal**            | Local OS timezone detection                      |
| **uv**                 | Project and dependency management                |
| **Ruff**               | Linter and formatter                             |
| **pre-commit**         | Git hook automation                              |

---

## Storage Model

Azure contains immutable NDJSON files organized by UTC hour:

```text
<container>/
│
├── 2026/
│   └── 09/
│       └── 09/
│           ├── 08/
│           │   ├── events-001.ndjson
│           │   └── events-002.ndjson
│           │
│           ├── 09/
│           │   └── events-003.ndjson
│           │
│           └── 10/
│               └── events-004.ndjson
```

The partition format is:

```text
YYYY/MM/DD/HH
```

All partition hours are **UTC**.

Blobs are considered immutable after they appear in storage. Event Search therefore does not perform ETag/version comparison or remote overwrite reconciliation.

---

## Local Cache

Downloaded NDJSON files are materialized into Parquet files using approximately the same partition layout:

```text
.cache/
│
├── event_search.duckdb
│
├── parquet/
│   └── 2026/
│       └── 09/
│           └── 09/
│               ├── 08/
│               │   ├── events-001.parquet
│               │   └── events-002.parquet
│               │
│               └── 09/
│                   └── events-003.parquet
│
└── tmp/
```

`event_search.duckdb` stores lightweight application metadata:

```text
┌───────────────────────────┐
│ cached_blobs              │
│                           │
│ Azure blob → Parquet file │
└───────────────────────────┘

┌───────────────────────────┐
│ latest_search_results     │
│                           │
│ last CLI search result    │
│ + event locators          │
└───────────────────────────┘
```

The actual event data remains in Parquet rather than being duplicated into DuckDB.

---

## Indexed Event Fields

During materialization, Event Search extracts a small set of searchable fields.

| Field             | Source                                                         |
| ----------------- | -------------------------------------------------------------- |
| `event_id`        | `event_id`                                                     |
| `timestamp`       | `timestamp`                                                    |
| `user_id`         | `actor.user_id`, fallback `attributes.user_id`                 |
| `organization_id` | `actor.organization_id`, fallback `attributes.organization_id` |
| `event_name`      | `event.name`, fallback `event_name`                            |
| `category`        | `event.category`, fallback `category`                          |

Each Parquet row also contains:

```text
blob_partition
blob_name
source_line
raw_json
```

`raw_json` preserves the original NDJSON event line and is used by the `show` command.

---

# Installation

## Requirements

Make sure the following tools are installed:

```text
Python >= 3.12
uv
make
git
```

Clone the repository:

```bash
git clone <repository-url>
cd event-search
```

Create your environment configuration:

```bash
cp .env.example .env
```

Then install everything:

```bash
make install
```

`make install` runs:

```text
uv sync
    │
    ├── creates .venv
    ├── installs event-search
    ├── installs runtime dependencies
    └── installs development dependencies
              │
              ▼
uv run pre-commit install
              │
              ▼
.git/hooks/pre-commit
```

After installation:

```bash
uv run event-search --help
```

If the virtual environment is activated, you can simply use:

```bash
event-search --help
```

---

# Configuration

Configuration is loaded from environment variables or `.env`.

Example:

```dotenv
EVENT_SEARCH_AZURE__CONTAINER_URL=https://storage-account.blob.core.windows.net/events
EVENT_SEARCH_AZURE__SAS_TOKEN="sv=...&spr=https&sr=c&sp=rl&se=...&sig=..."

EVENT_SEARCH_CACHE__PARQUET_DIR=.cache/parquet
EVENT_SEARCH_CACHE__DATABASE_PATH=.cache/event_search.duckdb
EVENT_SEARCH_CACHE__TEMP_DIR=.cache/tmp

EVENT_SEARCH_SEARCH__DEFAULT_LIMIT=100
EVENT_SEARCH_SEARCH__MAX_LIMIT=10000
```

## Azure SAS permissions

Event Search only needs read access.

The SAS token should contain:

```text
sr=c
sp=rl
spr=https
```

Where:

```text
r = read
l = list
c = container resource
```

The application does not create, modify, or delete Azure blobs.

---

# CLI

The application exposes four commands:

```text
event-search
│
├── search    Search events
├── show      Show raw event JSON
├── sync      Synchronize the local cache
└── status    Show local cache statistics
```

Global options:

```text
--help
--version
```

---

# `search`

Search events in a selected time range.

```bash
event-search search [OPTIONS]
```

If no time range is provided, Event Search searches the **last hour**.

## Parameters

| Parameter                | Type     | Description               |
| ------------------------ | -------- | ------------------------- |
| `--time RANGE`           | text     | Relative time expression  |
| `--from DATETIME`        | datetime | Start of the interval     |
| `--to DATETIME`          | datetime | End of the interval       |
| `--event-id TEXT`        | text     | Exact event ID            |
| `--user-id TEXT`         | text     | Exact user ID             |
| `--organization-id TEXT` | text     | Exact organization ID     |
| `--event-name TEXT`      | text     | Exact event name          |
| `--category TEXT`        | text     | Exact event category      |
| `--limit INTEGER`        | integer  | Maximum number of results |
| `--help`                 | —        | Show command help         |

Filters are currently combined using **AND** and use exact equality.

---

## Search the last hour

```bash
event-search search
```

Equivalent to:

```bash
event-search search --time "last hour"
```

---

## Search the last 30 minutes

```bash
event-search search --time "last 30 minutes"
```

---

## Search the last two hours

```bash
event-search search --time "last 2 hours"
```

---

## Search today

```bash
event-search search --time "today"
```

---

## Search yesterday

```bash
event-search search --time "yesterday"
```

---

## Explicit time range

```bash
event-search search \
    --from "2026-09-09T10:00" \
    --to "2026-09-09T14:00"
```

Datetime values without an explicit timezone are interpreted using the **local OS timezone**.

You can also provide an explicit offset:

```bash
event-search search \
    --from "2026-09-09T10:00+03:00" \
    --to "2026-09-09T14:00+03:00"
```

Or UTC:

```bash
event-search search \
    --from "2026-09-09T07:00Z" \
    --to "2026-09-09T11:00Z"
```

---

## Search by user

```bash
event-search search \
    --time "last 2 days" \
    --user-id "user-123"
```

---

## Search by organization and event

```bash
event-search search \
    --time "today" \
    --organization-id "organization-42" \
    --event-name "LOGIN"
```

Equivalent query semantics:

```text
timestamp >= <from>
AND timestamp < <to>
AND organization_id = 'organization-42'
AND event_name = 'LOGIN'
```

---

## Limit results

```bash
event-search search \
    --time "last 2 days" \
    --limit 500
```

The default and maximum values are configurable:

```dotenv
EVENT_SEARCH_SEARCH__DEFAULT_LIMIT=100
EVENT_SEARCH_SEARCH__MAX_LIMIT=10000
```

---

# Time Expressions

`--time` supports relative expressions.

```text
last 15 minutes
last 30 minutes
last hour
last 2 hours
last 24 hours
last day
last 2 days
today
current date
yesterday
current hour
```

General syntax:

```text
last <N> minute
last <N> minutes

last <N> hour
last <N> hours

last <N> day
last <N> days
```

For example:

```bash
event-search search --time "last 7 days"
```

The application automatically:

```text
             User input
                 │
                 │ local timezone
                 ▼
        ┌─────────────────┐
        │ Local TimeRange │
        └────────┬────────┘
                 │
                 │ astimezone(UTC)
                 ▼
        ┌─────────────────┐
        │  UTC TimeRange  │
        └────────┬────────┘
                 │
                 │ resolve hours
                 ▼
       ┌────────────────────┐
       │ UTC Blob Partitions│
       └────────────────────┘
```

The resolved local and UTC intervals are printed before the search results.

---

# `show`

Display the original JSON for an event returned by the **latest search**.

```bash
event-search show EVENT_ID
```

Example:

```bash
event-search show 550e8400-e29b-41d4-a716-446655440000
```

Output includes source information:

```text
╭─ Event ───────────────────────────────────────────╮
│ ID:     550e8400-e29b-41d4-a716-446655440000     │
│ Source: 2026/09/09/08/events-001.ndjson          │
│ Line:   421                                       │
╰───────────────────────────────────────────────────╯

{
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-09-09T08:42:17Z",
    ...
}
```

`show` deliberately does **not** perform a global Azure or Parquet search.

Instead:

```text
search
  │
  ▼
latest_search_results
  │
  │ event_id → EventLocator
  ▼
show EVENT_ID
  │
  ▼
one exact Parquet file
  │
  ▼
source_line + event_id
  │
  ▼
raw_json
```

This keeps `show` fast and predictable.

---

## Event ID autocomplete

`EVENT_ID` supports shell completion.

After performing:

```bash
event-search search --time "last hour"
```

you can use:

```bash
event-search show <TAB>
```

or:

```bash
event-search show 550<TAB>
```

Completion candidates come exclusively from:

```text
latest_search_results
```

Autocomplete does **not**:

```text
✗ access Azure
✗ scan Parquet files
✗ scan the complete cache
```

It only performs a small prefix query against the local DuckDB metadata database.

---

# `sync`

Synchronize Azure event blobs into the local Parquet cache without executing an event search.

```bash
event-search sync [OPTIONS]
```

## Parameters

| Parameter         | Type     | Description                       |
| ----------------- | -------- | --------------------------------- |
| `--time RANGE`    | text     | Relative synchronization interval |
| `--from DATETIME` | datetime | Start of the interval             |
| `--to DATETIME`   | datetime | End of the interval               |
| `--help`          | —        | Show command help                 |

Without parameters:

```bash
event-search sync
```

synchronizes the last hour.

---

## Synchronize two days

```bash
event-search sync --time "last 2 days"
```

---

## Synchronize an explicit interval

```bash
event-search sync \
    --from "2026-09-08T10:00" \
    --to "2026-09-09T18:00"
```

The synchronization algorithm is intentionally simple because Azure blobs are immutable:

```text
for each UTC partition
          │
          ▼
    list Azure blobs
          │
          ▼
   ┌──────────────────┐
   │ manifest contains│── yes ──► Parquet exists?
   │ blob_name?       │               │
   └────────┬─────────┘               ├── yes ──► skip
            │ no                      │
            │                         └── no
            └──────────────┬─────────────┘
                           ▼
                       download
                           │
                           ▼
                    parse NDJSON
                           │
                           ▼
                    write Parquet
                           │
                           ▼
                    update manifest
```

There is no ETag comparison because blobs are immutable by contract.

---

# `status`

Display information about the local cache.

```bash
event-search status
```

Example:

```text
          Local cache
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Metric               ┃        Value ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ Cached blobs         │          142 │
│ Cached events        │      284,392 │
│ Parquet size         │     84.7 MB  │
│ Last materialization │ 2026-09-09...│
└──────────────────────┴──────────────┘
```

`status` is entirely local:

```text
event-search status
        │
        ▼
     DuckDB
        +
 local filesystem
```

Azure Blob Storage is not accessed.

---

# `--version`

Display the installed application version:

```bash
event-search --version
```

Example:

```text
event-search, version 0.1.0
```

The version has a single source of truth:

```text
pyproject.toml
     │
     │ [project]
     │ version = "0.1.0"
     ▼
Python package metadata
     │
     ▼
importlib.metadata
     │
     ▼
event_search.__version__
     │
     ▼
event-search --version
```

---

# `--help`

Global help:

```bash
event-search --help
```

Command-specific help:

```bash
event-search search --help
event-search show --help
event-search sync --help
event-search status --help
```

---

# Development

Development dependencies are managed by uv using the `dev` dependency group.

```toml
[dependency-groups]
dev = [
    "pre-commit>=4.6,<5",
    "ruff>=0.16,<0.17",
]
```

---

## Make commands

The project provides a small Makefile for common development operations.

```text
make
│
├── install    Install/synchronize the project
│
└── fmt        Lint, autofix and format source files
```

### Install

```bash
make install
```

Equivalent to:

```bash
uv sync
uv run pre-commit install
```

### Format

```bash
make fmt
```

Equivalent to:

```bash
uv run ruff check . --fix
uv run ruff format .
```

The order is intentional:

```text
source code
    │
    ▼
ruff check --fix
    │
    │ lint + safe autofixes
    ▼
ruff format
    │
    │ deterministic formatting
    ▼
clean source code
```

---

# Ruff

Run the linter:

```bash
uv run ruff check .
```

Automatically fix supported violations:

```bash
uv run ruff check . --fix
```

Format the project:

```bash
uv run ruff format .
```

Check formatting without modifying files:

```bash
uv run ruff format --check .
```

The Ruff configuration lives in `pyproject.toml`.

Enabled rule families include:

```text
E     pycodestyle errors
W     pycodestyle warnings
F     Pyflakes
I     import sorting
UP    pyupgrade
B     flake8-bugbear
SIM   flake8-simplify
C4    flake8-comprehensions
PIE   flake8-pie
RUF   Ruff-specific rules
```

---

# pre-commit

Git hooks are installed automatically by:

```bash
make install
```

The configured pipeline is:

```text
git commit
    │
    ▼
┌──────────────────┐
│    pre-commit    │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ ruff-check --fix │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   ruff-format    │
└────────┬─────────┘
         │
         ▼
       commit
```

Run all hooks manually:

```bash
uv run pre-commit run --all-files
```

---

# Project Structure

```text
event-search/
│
├── .env.example
├── .gitignore
├── .pre-commit-config.yaml
├── Makefile
├── README.md
├── pyproject.toml
├── uv.lock
│
└── src/
    └── event_search/
        │
        ├── __init__.py
        ├── __main__.py
        ├── bootstrap.py
        │
        ├── config/
        │   └── settings.py
        │
        ├── domain/
        │   ├── errors.py
        │   ├── models.py
        │   └── ports.py
        │
        ├── application/
        │   ├── search_service.py
        │   ├── sync_service.py
        │   └── time.py
        │
        ├── infrastructure/
        │   │
        │   ├── azure/
        │   │   └── blob_source.py
        │   │
        │   ├── cache/
        │   │   ├── extractor.py
        │   │   ├── manifest.py
        │   │   ├── materializer.py
        │   │   └── search_result_store.py
        │   │
        │   └── query/
        │       ├── duckdb_engine.py
        │       └── parquet_event_reader.py
        │
        ├── presentation/
        │   └── console.py
        │
        └── cli/
            ├── main.py
            │
            └── commands/
                ├── search.py
                ├── show.py
                ├── status.py
                └── sync.py
```

---

# Architecture

The project follows a lightweight ports-and-adapters approach without introducing a DI framework.

```text
┌─────────────────────────────────────────────────────────────┐
│                         CLI                                 │
│                                                             │
│                  Click + Rich                               │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│                                                             │
│       SearchService     SyncService     TimeResolver         │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                       Domain Ports                          │
│                                                             │
│ BlobSource          QueryEngine          Materializer        │
│ ManifestRepository  SearchResultStore    EventDetailsReader  │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                     Infrastructure                          │
│                                                             │
│ Azure Blob      DuckDB      PyArrow      Parquet             │
└─────────────────────────────────────────────────────────────┘
```

Dependencies point inward:

```text
CLI ───────► Application ───────► Domain
                 ▲
                 │
           Infrastructure
```

`bootstrap.py` is the composition root responsible for wiring concrete infrastructure implementations into application services.

---

# Search Lifecycle

A normal search executes the following pipeline:

```text
event-search search --time "last 2 hours"
                    │
                    ▼
             TimeRangeResolver
                    │
                    ▼
          Local timezone interval
                    │
                    ▼
              UTC interval
                    │
                    ▼
          BlobPartitionResolver
                    │
                    ▼
         2026/09/09/18
         2026/09/09/19
         2026/09/09/20
                    │
                    ▼
               SyncService
                    │
           ┌────────┴────────┐
           ▼                 ▼
      cached blobs      missing blobs
           │                 │
           │                 ▼
           │              Azure
           │                 │
           │                 ▼
           │              NDJSON
           │                 │
           │                 ▼
           │              Parquet
           └────────┬────────┘
                    ▼
            DuckDBQueryEngine
                    │
                    ▼
             SearchSummary[]
                    │
           ┌────────┴─────────┐
           ▼                  ▼
      Rich table       latest_search_results
```

---

# Cache Philosophy

The cache is an optimization, not a second source of truth.

```text
              SOURCE OF TRUTH
                    │
                    ▼
           Azure Blob Storage
                    │
                    │ immutable
                    ▼
             Local Parquet
                    │
                    │ disposable
                    ▼
                  DuckDB
```

Deleting `.cache/` is therefore safe:

```bash
rm -rf .cache
```

The required data will be materialized again from Azure during subsequent `search` or `sync` operations.

---

# Current Scope

Event Search intentionally keeps the first version small.

Currently supported:

```text
✓ immutable Azure blobs
✓ NDJSON input
✓ UTC hour partitions
✓ local timezone input
✓ cache-aside synchronization
✓ Parquet materialization
✓ exact field filters
✓ DuckDB queries
✓ latest-result event lookup
✓ raw JSON display
✓ event ID autocomplete
```

Intentionally not implemented yet:

```text
○ LIKE / regex filters
○ OR filter expressions
○ arbitrary JSON path filters
○ cache cleanup/reconciliation
○ remote blob deletion reconciliation
○ streaming Parquet materialization
○ concurrent CLI process coordination
```

These can be introduced independently without changing the core storage model.

---

## License

This project is licensed under the MIT License.
See the [LICENSE](LICENSE) file for details.
