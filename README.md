<h1 align="center">Event Search</h1>

<p align="center">
  <strong>Fast local search over immutable NDJSON event archives stored in Azure Blob Storage.</strong>
</p>

<p align="center">
  Azure Blob Storage → Compacted Parquet Cache → DuckDB → Rich CLI
  <br>
  SQLite Control Plane
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/uv-package%20manager-DE5FE9" alt="uv">
  <img src="https://img.shields.io/badge/Azure-Blob%20Storage-0078D4?logo=microsoftazure&logoColor=white" alt="Azure Blob Storage">
  <img src="https://img.shields.io/badge/DuckDB-local%20analytics-FFF000?logo=duckdb&logoColor=black" alt="DuckDB">
  <img src="https://img.shields.io/badge/SQLite-control%20plane-003B57?logo=sqlite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/badge/Apache%20Parquet-cache-50ABF1?logo=apacheparquet&logoColor=white" alt="Apache Parquet">
  <img src="https://img.shields.io/badge/Loguru-logging-4B8BBE" alt="Loguru">
  <img src="https://img.shields.io/badge/Ruff-lint%20%26%20format-D7FF64?logo=ruff&logoColor=black" alt="Ruff">
  <img src="https://img.shields.io/badge/pre--commit-enabled-FAB040?logo=pre-commit&logoColor=black" alt="pre-commit">
</p>

---

## Overview

**Event Search** is a local CLI application for searching immutable NDJSON event archives stored in Azure Blob Storage.

The application uses a cache-aside strategy:

1. Resolve the requested local time interval.
2. Convert it to UTC.
3. Resolve UTC-hour partitions.
4. List Azure blobs for each partition.
5. Check the local SQLite manifest.
6. Download only previously unprocessed blobs.
7. Compact multiple NDJSON files into larger Parquet files.
8. Query local Parquet files using DuckDB.

The architecture deliberately separates mutable metadata from analytical event data:

```text
Source of truth
───────────────
Azure Blob Storage

Control plane
─────────────
SQLite

Data plane
──────────
Parquet + DuckDB
```

Azure Blob Storage always remains the source of truth.

The entire local `.cache/` directory is disposable and can be rebuilt from Azure.

---

## High-Level Architecture

```text
                         ┌─────────────────────────────┐
                         │     Azure Blob Storage      │
                         │                             │
                         │     immutable *.ndjson      │
                         └──────────────┬──────────────┘
                                        │
                                        │ list / download
                                        ▼
                                ┌───────────────┐
                                │  SyncService  │
                                └───────┬───────┘
                                        │
                             missing blobs only
                                        │
                                        ▼
                            ┌───────────────────────┐
                            │ NDJSON grouping       │
                            │ target batch size     │
                            └───────────┬───────────┘
                                        │
                                        │ materialize
                                        ▼
                         ┌─────────────────────────────┐
                         │   Local Parquet Cache       │
                         │                             │
                         │   part-<uuid>.parquet       │
                         └──────────────┬──────────────┘
                                        │
                                        │ analytical query
                                        ▼
                                ┌───────────────┐
                                │    DuckDB     │
                                └───────┬───────┘
                                        │
                         ┌──────────────┴──────────────┐
                         │                             │
                         ▼                             ▼
                  ┌─────────────┐            ┌──────────────────────┐
                  │ Rich Table  │            │ latest_search_results│
                  └─────────────┘            │       SQLite         │
                                             └──────────┬───────────┘
                                                        │
                                         ┌──────────────┴──────────────┐
                                         │                             │
                                         ▼                             ▼
                                   show EVENT_ID                  shell <TAB>
                                         │
                                         ▼
                                  raw event JSON
```

---

## Features

- Search immutable NDJSON event archives stored in Azure Blob Storage
- Hive-style Azure Blob partition discovery
- Automatic local timezone detection
- Local time → UTC partition resolution
- Relative time expressions such as `last hour`, `today`, and `last 2 days`
- Automatic cache-aside synchronization
- Concurrent synchronization of UTC-hour partitions
- Incremental synchronization of only missing Azure blobs
- NDJSON compaction into larger Parquet files
- Configurable target Parquet batch size
- Batched Parquet writes using PyArrow
- SQLite-backed cache manifest
- SQLite-backed latest search results
- DuckDB-powered analytical queries over local Parquet files
- Exact filtering by common event fields
- Rich terminal output
- Raw JSON event inspection
- `show EVENT_ID` shell autocomplete
- Local cache statistics
- Configurable Loguru logging
- SAS-based Azure authentication
- Ruff linting and formatting
- pytest + coverage
- pre-commit integration
- uv-based dependency management

---

## Technology Stack

| Technology | Purpose |
| --- | --- |
| **Python 3.12+** | Application runtime |
| **Azure Blob Storage** | Source of immutable NDJSON event archives |
| **azure-storage-blob** | Azure Blob Storage client |
| **Apache Parquet** | Local columnar event cache |
| **PyArrow** | NDJSON → Parquet materialization |
| **DuckDB** | Analytical query engine over local Parquet |
| **SQLite** | Mutable local control-plane metadata |
| **orjson** | Fast JSON parsing |
| **Click** | CLI framework |
| **Rich** | Terminal tables and JSON rendering |
| **Loguru** | Application logging |
| **Pydantic Settings** | Environment-based configuration |
| **tzlocal** | Local OS timezone detection |
| **pytest / pytest-cov** | Automated tests and coverage |
| **uv** | Project and dependency management |
| **Ruff** | Linter and formatter |
| **pre-commit** | Git hook automation |

---

# Storage Model

## Azure Blob Layout

Azure contains immutable NDJSON files organized by UTC hour using Hive-style partitions:

```text
<container>/
│
└── activity-logs/
    │
    └── year=2026/
        │
        └── month=09/
            │
            └── day=09/
                │
                ├── hour=08/
                │   ├── events-001.ndjson
                │   └── events-002.ndjson
                │
                ├── hour=09/
                │   └── events-003.ndjson
                │
                └── hour=10/
                    └── events-004.ndjson
```

The physical Azure partition layout is:

```text
<folder_name>/year=YYYY/month=MM/day=DD/hour=HH/
```

For example:

```text
activity-logs/year=2026/month=09/day=09/hour=08/
```

All partition hours are **UTC**.

---

## Logical Partition Format

The application does not expose the Azure-specific Hive representation outside the Azure adapter.

Internally, partitions use a neutral format:

```text
YYYY/MM/DD/HH
```

For example:

```text
logical partition:

2026/09/09/08
```

The Azure adapter maps it to:

```text
activity-logs/year=2026/month=09/day=09/hour=08/
```

The same logical partition is used locally:

```text
.cache/parquet/2026/09/09/08/
```

This keeps application and domain layers independent from the physical Azure storage layout.

---

## Immutable Blob Contract

Azure blobs are considered immutable after they appear in storage.

Because of this contract, Event Search does not perform:

```text
ETag comparison

blob version reconciliation

remote overwrite detection
```

A blob is considered already processed when:

```text
SQLite contains its blob_name

AND

the referenced Parquet file still exists locally
```

---

# Local Cache

The local cache contains two different types of state:

```text
.cache/
│
├── event_search.sqlite
│
├── parquet/
│   └── 2026/
│       └── 09/
│           └── 09/
│               ├── 08/
│               │   ├── part-<uuid>.parquet
│               │   └── part-<uuid>.parquet
│               │
│               └── 09/
│                   └── part-<uuid>.parquet
│
└── tmp/
```

---

## Control Plane — SQLite

SQLite stores mutable application metadata.

It does **not** store event payloads.

The main tables are:

```text
┌──────────────────────────────────────────┐
│ processed_blobs                          │
│                                          │
│ blob_name                                │
│ parquet_path                             │
│ materialized_at                          │
└──────────────────────────────────────────┘

┌──────────────────────────────────────────┐
│ parquet_files                            │
│                                          │
│ parquet_path                             │
│ partition                                │
│ events_count                             │
│ materialized_at                          │
└──────────────────────────────────────────┘

┌──────────────────────────────────────────┐
│ latest_search_results                    │
│                                          │
│ latest CLI search result                 │
│ + EventLocator                           │
└──────────────────────────────────────────┘
```

Multiple Azure blobs can reference the same Parquet file:

```text
001.ndjson ─┐
002.ndjson ─┼────► part-a.parquet
003.ndjson ─┘

004.ndjson ─┐
005.ndjson ─┴────► part-b.parquet
```

SQLite therefore acts as the application **control plane**.

The database is configured to use WAL mode for lightweight concurrent access.

---

## Data Plane — DuckDB + Parquet

Actual event data is stored in Parquet.

DuckDB is used as an ephemeral analytical query engine:

```text
Parquet files
     │
     ▼
   DuckDB
     │
     ▼
 filters / sort / limit
     │
     ▼
 SearchSummary[]
```

DuckDB does not own the cache metadata database.

The storage responsibilities are deliberately separated:

```text
Azure Blob Storage  → source of truth

SQLite              → mutable control-plane metadata

Parquet             → local columnar event cache

DuckDB              → analytical query engine
```

---

# Parquet Compaction

Event Search does not create one Parquet file for every Azure NDJSON blob.

Instead, missing NDJSON files from the same UTC-hour partition are grouped before materialization.

Example:

```text
001.ndjson ─┐
002.ndjson ─┼────► part-550e8400-....parquet
003.ndjson ─┘

004.ndjson ─┐
005.ndjson ─┴────► part-6ba7b810-....parquet
```

This reduces the number of small Parquet files and improves local analytical query performance.

---

## Target Parquet Size

The approximate grouping threshold is configured using:

```dotenv
EVENT_SEARCH_SYNC__TARGET_PARQUET_SIZE_MB=128
```

The value represents the approximate total size of the **input NDJSON files** assigned to one materialization group.

It is not a hard limit on the final compressed Parquet file.

For example:

```text
NDJSON input batch
≈ 128 MB

        │
        │ PyArrow + Zstandard compression
        ▼

Parquet output
≈ 15–40 MB
```

The actual ratio depends on the structure and repetition level of event data.

---

## Incremental Materialization

Existing Parquet files are not rewritten during normal synchronization.

Suppose the first sync processes:

```text
001.ndjson
002.ndjson
003.ndjson
```

and creates:

```text
part-a.parquet
```

Later Azure receives:

```text
004.ndjson
```

The next sync creates:

```text
part-b.parquet
```

instead of rebuilding:

```text
part-a.parquet
```

So the cache remains append-oriented:

```text
existing Parquet
      │
      ├── unchanged
      │
new blobs
      │
      ▼
new Parquet parts
```

---

## Memory Usage During Materialization

Event Search does not need to load an entire large materialization group into Python objects before writing Parquet.

Rows are buffered and written through `PyArrow ParquetWriter`.

Conceptually:

```text
NDJSON stream
     │
     ▼
small row batch
     │
     ▼
Parquet row group
     │
     ▼
next row batch
```

This keeps memory usage bounded while processing larger NDJSON groups.

---

# Indexed Event Fields

During materialization, Event Search extracts a small set of searchable fields.

| Field | Source |
| --- | --- |
| `event_id` | `event_id` |
| `timestamp` | `timestamp` |
| `user_id` | `actor.user_id`, fallback `attributes.user_id` |
| `organization_id` | `actor.organization_id`, fallback `attributes.organization_id` |
| `event_name` | `event.name`, fallback `event_name` |
| `category` | `event.category`, fallback `category` |

Each Parquet row also contains provenance fields:

```text
blob_partition
blob_name
source_line
raw_json
```

For example:

```text
part-a.parquet

row 1
    blob_name = events-001.ndjson
    source_line = 1

row 2
    blob_name = events-001.ndjson
    source_line = 2

row 3
    blob_name = events-002.ndjson
    source_line = 1
```

Compaction therefore does not remove information about the original Azure source.

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

Create the environment configuration:

```bash
cp .env.example .env
```

Install the project:

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

If the virtual environment is activated:

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
EVENT_SEARCH_AZURE__FOLDER_NAME=activity-logs

EVENT_SEARCH_CACHE__PARQUET_DIR=.cache/parquet
EVENT_SEARCH_CACHE__DATABASE_PATH=.cache/event_search.sqlite
EVENT_SEARCH_CACHE__TEMP_DIR=.cache/tmp

EVENT_SEARCH_SEARCH__DEFAULT_LIMIT=100
EVENT_SEARCH_SEARCH__MAX_LIMIT=10000

EVENT_SEARCH_SYNC__CONCURRENCY=4
EVENT_SEARCH_SYNC__TARGET_PARQUET_SIZE_MB=128

EVENT_SEARCH_LOGGING__LEVEL=INFO
```

---

## Azure Settings

```dotenv
EVENT_SEARCH_AZURE__CONTAINER_URL=https://storage-account.blob.core.windows.net/events
EVENT_SEARCH_AZURE__SAS_TOKEN="..."
EVENT_SEARCH_AZURE__FOLDER_NAME=activity-logs
```

`CONTAINER_URL` should point to the Azure Blob container.

The event folder must be configured separately using:

```dotenv
EVENT_SEARCH_AZURE__FOLDER_NAME=activity-logs
```

Do not include the event folder in `CONTAINER_URL`.

---

## Azure SAS Permissions

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

## Cache Settings

```dotenv
EVENT_SEARCH_CACHE__PARQUET_DIR=.cache/parquet
EVENT_SEARCH_CACHE__DATABASE_PATH=.cache/event_search.sqlite
EVENT_SEARCH_CACHE__TEMP_DIR=.cache/tmp
```

The SQLite database stores control-plane metadata.

Parquet contains event data.

Temporary downloaded NDJSON files are stored under:

```text
.cache/tmp
```

and deleted after materialization.

---

## Search Settings

```dotenv
EVENT_SEARCH_SEARCH__DEFAULT_LIMIT=100
EVENT_SEARCH_SEARCH__MAX_LIMIT=10000
```

These control the default and maximum number of returned search rows.

---

## Sync Settings

```dotenv
EVENT_SEARCH_SYNC__CONCURRENCY=4
EVENT_SEARCH_SYNC__TARGET_PARQUET_SIZE_MB=128
```

`CONCURRENCY` controls how many UTC-hour partitions can be synchronized simultaneously.

For example:

```text
2026/09/09/08 ─┐
2026/09/09/09 ─┼── ThreadPoolExecutor
2026/09/09/10 ─┤
2026/09/09/11 ─┘
```

Each worker owns one logical hour partition.

The Azure Blob adapter currently uses the synchronous Azure SDK, so synchronization uses `ThreadPoolExecutor` rather than asyncio.

`TARGET_PARQUET_SIZE_MB` controls the approximate NDJSON input size grouped into one generated Parquet file.

---

# Logging

Logging is implemented using Loguru.

The minimum logging level is controlled through:

```dotenv
EVENT_SEARCH_LOGGING__LEVEL=INFO
```

For troubleshooting:

```dotenv
EVENT_SEARCH_LOGGING__LEVEL=DEBUG
```

Typical debug messages include:

```text
Azure partition listing
Azure blob discovery
cache hit
cache miss
blob download
partition synchronization state
Parquet grouping
source NDJSON size
generated Parquet size
DuckDB query execution
```

For example:

```text
DEBUG | Listing Azure blobs: partition=2026/09/09/08
DEBUG | Azure blobs discovered: partition=2026/09/09/08, count=87
DEBUG | Cache hit: blob=...
DEBUG | Cache miss: blob=...
DEBUG | Sync partition state: discovered=87, cached=80, missing=7
DEBUG | Materialization plan: source_blobs=7, groups=1
DEBUG | Parquet created: path=..., events=...
DEBUG | DuckDB search completed: results=100
```

Logs are written to `stderr`.

Normal CLI output remains on `stdout`.

Credentials such as SAS tokens should never be logged.

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

---

## Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `--time RANGE` | text | Relative time expression |
| `--from DATETIME` | datetime | Start of the interval |
| `--to DATETIME` | datetime | End of the interval |
| `--event-id TEXT` | text | Exact event ID |
| `--user-id TEXT` | text | Exact user ID |
| `--organization-id TEXT` | text | Exact organization ID |
| `--event-name TEXT` | text | Exact event name |
| `--category TEXT` | text | Exact event category |
| `--limit INTEGER` | integer | Maximum number of results |
| `--help` | — | Show command help |

Filters are combined using **AND** and currently use exact equality.

---

## Search the Last Hour

```bash
event-search search
```

Equivalent to:

```bash
event-search search --time "last hour"
```

---

## Search the Last 30 Minutes

```bash
event-search search --time "last 30 minutes"
```

---

## Search the Last Two Hours

```bash
event-search search --time "last 2 hours"
```

---

## Search Today

```bash
event-search search --time "today"
```

---

## Search Yesterday

```bash
event-search search --time "yesterday"
```

---

## Explicit Time Range

```bash
event-search search \
    --from "2026-09-09T10:00" \
    --to "2026-09-09T14:00"
```

Datetime values without an explicit timezone are interpreted using the **local OS timezone**.

An explicit offset can also be provided:

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

## Search by User

```bash
event-search search \
    --time "last 2 days" \
    --user-id "user-123"
```

---

## Search by Organization and Event

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

## Limit Results

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

Example:

```bash
event-search search --time "last 7 days"
```

The application automatically performs:

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

The resolved local and UTC intervals are printed before search results.

---

# Search Lifecycle

A normal search follows this pipeline:

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
         logical UTC partitions
                    │
         ┌──────────┼──────────┐
         ▼          ▼          ▼
2026/09/09/18  /19       /20
         │          │          │
         └──────────┼──────────┘
                    ▼
               SyncService
                    │
                    ▼
             Azure list blobs
                    │
                    ▼
             SQLite manifest
                    │
           ┌────────┴────────┐
           ▼                 ▼
      cached blobs      missing blobs
           │                 │
           │                 ▼
           │          Azure download
           │                 │
           │                 ▼
           │          NDJSON grouping
           │                 │
           │                 ▼
           │          compacted Parquet
           │                 │
           │                 ▼
           │          SQLite manifest
           │
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
                              │
                              ▼
                            SQLite
```

A repeated search still checks Azure for newly arrived blobs, but previously materialized blobs are not downloaded again.

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

Example output:

```text
╭─ Event ───────────────────────────────────────────╮
│ ID:     550e8400-e29b-41d4-a716-446655440000      │
│ Source: 2026/09/09/08/events-001.ndjson           │
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
event_id + source_line
  │
  ▼
raw_json
```

This keeps `show` fast and predictable.

---

## Event ID Autocomplete

`EVENT_ID` supports shell completion.

After:

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

It only performs a small prefix lookup against the local SQLite metadata database.

---

# `sync`

Synchronize Azure event blobs into the local Parquet cache without executing an event query.

```bash
event-search sync [OPTIONS]
```

---

## Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `--time RANGE` | text | Relative synchronization interval |
| `--from DATETIME` | datetime | Start of the interval |
| `--to DATETIME` | datetime | End of the interval |
| `--help` | — | Show command help |

Without parameters:

```bash
event-search sync
```

synchronizes the last hour.

---

## Synchronize Two Days

```bash
event-search sync --time "last 2 days"
```

---

## Synchronize an Explicit Interval

```bash
event-search sync \
    --from "2026-09-08T10:00" \
    --to "2026-09-09T18:00"
```

---

## Synchronization Algorithm

Synchronization is performed per UTC-hour partition.

```text
              UTC partitions
                    │
                    ▼
          ThreadPoolExecutor
                    │
         ┌──────────┼──────────┐
         ▼          ▼          ▼
       hour A     hour B     hour C
         │          │          │
         ▼          ▼          ▼
      Azure list Azure list Azure list
         │          │          │
         ▼          ▼          ▼
        SQLite manifest lookup
                    │
            ┌───────┴────────┐
            │                │
            ▼                ▼
          cached           missing
            │                │
            ▼                ▼
           skip           download
                             │
                             ▼
                      group by target
                             │
                             ▼
                      ParquetWriter
                             │
                             ▼
                   part-<uuid>.parquet
                             │
                             ▼
                     SQLite manifest
```

There is no ETag comparison because blobs are immutable by contract.

---

## Cache Hit Semantics

A cache hit requires both:

```text
processed_blobs contains blob_name

AND

referenced parquet_path exists
```

If the manifest entry does not exist:

```text
cache miss
reason = manifest_entry_missing
```

If SQLite contains the entry but the physical Parquet file was removed:

```text
cache miss
reason = parquet_missing
```

The blob will then be downloaded and materialized again.

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
│ Parquet size         │      84.7 MB │
│ Last materialization │ 2026-09-09...│
└──────────────────────┴──────────────┘
```

`status` is entirely local:

```text
event-search status
        │
        ▼
      SQLite
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
     │ version = "..."
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

Development dependencies are managed by uv.

Install the project and development tools:

```bash
make install
```

---

## Make Commands

The project provides a Makefile for common development operations.

```text
make
│
├── install      Install/synchronize dependencies
├── fmt          Autofix lint issues and format code
├── lint         Check linting and formatting
├── test         Run the complete test suite
├── test-cov     Run tests with coverage reports
└── check        Run lint + tests
```

---

## Install

```bash
make install
```

Equivalent to:

```bash
uv sync

uv run pre-commit install
```

---

## Format

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

## Lint

```bash
make lint
```

Equivalent to:

```bash
uv run ruff check .

uv run ruff format --check .
```

---

## Tests

Run the complete suite:

```bash
make test
```

Equivalent to:

```bash
uv run pytest
```

---

## Coverage

Run tests with coverage:

```bash
make test-cov
```

Equivalent to:

```bash
uv run pytest \
    --cov=event_search \
    --cov-report=term-missing \
    --cov-report=html
```

The test configuration requires at least:

```text
90% coverage
```

---

## Full Check

Run linting and tests:

```bash
make check
```

Equivalent to:

```text
make lint
     │
     ▼
make test
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

Enabled rule families may include:

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
├── tests/
│   ├── integration/
│   └── unit/
│
└── src/
    └── event_search/
        │
        ├── __init__.py
        ├── __main__.py
        ├── bootstrap.py
        ├── logging.py
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
        │   │   ├── materializer.py
        │   │   ├── sqlite.py
        │   │   ├── sqlite_manifest.py
        │   │   └── sqlite_search_result_store.py
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

The project follows a lightweight ports-and-adapters approach without introducing a dependency injection framework.

```text
┌─────────────────────────────────────────────────────────────┐
│                         CLI                                 │
│                                                             │
│                      Click + Rich                           │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│                                                             │
│       SearchService     SyncService     TimeResolver        │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                       Domain Ports                          │
│                                                             │
│ BlobSource          QueryEngine          Materializer       │
│ ManifestRepository  SearchResultStore    EventDetailsReader │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                     Infrastructure                          │
│                                                             │
│ Azure Blob       SQLite       PyArrow       DuckDB          │
│ source           metadata     materialize   analytics       │
│                                 │             │             │
│                                 └── Parquet ──┘             │
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

# Cache Philosophy

The cache is an optimization, not a second source of truth.

```text
                  SOURCE OF TRUTH
                        │
                        ▼
               Azure Blob Storage
                        │
                        │ immutable blobs
                        ▼
              ┌─────────────────────┐
              │    LOCAL CACHE      │
              │                     │
              │ Parquet     SQLite  │
              │ event data  metadata│
              └─────────┬───────────┘
                        │
                        ▼
                      DuckDB
                        │
                        ▼
                      Search
```

Deleting `.cache/` is therefore safe:

```bash
rm -rf .cache
```

The required event data and metadata will be reconstructed during subsequent `search` or `sync` operations.

---

# Cache-Aside Behavior

The local cache prevents repeated blob downloads, but a search may still list Azure blobs.

First search:

```text
Azure list
    │
    ▼
manifest miss
    │
    ▼
download
    │
    ▼
Parquet
    │
    ▼
SQLite manifest
    │
    ▼
DuckDB search
```

Second search over the same partition:

```text
Azure list
    │
    ▼
manifest hit
    │
    ▼
skip download
    │
    ▼
existing Parquet
    │
    ▼
DuckDB search
```

This allows Event Search to detect new immutable blobs while avoiding unnecessary re-downloads.

---

# Concurrency Model

Synchronization concurrency is partition-based.

```text
ThreadPoolExecutor
        │
        ├── worker 1 → 2026/09/09/08
        ├── worker 2 → 2026/09/09/09
        ├── worker 3 → 2026/09/09/10
        └── worker 4 → 2026/09/09/11
```

A single worker processes one UTC-hour partition.

The concurrency level is configured using:

```dotenv
EVENT_SEARCH_SYNC__CONCURRENCY=4
```

This improves Azure listing, download, and materialization throughput without introducing async infrastructure into the CLI application.

---

# Failure and Atomicity Model

Parquet files are first written to a temporary path.

Conceptually:

```text
NDJSON
   │
   ▼
part-<uuid>.parquet.tmp
   │
   │ successful write
   ▼
atomic filesystem rename
   │
   ▼
part-<uuid>.parquet
   │
   ▼
SQLite manifest update
```

Temporary files are removed after successful or failed materialization.

A blob is not considered cached until the local Parquet file exists and the manifest references it.

---

# Current Scope

Currently supported:

```text
✓ immutable Azure blobs
✓ Hive-style Azure partition layout
✓ storage-neutral logical partitions
✓ NDJSON input
✓ UTC-hour partition resolution
✓ local timezone input
✓ relative time expressions
✓ cache-aside synchronization
✓ concurrent partition synchronization
✓ incremental blob synchronization
✓ NDJSON compaction
✓ configurable materialization target size
✓ batched PyArrow Parquet writes
✓ SQLite metadata
✓ Parquet event cache
✓ DuckDB analytical queries
✓ exact field filters
✓ latest-result event lookup
✓ raw JSON display
✓ event ID autocomplete
✓ configurable Loguru logging
✓ local cache status
```

Intentionally not implemented yet:

```text
○ LIKE / regex filters

○ OR filter expressions

○ arbitrary JSON path filters

○ cache cleanup / orphan reconciliation

○ remote blob deletion reconciliation

○ cross-process partition locking

○ explicit local Parquet re-compaction
```

These features can be introduced independently without changing the core storage model.

---

# Known Concurrency Limitation

Concurrency inside a single Event Search process is partition-safe because one worker owns one partition.

However, running multiple independent `sync` processes for the same partition at the same time is not currently coordinated.

For example:

```text
process A
    │
    └── sees blob X as missing

process B
    │
    └── sees blob X as missing
```

Both processes may materialize the same source blob into different local Parquet files.

Generated Parquet files use collision-resistant names, so files are not overwritten, but duplicate physical materialization is still possible.

Cross-process partition locking or SQLite-backed partition leasing is intentionally left for a later hardening step.

---

# License

This project is licensed under the MIT License.

See the [LICENSE](LICENSE) file for details.