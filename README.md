# Distributed Data Watcher Service (Python Assessment)

A **runnable, asynchronous landing-zone ingestion service** for the supplied
Python technical assessment. This repository uses Python 3.10+, Pydantic v2,
`asyncio`, worker threads, SQLite, pytest, and Docker.

## At a glance

```text
Producer -> ./inbox/*.json
               |
               v
    asyncio watcher (polling and file stability)
               |
               v
   Bounded task dispatch (asyncio.to_thread)
               |
               +--> Pydantic validation -- invalid --> ./error/
               |
               +--> Moving average + 2-sigma analysis
               |
               +--> SQLite WAL + idempotency key
               |
               +--> ./processed/
```

This is a **single-instance reference implementation**. Its watcher is
non-blocking and handles concurrent arrivals. It is **not** a multi-machine
distributed system; the final section explains the modifications necessary for
a truly distributed 10,000-files/minute pipeline.

## Project files

```text
distributed-data-watcher/
├── watcher/
│   ├── __init__.py
│   ├── __main__.py       # command-line entry point
│   ├── models.py         # Pydantic schema and JSON array parser
│   ├── analysis.py       # moving average and two-sigma detection
│   ├── storage.py        # SQLite setup and idempotent writes
│   └── service.py        # non-blocking watch loop and file lifecycle
├── tests/
│   ├── test_analysis.py  # calculation unit tests
│   └── test_service.py   # validation, DB, lifecycle, concurrency
├── sample_data/
│   └── sensor_data.json
├── inbox/
├── processed/
├── error/
├── data/
├── requirements.txt
├── Dockerfile
├── compose.yaml
├── pytest.ini
├── .dockerignore
└── .gitignore
```

## 1. Run using a virtual environment

Run these commands from the project root:

**Linux/macOS**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m watcher
```

**Windows PowerShell**

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m watcher
```

In **another terminal**, copy the sample file into the watched inbox:

```bash
cp sample_data/sensor_data.json inbox/sensor_data.json
```

PowerShell equivalent:

```powershell
Copy-Item sample_data\sensor_data.json inbox\sensor_data.json
```

After the file is stable for at least 1 second, it is validated and processed.
Look for the file under `processed/`, see the log output, and inspect
`data/telemetry.sqlite3`.

Press Ctrl+C to stop.

## 2. Run with Docker Compose

Make sure Docker with Compose v2 is available. From the project root:

```bash
docker compose up --build
```

With the service running, copy files into your host's `inbox/` directory.
Bind mounts share the `inbox`, `processed`, `error`, and `data` directories with
the container. Database rows persist across container recreation.

Stop using Ctrl+C or `docker compose down`.

**Plain Docker alternative (Linux/macOS):**

```bash
docker build -t telemetry-watcher .
docker run --rm \
  -v "$PWD/inbox:/app/inbox" \
  -v "$PWD/processed:/app/processed" \
  -v "$PWD/error:/app/error" \
  -v "$PWD/data:/app/data" \
  telemetry-watcher
```

## 3. Inspect results

Using Python (no external `sqlite3` CLI needed):

```bash
python -c "import sqlite3; c=sqlite3.connect('data/telemetry.sqlite3'); print(*c.execute('SELECT filename, average_value, latest_moving_average, outlier_count, reading_count, processed_at FROM summaries'), sep='\n'); c.close()"
```

Expected statistics for the supplied sensor data:

| Metric | Value |
|---|---:|
| Number of readings | 8 |
| Full-file arithmetic mean (`average_value`) | 29.7625 |
| Population standard deviation | ~22.3257103750 |
| Lower outlier boundary, `mean - 2*std` | ~-14.8889207501 |
| Upper outlier boundary, `mean + 2*std` | ~74.4139207501 |
| Outlier | **85.0** |
| `outlier_count` | **1** |
| Last trailing 3-reading moving average | **17.2** |

The **moving average window defaults to 3**, because the assessment does not
specify a size. It is configurable with `--window-size`. The first two
moving-average values use shorter windows (the available prefix); all later
values use the three most recent entries in their *file order*. No sorting by
timestamp occurs. The sample moving-average sequence is:

```text
25.4000, 25.7500, 25.4333, 45.3000,
45.0000, 45.3667, 17.4333, 17.2000
```

Outlier detection is global to the input file, rather than relative to each
sliding window. The two-standard-deviation rule is **strictly greater than**:
`abs(reading - mean) > 2 * population_std`. The single reading `1.2` is not
an outlier under this rule.

There are two different averages in the database by design:

- `average_value` = arithmetic mean of every reading in the file.
- `latest_moving_average` = final value of the trailing moving average.
  The complete moving-average series is calculated but not persisted because
  the assessment only requires a summary.

## 4. Configuration

```bash
python -m watcher \
  --inbox ./inbox \
  --processed ./processed \
  --error ./error \
  --db ./data/telemetry.sqlite3 \
  --window-size 3 \
  --poll-interval 0.5 \
  --settle-seconds 1.0 \
  --max-workers 4 \
  --retry-seconds 5
```

- `--max-workers` bounds active file-processing tasks.
- `--settle-seconds` defers processing until size and modification time
  remain unchanged for a short interval, reducing partial-write errors.
- `--retry-seconds` applies a backoff after transient processing failures.
- For **correct producer-side hand-off**, write the data to a temporary
  filename and atomically rename it to `<name>.json` in `inbox/` when complete.
  A fixed quiescence interval alone cannot prove that writing has finished.

## 5. Pydantic validation

Each JSON file must be a nonempty **array** of objects with:

```json
{"id": 1, "value": 25.4, "timestamp": "2026-03-17T10:00:00Z"}
```

- `id`: nonnegative integer (not an integer-like string).
- `value`: finite number (not a string, `NaN`, or infinity).
- `timestamp`: ISO 8601 datetime **with timezone**, such as UTC `Z`.
- Unknown object fields are rejected (`extra="forbid"`).

Malformed JSON, empty arrays, and invalid readings are moved to `error/`.
SQLite/network/disk infrastructure errors are **retried** instead of
mistakenly classifying valid telemetry as malformed.

## 6. SQLite schema and fault handling

The database table contains required fields:

`filename`, `average_value`, `outlier_count`, `processed_at`

plus:

`id`, `file_key`, `latest_moving_average`, `reading_count`

The `file_key` is a SHA-256 hash of the filename + a separator + file bytes.
`INSERT ... ON CONFLICT(file_key) DO NOTHING` prevents duplicate rows from a
replay with identical filename and content.

SQLite uses WAL and a separate short-lived connection for each worker.
This supports concurrent readers and serialized writes, but SQLite remains
a single-writer database.

**Commit-before-move trade-off:** the summary is committed before the file is
moved. If a process crashes after the DB commit but before the move, the file
may remain in `inbox/`. On restart, it will be processed again without a
duplicate row, then moved. If the move fails transiently, it remains in
`inbox/` and retries. Files moved to existing target filenames gain a UUID
suffix to avoid overwriting previously processed files. If `rename()` reports
a cross-filesystem move (possible with separate Docker bind mounts), the
service copies to a temporary destination file, flushes it, renames it within
the destination filesystem, and deletes the original. This fallback is not
atomic across both filesystems, but avoids publishing incomplete copies.

The design provides **idempotent at-least-once attempts**, not an atomic
transaction spanning the local filesystem and SQLite. Multi-process
coordination, durable work claims, and a transactional outbox would be needed
for stronger distributed guarantees.

## 7. Testing

```bash
python -m pytest -q
```

The test suite verifies:

1. Trailing moving average with partial windows.
2. Moving windows larger than the input.
3. Sample mean, standard deviation, and outlier count.
4. Strict `> 2σ` threshold behavior.
5. Constant and single-element inputs.
6. Invalid window sizes and nonfinite values.
7. Pydantic validation, including timezone-aware timestamps.
8. Successful DB insert and processed-file movement.
9. Duplicate replay and collision-safe destination filenames.
10. Malformed input quarantine.
11. Reused filename with *different* content.
12. Concurrent arrivals of five valid files and one malformed file.
13. Cross-filesystem moves with Docker-style separate bind mounts.

All tests use isolated temporary directories and do not require Docker.

## 8. Design decisions

| Concern | Choice | Why |
|---|---|---|
| File monitoring | Periodic `asyncio` polling | Simple cross-platform implementation with no watcher dependencies |
| Blocking work | `asyncio.to_thread` | Parsing, file I/O, SQLite access do not block the event loop |
| Concurrency control | `max_workers` active tasks | Bounds outstanding tasks and limits memory pressure |
| Input readiness | File size and mtime quiescence | Reduces ingestion of partially written files |
| Validation | Pydantic v2 | Declarative types and meaningful validation errors |
| Moving average | O(n) rolling sum | Avoids O(n × window size) recalculation |
| Outliers | Global mean + population standard deviation | Explicit, deterministic interpretation of the requirement |
| Persistence | SQLite WAL and idempotency key | Minimal deployment cost and replay safety |
| Lifecycle | `processed/` or `error/` | Operational traceability and easy inspection |
| Failures | Retry infrastructure faults; quarantine bad inputs | Avoids incorrectly discarding valid readings |

`asyncio.to_thread` is appropriate for this small service and guarantees the
asyncio event loop stays responsive. **It is not equivalent to multi-core CPU
parallelism** for pure Python CPU-bound code because of the GIL. Use process
pools or separate workers for CPU-intensive high-volume analysis.

## 9. Scale to 10,000 files per minute

**Throughput:** `10,000 / 60 ≈ 166.7 files/sec`.

At an assumed average **250 ms of end-to-end service time per file**, Little's
law suggests `166.7 * 0.25 ≈ 41.7` concurrent in-flight files just to keep up
in the ideal case. That is a concurrency estimate, **not** a benchmark-derived
worker count. Add headroom, measure peak loads, and account for storage and
CPU capacity.

The reference implementation is useful for a technical assessment, but the
following is a more realistic scalable architecture:

```text
Many producers
    |
    v
Object storage (S3 / Blob Storage)
    |
    v
Object-created events --> durable queue (SQS / Kafka / RabbitMQ)
                               |
               +---------------+---------------+
               |               |               |
               v               v               v
           Worker A         Worker B         Worker N
               |               |               |
               +---------------+---------------+
                               |
                  Central PostgreSQL (batch writes)
                               |
                  Success archive / Dead-letter queue
```

**Priority changes for scale:**

1. **Use object storage and event notifications** instead of repeatedly
   scanning a shared 10k+/minute directory. Event delivery is treated as
   at-least-once; it must not be assumed unique.
2. **Use a durable queue** for backpressure, buffering, retries, and load
   redistribution. A worker acknowledges a message only after persistence
   and lifecycle work is complete.
3. **Scale stateless workers horizontally** based on queue depth, queue age,
   latency, and CPU load. Use process-level parallelism for CPU-heavy JSON.
4. **Use PostgreSQL or suitable managed storage** rather than SQLite's
   single-writer architecture; tune connections and batch inserts.
5. **Make ingestion idempotent** via source object version/ETag or ingestion ID
   and a database uniqueness constraint. For multi-worker coordination use
   queue visibility timeouts / leases and retry-safe handlers.
6. **Implement operational controls:** dead-letter queue, exponential
   backoff with jitter, structured logs, OpenTelemetry tracing, saturation
   alerts, object retention policies, payload limits, and security controls.
7. **Load test:** measure actual parsing cost, file size distribution,
   p50/p95/p99 processing time, database write throughput, queue age,
   error rates, and memory use before selecting worker count.

For example, if each file averages **200 KB**, the target rate amounts to
about **33.3 MB/sec** of raw input and **2.88 TB/day** (decimal units), before
replication, database overhead, logging, or retention. The average file size
is an assumption, not supplied in the assessment.
