"""Bounded async polling watcher backed by worker threads."""
import asyncio
import errno
import hashlib
import logging
import os
import shutil
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from pydantic import ValidationError

from .analysis import analyze
from .models import parse_readings
from .storage import initialize_database, insert_summary


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class Outcome:
    status: str  # processed | error | retry | skipped
    filename: str
    detail: str


@dataclass(frozen=True)
class Observation:
    signature: tuple[int, int]  # (size, modified_at_ns)
    unchanged_since: float


def move_without_clobber(source: Path, destination_directory: Path) -> Path:
    """Move to the lifecycle directory without overwriting an earlier file."""
    destination_directory.mkdir(parents=True, exist_ok=True)
    destination = destination_directory / source.name
    if destination.exists():
        destination = destination_directory / (
            f"{source.stem}.{uuid4().hex}{source.suffix}"
        )
    try:
        return source.rename(destination)  # atomic on the same filesystem
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise

    # Docker may bind-mount inbox/ and processed/ separately, making rename
    # cross-filesystem. Publish a completed copy atomically in the destination
    # and only then delete the source (not an atomic cross-FS transaction).
    temporary = destination_directory / (
        f".{source.stem}.{uuid4().hex}.partial"
    )
    try:
        with source.open("rb") as input_file, temporary.open("xb") as output_file:
            shutil.copyfileobj(input_file, output_file)
            output_file.flush()
            os.fsync(output_file.fileno())
        temporary.rename(destination)
        source.unlink()
        return destination
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def process_file(
    path: Path, db_path: Path, processed_dir: Path,
    error_dir: Path, window_size: int,
) -> Outcome:
    """Worker-thread function; distinguish bad data from infrastructure faults."""
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return Outcome("skipped", path.name, "File disappeared before reading")
    except OSError as exc:
        return Outcome("retry", path.name, f"Cannot read file: {exc}")

    try:
        readings = parse_readings(raw)
        stats = analyze([reading.value for reading in readings], window_size)
    except (ValidationError, ValueError, ArithmeticError) as exc:
        try:
            destination = move_without_clobber(path, error_dir)
        except OSError as move_exc:
            return Outcome("retry", path.name, f"Could not quarantine: {move_exc}")
        return Outcome("error", path.name, f"Invalid telemetry ({exc}); moved to {destination}")

    # Include the original name: same content with a different name is a new file.
    file_key = hashlib.sha256(path.name.encode("utf-8") + b"\0" + raw).hexdigest()
    try:
        created = insert_summary(db_path, file_key, path.name, stats, len(readings))
    except sqlite3.Error as exc:
        # Do not quarantine valid data because of a temporary DB outage.
        return Outcome("retry", path.name, f"SQLite failure: {exc}")

    try:
        destination = move_without_clobber(path, processed_dir)
    except OSError as exc:
        # The DB commit has already occurred. Idempotency makes a retry safe.
        return Outcome("retry", path.name, f"DB committed, move failed: {exc}")

    action = "inserted" if created else "already recorded"
    return Outcome(
        "processed",
        path.name,
        f"{action}; mean={stats.average_value:.4f}, "
        f"last_moving_average={stats.latest_moving_average:.4f}, "
        f"outliers={stats.outlier_count}, destination={destination}",
    )


def snapshot_json_files(inbox: Path) -> list[tuple[Path, tuple[int, int]]]:
    """Run filesystem enumeration outside the asyncio event loop."""
    result: list[tuple[Path, tuple[int, int]]] = []
    for path in inbox.iterdir():
        if path.suffix.lower() != ".json":
            continue
        try:
            if path.is_file():
                info = path.stat()
                result.append((path, (info.st_size, info.st_mtime_ns)))
        except FileNotFoundError:
            continue
    return result


class FileWatcher:
    def __init__(
        self,
        inbox: Path,
        processed_dir: Path,
        error_dir: Path,
        db_path: Path,
        *,
        window_size: int = 3,
        poll_interval: float = 0.5,
        settle_seconds: float = 1.0,
        max_workers: int = 4,
        retry_seconds: float = 5.0,
    ) -> None:
        if window_size < 1 or max_workers < 1:
            raise ValueError("window_size and max_workers must be positive")
        if poll_interval <= 0 or settle_seconds < 0 or retry_seconds < 0:
            raise ValueError("Invalid timing options")
        self.inbox = inbox
        self.processed_dir = processed_dir
        self.error_dir = error_dir
        self.db_path = db_path
        self.window_size = window_size
        self.poll_interval = poll_interval
        self.settle_seconds = settle_seconds
        self.max_workers = max_workers
        self.retry_seconds = retry_seconds
        self._observed: dict[Path, Observation] = {}
        self._active: dict[Path, asyncio.Task[None]] = {}
        self._retry_after: dict[Path, float] = {}

    async def _handle(self, path: Path) -> None:
        try:
            outcome = await asyncio.to_thread(
                process_file, path, self.db_path,
                self.processed_dir, self.error_dir, self.window_size,
            )
            if outcome.status == "retry":
                self._retry_after[path] = time.monotonic() + self.retry_seconds
                LOGGER.warning("%s: %s", outcome.filename, outcome.detail)
            elif outcome.status == "error":
                LOGGER.error("%s: %s", outcome.filename, outcome.detail)
            else:
                LOGGER.info("%s: %s", outcome.filename, outcome.detail)
        except Exception:
            self._retry_after[path] = time.monotonic() + self.retry_seconds
            LOGGER.exception("Unexpected worker failure for %s", path.name)
        finally:
            self._active.pop(path, None)

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        if stop_event is None:
            stop_event = asyncio.Event()
        for directory in (self.inbox, self.processed_dir, self.error_dir):
            directory.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(initialize_database, self.db_path)
        LOGGER.info(
            "Watching %s (workers=%d, moving window=%d)",
            self.inbox, self.max_workers, self.window_size,
        )
        try:
            while not stop_event.is_set():
                snapshot = await asyncio.to_thread(snapshot_json_files, self.inbox)
                now = time.monotonic()
                present = {path for path, _ in snapshot}
                for path in list(self._observed):
                    if path not in present:
                        self._observed.pop(path, None)
                        self._retry_after.pop(path, None)
                for path, signature in snapshot:
                    old = self._observed.get(path)
                    if old is None or old.signature != signature:
                        self._observed[path] = Observation(signature, now)
                        continue
                    if now - old.unchanged_since < self.settle_seconds:
                        continue
                    if path in self._active or now < self._retry_after.get(path, 0):
                        continue
                    if len(self._active) >= self.max_workers:
                        continue  # bounded task count; pick up on next scan
                    self._active[path] = asyncio.create_task(self._handle(path))
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval)
                except asyncio.TimeoutError:
                    pass
        finally:
            # Gracefully finish accepted files rather than interrupting DB commits/moves.
            if self._active:
                await asyncio.gather(*tuple(self._active.values()), return_exceptions=True)
            LOGGER.info("Watcher stopped")
