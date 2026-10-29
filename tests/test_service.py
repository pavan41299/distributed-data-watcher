"""End-to-end tests: validation, file lifecycle, SQLite, concurrency, retries."""
import asyncio
import errno
import json
import sqlite3
from pathlib import Path

import pytest
from pydantic import ValidationError

from watcher.models import parse_readings
from watcher.service import FileWatcher, process_file
from watcher.storage import initialize_database


def readings(values):
    return [
        {
            "id": i,
            "value": value,
            "timestamp": f"2026-03-17T10:{i:02d}:00Z",
        }
        for i, value in enumerate(values)
    ]


def dirs(tmp_path):
    inbox = tmp_path / "inbox"
    processed = tmp_path / "processed"
    error = tmp_path / "error"
    db = tmp_path / "data" / "telemetry.sqlite3"
    for directory in (inbox, processed, error):
        directory.mkdir()
    initialize_database(db)
    return inbox, processed, error, db


def query_rows(db):
    with sqlite3.connect(db) as connection:
        return connection.execute(
            "SELECT filename, average_value, latest_moving_average, "
            "outlier_count, reading_count FROM summaries ORDER BY id"
        ).fetchall()


def test_pydantic_validation():
    parsed = parse_readings(json.dumps(readings([1, 2])).encode())
    assert parsed[0].value == 1.0
    with pytest.raises(ValidationError):
        parse_readings(b'[{"id": 1, "value": "wrong", "timestamp": "2026-03-17T10:00:00Z"}]')
    with pytest.raises(ValidationError):
        parse_readings(b'[{"id": 1, "value": 25.0, "timestamp": "2026-03-17T10:00:00"}]')
    with pytest.raises(ValueError):
        parse_readings(b"[]")


def test_successful_file_and_replay_are_idempotent(tmp_path):
    inbox, processed, error, db = dirs(tmp_path)
    file = inbox / "sensor_data.json"
    payload = json.dumps(readings([1, 2, 3])).encode()
    file.write_bytes(payload)
    first = process_file(file, db, processed, error, 2)
    assert first.status == "processed"
    assert not file.exists()
    assert (processed / "sensor_data.json").exists()
    assert query_rows(db) == [("sensor_data.json", 2.0, 2.5, 0, 3)]

    # Simulated retry after commit: original content/name must not duplicate DB.
    file.write_bytes(payload)
    replay = process_file(file, db, processed, error, 2)
    assert replay.status == "processed"
    assert len(query_rows(db)) == 1
    assert len(list(processed.glob("*.json"))) == 2
    assert not list(error.iterdir())


def test_invalid_file_is_quarantined(tmp_path):
    inbox, processed, error, db = dirs(tmp_path)
    bad = inbox / "bad.json"
    bad.write_text("{oops", encoding="utf-8")
    outcome = process_file(bad, db, processed, error, 3)
    assert outcome.status == "error"
    assert (error / "bad.json").is_file()
    assert not bad.exists()
    assert query_rows(db) == []


def test_same_filename_different_content_is_new_record(tmp_path):
    inbox, processed, error, db = dirs(tmp_path)
    path = inbox / "data.json"
    path.write_text(json.dumps(readings([1, 2])), encoding="utf-8")
    assert process_file(path, db, processed, error, 3).status == "processed"
    path.write_text(json.dumps(readings([2, 5])), encoding="utf-8")
    assert process_file(path, db, processed, error, 3).status == "processed"
    assert len(query_rows(db)) == 2


def test_watcher_multiple_simultaneous_files(tmp_path):
    inbox, processed, error, db = dirs(tmp_path)
    for i in range(5):
        (inbox / f"sensor_{i}.json").write_text(
            json.dumps(readings([i, i + 1, i + 2])), encoding="utf-8",
        )
    (inbox / "malformed.json").write_text("not a json file", encoding="utf-8")

    async def execute():
        stop = asyncio.Event()
        watcher = FileWatcher(
            inbox, processed, error, db,
            poll_interval=0.015,
            settle_seconds=0.015,
            max_workers=2,
        )
        task = asyncio.create_task(watcher.run(stop))
        try:
            async def all_done():
                while len(list(processed.glob("*.json"))) < 5 or not (error / "malformed.json").exists():
                    await asyncio.sleep(0.015)
            await asyncio.wait_for(all_done(), timeout=7)
        finally:
            stop.set()
            await task

    asyncio.run(execute())
    assert len(query_rows(db)) == 5
    assert len(list(processed.glob("*.json"))) == 5
    assert (error / "malformed.json").exists()
    assert not list(inbox.glob("*.json"))


def test_cross_filesystem_move_fallback(tmp_path, monkeypatch):
    """Separate Docker bind mounts can make rename raise EXDEV."""
    inbox, processed, error, db = dirs(tmp_path)
    source = inbox / "cross_device.json"
    source.write_text(json.dumps(readings([4, 5, 6])), encoding="utf-8")
    original = Path.rename

    def simulate_exdev(self, target):
        if self == source:
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return original(self, target)

    monkeypatch.setattr(Path, "rename", simulate_exdev)
    result = process_file(source, db, processed, error, 2)
    assert result.status == "processed"
    assert not source.exists()
    assert (processed / "cross_device.json").exists()
    assert len(query_rows(db)) == 1
