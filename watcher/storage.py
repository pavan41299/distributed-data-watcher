"""SQLite persistence: short transactions, WAL, idempotent file keys."""
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from .analysis import Statistics


def _connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(db_path), timeout=30.0)
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def initialize_database(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(_connect(db_path)) as conn:
        # WAL permits multiple readers while writes are committed.
        conn.execute("PRAGMA journal_mode=WAL")
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS summaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_key TEXT NOT NULL UNIQUE,
                    filename TEXT NOT NULL,
                    average_value REAL NOT NULL,
                    latest_moving_average REAL NOT NULL,
                    outlier_count INTEGER NOT NULL CHECK (outlier_count >= 0),
                    reading_count INTEGER NOT NULL CHECK (reading_count > 0),
                    processed_at TEXT NOT NULL
                )
                """
            )


def insert_summary(
    db_path: Path, file_key: str, filename: str,
    stats: Statistics, reading_count: int,
) -> bool:
    """Insert once. Returns False on a replay of identical file content/name."""
    processed_at = datetime.now(timezone.utc).isoformat()
    with closing(_connect(db_path)) as conn:
        with conn:
            cursor = conn.execute(
                """
                INSERT INTO summaries (
                    file_key, filename, average_value, latest_moving_average,
                    outlier_count, reading_count, processed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_key) DO NOTHING
                """,
                (
                    file_key, filename, stats.average_value,
                    stats.latest_moving_average, stats.outlier_count,
                    reading_count, processed_at,
                ),
            )
            return cursor.rowcount == 1
