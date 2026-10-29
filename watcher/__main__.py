"""CLI entry point: python -m watcher."""
import argparse
import asyncio
import logging
import signal
from pathlib import Path

from .service import FileWatcher


def main() -> None:
    parser = argparse.ArgumentParser(description="Asynchronous telemetry directory watcher")
    parser.add_argument("--inbox", type=Path, default=Path("./inbox"))
    parser.add_argument("--processed", type=Path, default=Path("./processed"))
    parser.add_argument("--error", type=Path, default=Path("./error"))
    parser.add_argument("--db", type=Path, default=Path("./data/telemetry.sqlite3"))
    parser.add_argument("--window-size", type=int, default=3)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--settle-seconds", type=float, default=1.0)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--retry-seconds", type=float, default=5.0)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    async def main_async() -> None:
        stop = asyncio.Event()
        watcher = FileWatcher(
            args.inbox, args.processed, args.error, args.db,
            window_size=args.window_size,
            poll_interval=args.poll_interval,
            settle_seconds=args.settle_seconds,
            max_workers=args.max_workers,
            retry_seconds=args.retry_seconds,
        )
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop.set)
            except NotImplementedError:
                # add_signal_handler is not available on Windows event loops.
                pass
        await watcher.run(stop)

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
