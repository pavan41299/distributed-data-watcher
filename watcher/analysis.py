"""Dependency-free, testable statistics (no I/O)."""
from collections import deque
from dataclasses import dataclass
from math import isfinite
from statistics import fmean, pstdev
from typing import Sequence


@dataclass(frozen=True)
class Statistics:
    average_value: float
    standard_deviation: float
    outlier_count: int
    moving_averages: tuple[float, ...]

    @property
    def latest_moving_average(self) -> float:
        return self.moving_averages[-1]


def moving_average(values: Sequence[float], window_size: int = 3) -> tuple[float, ...]:
    """Trailing average, using available readings for initial partial windows."""
    if isinstance(window_size, bool) or not isinstance(window_size, int) or window_size < 1:
        raise ValueError("window_size must be a positive integer")
    if not values:
        raise ValueError("values must not be empty")

    output: list[float] = []
    queue: deque[float] = deque()
    running_sum = 0.0
    for value in values:
        number = float(value)
        if not isfinite(number):
            raise ValueError("values must be finite")
        queue.append(number)
        running_sum += number
        if len(queue) > window_size:
            running_sum -= queue.popleft()
        output.append(running_sum / len(queue))
    if not all(isfinite(item) for item in output):
        raise ValueError("moving average overflow")
    return tuple(output)


def analyze(values: Sequence[float], window_size: int = 3) -> Statistics:
    """Population std-dev; outlier only when strictly beyond 2 std-devs."""
    if not values:
        raise ValueError("values must not be empty")
    cleaned = [float(v) for v in values]
    if not all(isfinite(v) for v in cleaned):
        raise ValueError("values must be finite")
    mean = fmean(cleaned)
    std_dev = pstdev(cleaned)
    if not isfinite(mean) or not isfinite(std_dev):
        raise ValueError("statistical calculation overflow")
    count = sum(abs(v - mean) > 2 * std_dev for v in cleaned)
    return Statistics(
        average_value=mean,
        standard_deviation=std_dev,
        outlier_count=count,
        moving_averages=moving_average(cleaned, window_size),
    )
