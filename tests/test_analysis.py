"""Statistical unit tests, including the assessment's sample."""
import math

import pytest

from watcher.analysis import analyze, moving_average


SAMPLE = [25.4, 26.1, 24.8, 85.0, 25.2, 25.9, 1.2, 24.5]


def test_moving_average_partial_windows():
    assert moving_average([10, 20, 30, 40], 3) == (10, 15, 20, 30)


def test_moving_average_larger_than_data():
    assert moving_average([3, 5], 10) == (3, 4)


def test_sample_one_outlier():
    stats = analyze(SAMPLE, 3)
    assert stats.average_value == pytest.approx(29.7625)
    assert stats.standard_deviation == pytest.approx(22.3257103750)
    assert stats.outlier_count == 1
    assert stats.latest_moving_average == pytest.approx(17.2)
    assert len(stats.moving_averages) == 8


def test_strict_two_standard_deviation_boundary():
    # For [0, 0, 0, 0, 10], mean=2 and population std=4.
    # |10 - 2| equals 2*4, so 10 is NOT an outlier.
    assert analyze([0, 0, 0, 0, 10]).outlier_count == 0
    # With six readings the lone 10 is beyond 2 population std deviations.
    assert analyze([0, 0, 0, 0, 0, 10]).outlier_count == 1


def test_constant_data_and_single_reading():
    constant = analyze([7, 7, 7])
    assert constant.standard_deviation == 0
    assert constant.outlier_count == 0
    assert analyze([100]).outlier_count == 0


@pytest.mark.parametrize("window", [0, -2, 1.5, True])
def test_bad_window(window):
    with pytest.raises(ValueError):
        moving_average([1, 2], window)


def test_empty_and_nonfinite_input():
    with pytest.raises(ValueError):
        analyze([])
    with pytest.raises(ValueError):
        moving_average([])
    with pytest.raises(ValueError):
        analyze([1.0, math.nan])
    with pytest.raises(ValueError):
        analyze([1.0, math.inf])
