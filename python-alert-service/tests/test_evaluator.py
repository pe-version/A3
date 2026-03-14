"""Unit tests for the OPERATORS dict in alert_evaluator."""

import pytest

from services.alert_evaluator import OPERATORS


@pytest.mark.parametrize("value,threshold,expected", [
    (85.0, 80.0, True),   # above
    (75.0, 80.0, False),  # below
    (80.0, 80.0, False),  # equal (boundary — must not trigger)
])
def test_gt(value, threshold, expected):
    assert OPERATORS["gt"](value, threshold) == expected


@pytest.mark.parametrize("value,threshold,expected", [
    (75.0, 80.0, True),   # below
    (85.0, 80.0, False),  # above
    (80.0, 80.0, False),  # equal (boundary — must not trigger)
])
def test_lt(value, threshold, expected):
    assert OPERATORS["lt"](value, threshold) == expected


@pytest.mark.parametrize("value,threshold,expected", [
    (85.0, 80.0, True),   # above
    (80.0, 80.0, True),   # equal (boundary — must trigger)
    (75.0, 80.0, False),  # below
])
def test_gte(value, threshold, expected):
    assert OPERATORS["gte"](value, threshold) == expected


@pytest.mark.parametrize("value,threshold,expected", [
    (75.0, 80.0, True),   # below
    (80.0, 80.0, True),   # equal (boundary — must trigger)
    (85.0, 80.0, False),  # above
])
def test_lte(value, threshold, expected):
    assert OPERATORS["lte"](value, threshold) == expected


@pytest.mark.parametrize("value,threshold,expected", [
    (80.0, 80.0, True),   # exact match
    (80.1, 80.0, False),  # no match
])
def test_eq(value, threshold, expected):
    assert OPERATORS["eq"](value, threshold) == expected


def test_operators_has_no_unknown_keys():
    assert set(OPERATORS.keys()) == {"gt", "lt", "gte", "lte", "eq"}
