"""Utility functions for Census API data parsing."""

from __future__ import annotations

from typing import TypeVar

T = TypeVar("T", int, float)

# Sentinel values the Census API uses instead of JSON null
_NULL_STRINGS = {"", "null"}


def _safe_cast(value: str | None, target_type: type[T]) -> T | None:
    """Safely cast a Census API string to a numeric type.

    Census API returns all values as strings, including the literal "null".

    Args:
        value: String value from Census API.
        target_type: The target numeric type (int or float).

    Returns:
        Converted value or None if conversion fails.
    """
    if value is None or value in _NULL_STRINGS:
        return None
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return None


def safe_int(value: str | None) -> int | None:
    """Safely convert Census string value to integer."""
    return _safe_cast(value, int)


def safe_float(value: str | None) -> float | None:
    """Safely convert Census string value to float."""
    return _safe_cast(value, float)


def parse_census_array(data: list[list[str]]) -> list[dict]:
    """Convert Census API 2D array response to list of dicts.

    The Census API returns data as a 2D array where the first row is headers
    and subsequent rows are data. All values are strings.

    Args:
        data: Raw 2D array from Census API response.

    Returns:
        List of dictionaries with lowercased, underscore-separated keys.
    """
    min_rows = 2
    if not data or len(data) < min_rows:
        return []
    headers = [h.lower().replace(" ", "_") for h in data[0]]
    return [dict(zip(headers, row, strict=False)) for row in data[1:]]


def make_fips(state_fips: str, county_fips: str) -> str:
    """Create full 5-digit FIPS code from state + county.

    Args:
        state_fips: 2-digit state FIPS code.
        county_fips: 3-digit county FIPS code.

    Returns:
        5-digit concatenated FIPS code.
    """
    return f"{state_fips.zfill(2)}{county_fips.zfill(3)}"
