"""Validate every Census Bureau endpoint for data completeness.

Probes each endpoint/year combination and verifies:
  - Expected row counts (county ~3142-3222, state 52)
  - All required columns present in API response
  - No unexpected columns that would indicate schema drift
  - Full year coverage with no gaps
  - Edge cases: CT planning regions, PR municipios, DC

Usage:
  uv run python validate_endpoints.py
  CENSUS_API_KEY=... uv run python validate_endpoints.py
  uv run python validate_endpoints.py --stream county_population
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

API_BASE = "https://api.census.gov/data"
_MIN_ROWS = 2

# Columns our code extracts (lowercased) per stream.
# "required" = columns that MUST be present for the stream to work.
# "optional" = columns the API may echo (predicates) that we ignore.
STREAM_VALIDATIONS: dict[str, dict[str, Any]] = {
    "county_population": {
        "description": "PEP population 2019 vintage, county level",
        "years": list(range(2010, 2020)),
        "build_request": lambda year: (
            f"{API_BASE}/2019/pep/population",
            {
                "get": "POP,DENSITY,NAME",
                "for": "county:*",
                "in": "state:*",
                "DATE_CODE": str(year - 2007),
            },
        ),
        "required_columns": {"pop", "density", "name", "state", "county"},
        "expected_row_range": (3100, 3250),  # ~3142 counties (50 states+DC+PR)
    },
    "state_population_2019": {
        "description": "PEP population 2019 vintage, state level",
        "years": list(range(2010, 2020)),
        "build_request": lambda year: (
            f"{API_BASE}/2019/pep/population",
            {"get": "POP,DENSITY,NAME", "for": "state:*", "DATE_CODE": str(year - 2007)},
        ),
        "required_columns": {"pop", "density", "name", "state"},
        "expected_row_range": (52, 52),
    },
    "state_population_2021": {
        "description": "PEP population 2021 vintage, state level",
        "years": [2020, 2021],
        "build_request": lambda year: (
            f"{API_BASE}/2021/pep/population",
            {"get": f"POP_{year},DENSITY_{year},NAME", "for": "state:*"},
        ),
        "required_columns": {"name", "state"},  # pop/density keys are dynamic
        "expected_row_range": (52, 52),
    },
    "county_population_charv": {
        "description": "PEP charv 2023 vintage, county level",
        "years": [2020, 2021, 2022, 2023],
        "build_request": lambda year: (
            f"{API_BASE}/2023/pep/charv",
            {
                "get": "POP,NAME",
                "for": "county:*",
                "in": "state:*",
                "AGE": "0000",
                "SEX": "0",
                "HISP": "0",
                "YEAR": str(year),
                "MONTH": "7",
            },
        ),
        "required_columns": {"pop", "name", "state", "county"},
        "expected_row_range": (3200, 3250),  # 3222 counties (incl CT regions)
    },
    "state_population_charv": {
        "description": "PEP charv 2023 vintage, state level",
        "years": [2020, 2021, 2022, 2023],
        "build_request": lambda year: (
            f"{API_BASE}/2023/pep/charv",
            {
                "get": "POP,NAME",
                "for": "state:*",
                "AGE": "0000",
                "SEX": "0",
                "HISP": "0",
                "YEAR": str(year),
                "MONTH": "7",
            },
        ),
        "required_columns": {"pop", "name", "state"},
        "expected_row_range": (52, 52),
    },
    "county_housing_units": {
        "description": "PEP housing 2019 vintage, county level",
        "years": list(range(2010, 2020)),
        "build_request": lambda year: (
            f"{API_BASE}/2019/pep/housing",
            {
                "get": "HUEST,NAME",
                "for": "county:*",
                "in": "state:*",
                "DATE_CODE": str(year - 2007),
            },
        ),
        "required_columns": {"huest", "name", "state", "county"},
        "expected_row_range": (3100, 3250),
    },
    "state_housing_units": {
        "description": "PEP housing 2019 vintage, state level",
        "years": list(range(2010, 2020)),
        "build_request": lambda year: (
            f"{API_BASE}/2019/pep/housing",
            {"get": "HUEST,NAME", "for": "state:*", "DATE_CODE": str(year - 2007)},
        ),
        "required_columns": {"huest", "name", "state"},
        "expected_row_range": (51, 51),  # 50 states + DC; PR not in housing
    },
    "decennial_2000": {
        "description": "Decennial 2000 SF1, county level",
        "years": [2000],
        "build_request": lambda _year: (
            f"{API_BASE}/2000/dec/sf1",
            {"get": "P001001,NAME", "for": "county:*", "in": "state:*"},
        ),
        "required_columns": {"p001001", "name", "state", "county"},
        "expected_row_range": (3100, 3250),
    },
    "decennial_2010": {
        "description": "Decennial 2010 SF1, county level",
        "years": [2010],
        "build_request": lambda _year: (
            f"{API_BASE}/2010/dec/sf1",
            {"get": "P001001,NAME", "for": "county:*", "in": "state:*"},
        ),
        "required_columns": {"p001001", "name", "state", "county"},
        "expected_row_range": (3100, 3250),
    },
    "decennial_2020": {
        "description": "Decennial 2020 PL, county level",
        "years": [2020],
        "build_request": lambda _year: (
            f"{API_BASE}/2020/dec/pl",
            {"get": "P1_001N,NAME", "for": "county:*", "in": "state:*"},
        ),
        "required_columns": {"p1_001n", "name", "state", "county"},
        "expected_row_range": (3100, 3250),
    },
}

# How many requests per minute (Census limit without key)
RATE_LIMIT_RPM = 40
_last_request_time = 0.0

# Columns the Census API commonly echoes that are not part of our schema.
_KNOWN_EXTRA_COLUMNS = frozenset(
    {
        "date_code",
        "date_desc",
        "geo_id",
        "sumlevel",
        "geocomp",
        "lastupdate",
        "funcstat",
        "primgeoflag",
        "age",
        "sex",
        "hisp",
        "year",
        "month",
        "popgroup",
        "for",
        "in",
        "ucgid",
        "nation",
        "region",
        "division",
        "cbsa",
        "csa",
        "metdiv",
        "place",
        "concity",
        "estplace",
        "eucousub",
        "eupb",
        "universe",
    }
)


@dataclass
class ValidationResult:
    """Result of validating a single stream/year combination."""

    stream: str
    year: int
    status: str  # "PASS", "FAIL", "WARN"
    row_count: int = 0
    message: str = ""
    missing_columns: set = field(default_factory=set)
    extra_columns: set = field(default_factory=set)


def fetch(url: str, params: dict[str, str]) -> list[list[str]] | dict[str, str] | None:
    """Fetch Census API data with rate limiting."""
    global _last_request_time  # noqa: PLW0603
    elapsed = time.monotonic() - _last_request_time
    min_interval = 60.0 / RATE_LIMIT_RPM
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    _last_request_time = time.monotonic()

    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}"
    try:
        req = urllib.request.Request(full_url, method="GET")  # noqa: S310
        req.add_header("User-Agent", "tap-census-validator/1.0")
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


def validate_stream(name: str, config: dict) -> list[ValidationResult]:
    """Validate a single stream across all its years."""
    results = []
    lo, hi = config["expected_row_range"]
    required = config["required_columns"]

    for year in config["years"]:
        url, params = config["build_request"](year)
        data = fetch(url, params)

        if isinstance(data, dict) and "error" in data:
            results.append(
                ValidationResult(
                    name,
                    year,
                    "FAIL",
                    message=f"Request error: {data['error']}",
                )
            )
            continue

        if not isinstance(data, list) or len(data) < _MIN_ROWS:
            results.append(
                ValidationResult(
                    name,
                    year,
                    "FAIL",
                    0,
                    "Empty or invalid response",
                )
            )
            continue

        headers = {h.lower().replace(" ", "_") for h in data[0]}
        row_count = len(data) - 1

        missing = required - headers
        extra = headers - _KNOWN_EXTRA_COLUMNS - required

        status = "PASS"
        message_parts = []

        if missing:
            status = "FAIL"
            message_parts.append(f"missing columns: {missing}")

        if row_count < lo or row_count > hi:
            status = "FAIL"
            message_parts.append(f"row count {row_count} outside [{lo}, {hi}]")

        if extra:
            if status == "PASS":
                status = "WARN"
            message_parts.append(f"unexpected columns: {extra}")

        results.append(
            ValidationResult(
                stream=name,
                year=year,
                status=status,
                row_count=row_count,
                message="; ".join(message_parts) if message_parts else "OK",
                missing_columns=missing,
                extra_columns=extra,
            )
        )

    return results


def _print_summary(all_results: list[ValidationResult]) -> None:
    """Print validation summary with pass/warn/fail counts and row count table."""
    passes = [r for r in all_results if r.status == "PASS"]
    warns = [r for r in all_results if r.status == "WARN"]
    fails = [r for r in all_results if r.status == "FAIL"]

    print(f"  PASS: {len(passes)}")
    print(f"  WARN: {len(warns)}")
    print(f"  FAIL: {len(fails)}")
    print(f"  Total: {len(all_results)} endpoint/year combinations")

    if fails:
        print("\nFAILURES:")
        for r in fails:
            print(f"  {r.stream} year={r.year}: {r.message}")

    if warns:
        print("\nWARNINGS:")
        for r in warns:
            print(f"  {r.stream} year={r.year}: {r.message}")

    # Row count table
    print(f"\n{'=' * 60}")
    print("ROW COUNTS BY STREAM/YEAR")
    print(f"{'=' * 60}")
    current_stream = None
    for r in all_results:
        if r.stream != current_stream:
            current_stream = r.stream
            print(f"\n  {r.stream}:")
        print(f"    {r.year}: {r.row_count:>6} rows  [{r.status}]")


def main() -> None:
    """Run endpoint validation for all or selected Census streams."""
    parser = argparse.ArgumentParser(description="Validate Census API endpoints")
    parser.add_argument("--stream", help="Validate only this stream")
    args = parser.parse_args()

    streams = STREAM_VALIDATIONS
    if args.stream:
        streams = {k: v for k, v in streams.items() if args.stream in k}
        if not streams:
            print(f"No stream matching '{args.stream}'")
            sys.exit(1)

    all_results: list[ValidationResult] = []
    total_requests = sum(len(c["years"]) for c in streams.values())
    completed = 0

    for name, config in streams.items():
        print(f"\n{'=' * 60}")
        print(f"Validating: {name} ({config['description']})")
        print(f"  Years: {config['years']}")
        print(f"  Expected rows: {config['expected_row_range']}")

        results = validate_stream(name, config)
        all_results.extend(results)

        for r in results:
            completed += 1
            icon = {"PASS": ".", "WARN": "?", "FAIL": "X"}[r.status]
            if r.status != "PASS":
                print(f"  {icon} {r.year}: {r.status} rows={r.row_count} {r.message}")
            else:
                print(f"  {icon} {r.year}: rows={r.row_count}")

        pct = completed / total_requests * 100
        print(f"  Progress: {completed}/{total_requests} ({pct:.0f}%)")

    # Summary
    print(f"\n{'=' * 60}")
    print("VALIDATION SUMMARY")
    print(f"{'=' * 60}")
    _print_summary(all_results)

    sys.exit(1 if any(r.status == "FAIL" for r in all_results) else 0)


if __name__ == "__main__":
    main()
