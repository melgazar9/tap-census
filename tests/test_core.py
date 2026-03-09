"""Tests standard tap features using the built-in SDK tests library."""

from __future__ import annotations

import os

import pytest
from singer_sdk.testing import get_tap_test_class

from tap_census.helpers import make_fips, parse_census_array, safe_float, safe_int
from tap_census.streams.population_streams import (
    _build_pep_params,
    _extract_pop_density,
    _find_vintage,
)
from tap_census.tap import TapCensus

SAMPLE_CONFIG = {
    "years": [2015, 2016, 2017, 2018, 2019, 2020, 2021],
    "geography_levels": ["county", "state"],
    "datasets": ["pep", "decennial"],
    "states": ["*"],
}


# SDK integration tests — only run with a real API key
if os.environ.get("CENSUS_API_KEY"):
    TestTapCensus = get_tap_test_class(
        tap_class=TapCensus,
        config={**SAMPLE_CONFIG, "api_key": os.environ["CENSUS_API_KEY"]},
    )


class TestHelpers:
    """Unit tests for helper functions."""

    def test_parse_census_array_basic(self) -> None:
        data = [
            ["POP", "NAME", "DATE_CODE", "state", "county"],
            ["4485414", "Maricopa County, Arizona", "12", "04", "013"],
            ["1043433", "Pima County, Arizona", "12", "04", "019"],
        ]
        result = parse_census_array(data)
        assert len(result) == 2
        assert result[0]["pop"] == "4485414"
        assert result[0]["name"] == "Maricopa County, Arizona"
        assert result[0]["state"] == "04"
        assert result[0]["county"] == "013"
        assert result[0]["date_code"] == "12"

    def test_parse_census_array_empty(self) -> None:
        assert parse_census_array([]) == []
        assert parse_census_array([["header"]]) == []
        assert parse_census_array(None) == []

    def test_make_fips(self) -> None:
        assert make_fips("04", "013") == "04013"
        assert make_fips("6", "37") == "06037"
        assert make_fips("06", "037") == "06037"

    def test_safe_int(self) -> None:
        assert safe_int("4940672") == 4940672
        assert safe_int("0") == 0
        assert safe_int(None) is None
        assert safe_int("") is None
        assert safe_int("null") is None
        assert safe_int("abc") is None

    def test_safe_float(self) -> None:
        assert safe_float("3.14") == pytest.approx(3.14)
        assert safe_float("0.0") == pytest.approx(0.0)
        assert safe_float(None) is None
        assert safe_float("") is None
        assert safe_float("null") is None
        assert safe_float("abc") is None


class TestVintageLookup:
    """Test PEP vintage lookup logic against verified API availability."""

    def test_county_vintage_2010_2019(self) -> None:
        """Years 2010-2019 should use the 2019 vintage for county data."""
        assert _find_vintage(2015, need_county=True) == 2019
        assert _find_vintage(2019, need_county=True) == 2019
        assert _find_vintage(2010, need_county=True) == 2019

    def test_county_vintage_2020_unavailable(self) -> None:
        """2020+ county PEP data does not exist (2021 vintage is state-only)."""
        assert _find_vintage(2020, need_county=True) is None
        assert _find_vintage(2021, need_county=True) is None

    def test_state_vintage_2020_2021(self) -> None:
        """2020-2021 state data should use the 2021 vintage."""
        assert _find_vintage(2020, need_county=False) == 2021
        assert _find_vintage(2021, need_county=False) == 2021

    def test_state_vintage_2010_2019(self) -> None:
        """2010-2019 state data should use the 2019 vintage."""
        assert _find_vintage(2015, need_county=False) == 2019

    def test_no_vintage_for_future_years(self) -> None:
        """Years beyond available vintages should return None."""
        assert _find_vintage(2022, need_county=False) is None
        assert _find_vintage(2023, need_county=False) is None


class TestPepParams:
    """Test PEP query parameter construction against verified API patterns."""

    def test_date_code_style_2019(self) -> None:
        """2019 vintage uses POP + DENSITY with DATE_CODE filter."""
        url, params = _build_pep_params(2019, 2019, "county:*", "state:*", "testkey")
        assert url == "https://api.census.gov/data/2019/pep/population"
        assert params["get"] == "POP,DENSITY,NAME"
        assert params["DATE_CODE"] == "12"  # 2019 - 2007 = 12
        assert params["for"] == "county:*"
        assert params["in"] == "state:*"
        assert params["key"] == "testkey"

    def test_date_code_2015(self) -> None:
        """DATE_CODE for 2015 should be 8 (2015 - 2007)."""
        _, params = _build_pep_params(2019, 2015, "county:*", "state:*", None)
        assert params["DATE_CODE"] == "8"
        assert "key" not in params

    def test_year_suffix_style_2021(self) -> None:
        """2021 vintage uses POP_{year} and DENSITY_{year} variables."""
        url, params = _build_pep_params(2021, 2021, "state:*", None, "testkey")
        assert url == "https://api.census.gov/data/2021/pep/population"
        assert params["get"] == "POP_2021,DENSITY_2021,NAME"
        assert "DATE_CODE" not in params
        assert "in" not in params


class TestExtractPopDensity:
    """Test population/density extraction for different vintage styles."""

    def test_date_code_style(self) -> None:
        record = {"pop": "4485414", "density": "487.50954904000000"}
        pop, density = _extract_pop_density(record, 2019, 2019)
        assert pop == 4485414
        assert density == pytest.approx(487.5095, rel=1e-3)

    def test_year_suffix_style(self) -> None:
        record = {"pop_2021": "7276316", "density_2021": "64.0221138030"}
        pop, density = _extract_pop_density(record, 2021, 2021)
        assert pop == 7276316
        assert density == pytest.approx(64.022, rel=1e-3)


class TestTapConfig:
    """Test tap configuration and stream discovery."""

    def test_discover_streams_default(self) -> None:
        tap = TapCensus(config=SAMPLE_CONFIG, parse_env_config=False)
        streams = tap.discover_streams()
        stream_names = [s.name for s in streams]
        assert "county_population" in stream_names
        assert "state_population" in stream_names
        assert "decennial_population" in stream_names
        assert "county_geography" in stream_names

    def test_discover_streams_pep_only(self) -> None:
        config = {**SAMPLE_CONFIG, "datasets": ["pep"]}
        tap = TapCensus(config=config, parse_env_config=False)
        streams = tap.discover_streams()
        stream_names = [s.name for s in streams]
        assert "county_population" in stream_names
        assert "state_population" in stream_names
        assert "decennial_population" not in stream_names
        assert "county_geography" in stream_names

    def test_discover_streams_county_only(self) -> None:
        config = {**SAMPLE_CONFIG, "geography_levels": ["county"]}
        tap = TapCensus(config=config, parse_env_config=False)
        streams = tap.discover_streams()
        stream_names = [s.name for s in streams]
        assert "county_population" in stream_names
        assert "state_population" not in stream_names

    def test_geography_always_included(self) -> None:
        config = {**SAMPLE_CONFIG, "datasets": [], "geography_levels": []}
        tap = TapCensus(config=config, parse_env_config=False)
        streams = tap.discover_streams()
        stream_names = [s.name for s in streams]
        assert "county_geography" in stream_names
