"""Population streams for Census Bureau PEP and Decennial data.

API documentation verified against live Census Bureau endpoints:
- PEP 2019 vintage: /data/2019/pep/population (variables: POP, DENSITY, NAME, DATE_CODE)
  Covers years 2010-2019 at county and state level.
  DATE_CODE mapping: year 2010 -> 3, 2011 -> 4, ..., 2019 -> 12.
- PEP 2021 vintage: /data/2021/pep/population (variables: POP_{year}, DENSITY_{year}, NAME)
  Covers years 2020-2021 at state level ONLY (no county geography).
- PEP charv 2023 vintage: /data/2023/pep/charv (variables: POP, YEAR, MONTH, NAME)
  Covers years 2020-2023 at county AND state level. No DENSITY variable.
  Predicates: AGE=0000, SEX=0, HISP=0, MONTH=7 for total July population.
- Decennial 2000/2010: /data/{year}/dec/sf1 (variable: P001001)
- Decennial 2020: /data/2020/dec/pl (variable: P1_001N)
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th

from tap_census.client import CensusStream
from tap_census.helpers import safe_float, safe_int

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

# ---------------------------------------------------------------------------
# PEP vintage registry
# ---------------------------------------------------------------------------
# Keyed by vintage year. The 2019 vintage uses generic POP/DENSITY variables
# with a DATE_CODE filter (DATE_CODE = year - 2007). The 2021 vintage uses
# year-suffixed variables (POP_2020, POP_2021) and only supports state geography.

PEP_VINTAGES: dict[int, dict[str, Any]] = {
    2019: {
        "years": range(2010, 2020),
        "supports_county": True,
        "variable_style": "date_code",
    },
    2021: {
        "years": range(2020, 2022),
        "supports_county": False,
        "variable_style": "year_suffix",
    },
}


def _find_vintage(year: int, *, need_county: bool) -> int | None:
    """Find the appropriate PEP vintage for a given year and geography need."""
    for vintage, meta in PEP_VINTAGES.items():
        if year in meta["years"] and (not need_county or meta["supports_county"]):
            return vintage
    return None


def _build_pep_params(
    vintage: int,
    year: int,
) -> tuple[str, dict[str, Any]]:
    """Build the URL and vintage-specific query params for a PEP API request.

    Geography and API key are injected separately via base class helpers.
    """
    meta = PEP_VINTAGES[vintage]
    url = f"https://api.census.gov/data/{vintage}/pep/population"

    params: dict[str, Any] = {}
    if meta["variable_style"] == "date_code":
        params["get"] = "POP,DENSITY,NAME"
        params["DATE_CODE"] = str(year - 2007)
    else:
        params["get"] = f"POP_{year},DENSITY_{year},NAME"

    return url, params


def _extract_pop_density(
    record: dict[str, str],
    vintage: int,
    year: int,
) -> tuple[int | None, float | None]:
    """Extract population and density from a parsed Census record."""
    if PEP_VINTAGES[vintage]["variable_style"] == "date_code":
        return safe_int(record.get("pop")), safe_float(record.get("density"))
    return (
        safe_int(record.get(f"pop_{year}")),
        safe_float(record.get(f"density_{year}")),
    )


# ---------------------------------------------------------------------------
# Base PEP stream — eliminates duplication between County and State streams
# ---------------------------------------------------------------------------


class PepPopulationBaseStream(CensusStream):
    """Shared logic for PEP population streams (Single Responsibility).

    Subclasses set ``requires_county_geography`` and override
    ``_transform_pep_record`` to customize for county vs. state geography.
    """

    path = ""

    @property
    def partitions(self) -> list[dict[str, Any]]:
        """One partition per configured year that has available PEP data."""
        default = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
        years = self.config.get("years", default)
        need_county = self.requires_county_geography
        return [{"year": y} for y in years if _find_vintage(y, need_county=need_county)]

    def _transform_pep_record(self, record: dict, year: int, vintage: int) -> dict:
        """Transform a parsed Census record to the output schema."""
        raise NotImplementedError

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Fetch PEP population for a single year partition."""
        if context is None:
            return

        year = context["year"]
        vintage = _find_vintage(year, need_county=self.requires_county_geography)
        if vintage is None:
            self.logger.warning("No PEP vintage for year %d, skipping", year)
            return

        url, params = _build_pep_params(vintage, year)
        self._inject_geography(params)
        self._inject_api_key(params)

        records = self._fetch_census_data(
            url,
            params,
            f"PEP population year={year} vintage={vintage}",
        )
        if not records:
            return

        yield from self._yield_with_schema_check(
            records,
            lambda r: self._transform_pep_record(r, year, vintage),
        )


# ---------------------------------------------------------------------------
# Concrete PEP streams
# ---------------------------------------------------------------------------


class CountyPopulationStream(PepPopulationBaseStream):
    """County-level population estimates from PEP (2010-2019).

    County PEP data is only available from the 2019 vintage.
    For 2020+ county population, use CountyPopulationCharvStream.
    """

    name = "county_population"
    primary_keys: ClassVar[tuple[str, ...]] = ("year", "fips")
    replication_key = None
    requires_county_geography: ClassVar[bool] = True

    schema = th.PropertiesList(
        th.Property("year", th.IntegerType, required=True),
        th.Property("state_fips", th.StringType, required=True),
        th.Property("county_fips", th.StringType, required=True),
        th.Property("fips", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("population", th.IntegerType),
        th.Property("density", th.NumberType),
    ).to_dict()

    def _transform_pep_record(self, record: dict, year: int, vintage: int) -> dict:
        pop, density = _extract_pop_density(record, vintage, year)
        return self._build_county_record(
            record,
            year,
            population=pop,
            density=density,
        )


class StatePopulationStream(PepPopulationBaseStream):
    """State-level population estimates from PEP (2010-2021).

    Uses the 2019 vintage for 2010-2019 and the 2021 vintage for 2020-2021.
    """

    name = "state_population"
    primary_keys: ClassVar[tuple[str, ...]] = ("year", "state_fips")
    replication_key = None
    requires_county_geography: ClassVar[bool] = False

    schema = th.PropertiesList(
        th.Property("year", th.IntegerType, required=True),
        th.Property("state_fips", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("population", th.IntegerType),
        th.Property("density", th.NumberType),
    ).to_dict()

    def _transform_pep_record(self, record: dict, year: int, vintage: int) -> dict:
        pop, density = _extract_pop_density(record, vintage, year)
        return self._build_state_record(
            record,
            year,
            population=pop,
            density=density,
        )


# ---------------------------------------------------------------------------
# Decennial Census
# ---------------------------------------------------------------------------
# Endpoints verified via live API calls:
#   GET /data/2000/dec/sf1?get=P001001,NAME&for=county:*&in=state:*
#   GET /data/2010/dec/sf1?get=P001001,NAME&for=county:*&in=state:*
#   GET /data/2020/dec/pl?get=P1_001N,NAME&for=county:*&in=state:*

DECENNIAL_ENDPOINTS: dict[int, dict[str, str]] = {
    2000: {"path": "2000/dec/sf1", "pop_var": "P001001"},
    2010: {"path": "2010/dec/sf1", "pop_var": "P001001"},
    2020: {"path": "2020/dec/pl", "pop_var": "P1_001N"},
}


class DecennialPopulationStream(CensusStream):
    """Decennial Census population counts at the county level.

    The most accurate population count, conducted every 10 years.
    Supports 2000, 2010, and 2020.
    """

    name = "decennial_population"
    primary_keys: ClassVar[tuple[str, ...]] = ("year", "fips")
    replication_key = None
    requires_county_geography: ClassVar[bool] = True

    schema = th.PropertiesList(
        th.Property("year", th.IntegerType, required=True),
        th.Property("state_fips", th.StringType, required=True),
        th.Property("county_fips", th.StringType, required=True),
        th.Property("fips", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("population", th.IntegerType),
    ).to_dict()

    path = ""

    @property
    def partitions(self) -> list[dict[str, Any]]:
        """Create partitions only for valid decennial years from config."""
        years = self.config.get("years", [2020])
        return [{"year": y} for y in years if y in DECENNIAL_ENDPOINTS]

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Fetch decennial county population for a census year."""
        if context is None:
            return

        year = context["year"]
        endpoint = DECENNIAL_ENDPOINTS.get(year)
        if endpoint is None:
            return

        pop_var = endpoint["pop_var"]
        url = f"{self.url_base}/{endpoint['path']}"

        params: dict[str, Any] = {"get": f"{pop_var},NAME"}
        self._inject_geography(params)
        self._inject_api_key(params)

        records = self._fetch_census_data(
            url,
            params,
            f"decennial population year={year}",
        )
        if not records:
            return

        pop_key = pop_var.lower()
        yield from self._yield_with_schema_check(
            records,
            lambda r: self._build_county_record(
                r,
                year,
                population=safe_int(r.get(pop_key)),
            ),
        )


# ---------------------------------------------------------------------------
# PEP Population Characteristics (charv) — 2023 vintage
# ---------------------------------------------------------------------------
# Verified via live API calls:
#   GET /data/2023/pep/charv?get=POP,NAME&for=county:013&in=state:04
#       &AGE=0000&SEX=0&HISP=0&YEAR=2023&MONTH=7
#   GET /data/2023/pep/charv?get=POP,NAME&for=state:04
#       &AGE=0000&SEX=0&HISP=0&YEAR=2023&MONTH=7
# Variables: POP (int), YEAR (string), MONTH (string), NAME (string)
# Predicates: AGE=0000, SEX=0, HISP=0 for total population; MONTH=7 for
#   July estimates (MONTH=4 is the April 1 Census base, only for 2020).
# County support: YES (verified with 3,222 counties).
# State support: YES (verified).
# Years: 2020-2023. Only the 2023 vintage exists (2020-2022 return 404).
# Note: No DENSITY variable available in charv.

CHARV_VINTAGE = 2023
CHARV_YEARS = range(2020, 2024)


class PepCharvBaseStream(CensusStream):
    """Base stream for PEP population characteristics (charv) 2020-2023.

    Uses the 2023 pep/charv endpoint which provides county-level population
    for 2020-2023, filling the gap where pep/population 2021 vintage lacks
    county geography. Filters for total population (AGE=0000, SEX=0, HISP=0)
    and July estimates (MONTH=7).
    """

    path = ""

    @property
    def partitions(self) -> list[dict[str, Any]]:
        """One partition per configured year within charv range (2020-2023)."""
        default = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
        years = self.config.get("years", default)
        return [{"year": y} for y in years if y in CHARV_YEARS]

    def _transform_charv_record(self, record: dict, year: int) -> dict:
        """Transform a parsed charv record to the output schema."""
        raise NotImplementedError

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Fetch charv population for a single year partition."""
        if context is None:
            return

        year = context["year"]
        if year not in CHARV_YEARS:
            return

        url = f"{self.url_base}/{CHARV_VINTAGE}/pep/charv"
        params: dict[str, Any] = {
            "get": "POP,NAME",
            "AGE": "0000",
            "SEX": "0",
            "HISP": "0",
            "YEAR": str(year),
            "MONTH": "7",
        }
        self._inject_geography(params)
        self._inject_api_key(params)

        records = self._fetch_census_data(
            url,
            params,
            f"charv population year={year}",
        )
        if not records:
            return

        yield from self._yield_with_schema_check(
            records,
            lambda r: self._transform_charv_record(r, year),
        )


class CountyPopulationCharvStream(PepCharvBaseStream):
    """County-level population from PEP characteristics (2020-2023).

    Fills the county population gap for 2020-2023 where the pep/population
    2021 vintage only supports state-level geography.
    Note: No density data available from this endpoint.
    """

    name = "county_population_charv"
    primary_keys: ClassVar[tuple[str, ...]] = ("year", "fips")
    replication_key = None
    requires_county_geography: ClassVar[bool] = True

    schema = th.PropertiesList(
        th.Property("year", th.IntegerType, required=True),
        th.Property("state_fips", th.StringType, required=True),
        th.Property("county_fips", th.StringType, required=True),
        th.Property("fips", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("population", th.IntegerType),
    ).to_dict()

    def _transform_charv_record(self, record: dict, year: int) -> dict:
        return self._build_county_record(
            record,
            year,
            population=safe_int(record.get("pop")),
        )


class StatePopulationCharvStream(PepCharvBaseStream):
    """State-level population from PEP characteristics (2020-2023).

    Note: No density data available from this endpoint. For 2020-2021 state
    population with density, use StatePopulationStream (pep/population 2021 vintage).
    """

    name = "state_population_charv"
    primary_keys: ClassVar[tuple[str, ...]] = ("year", "state_fips")
    replication_key = None
    requires_county_geography: ClassVar[bool] = False

    schema = th.PropertiesList(
        th.Property("year", th.IntegerType, required=True),
        th.Property("state_fips", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("population", th.IntegerType),
    ).to_dict()

    def _transform_charv_record(self, record: dict, year: int) -> dict:
        return self._build_state_record(
            record,
            year,
            population=safe_int(record.get("pop")),
        )
