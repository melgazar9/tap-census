"""Population streams for Census Bureau PEP and Decennial data.

API documentation verified against live Census Bureau endpoints:
- PEP 2019 vintage: /data/2019/pep/population (variables: POP, DENSITY, NAME, DATE_CODE)
  Covers years 2010-2019 at county and state level.
  DATE_CODE mapping: year 2010 -> 3, 2011 -> 4, ..., 2019 -> 12.
- PEP 2021 vintage: /data/2021/pep/population (variables: POP_{year}, DENSITY_{year}, NAME)
  Covers years 2020-2021 at state level ONLY (no county geography).
- Decennial 2000/2010: /data/{year}/dec/sf1 (variable: P001001)
- Decennial 2020: /data/2020/dec/pl (variable: P1_001N)
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th

from tap_census.client import CensusStream
from tap_census.helpers import make_fips, safe_float, safe_int

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


def _find_vintage(year: int, need_county: bool) -> int | None:
    """Find the appropriate PEP vintage for a given year and geography need."""
    for vintage, meta in PEP_VINTAGES.items():
        if year in meta["years"] and (not need_county or meta["supports_county"]):
            return vintage
    return None


def _build_pep_params(
    vintage: int,
    year: int,
    geo_for: str,
    geo_in: str | None,
    api_key: str | None,
) -> tuple[str, dict[str, Any]]:
    """Build the URL and query params for a PEP API request."""
    meta = PEP_VINTAGES[vintage]
    url = f"https://api.census.gov/data/{vintage}/pep/population"

    params: dict[str, Any] = {"for": geo_for}
    if geo_in:
        params["in"] = geo_in
    if api_key:
        params["key"] = api_key

    if meta["variable_style"] == "date_code":
        params["get"] = "POP,DENSITY,NAME"
        params["DATE_CODE"] = str(year - 2007)
    else:
        params["get"] = f"POP_{year},DENSITY_{year},NAME"

    return url, params


def _extract_pop_density(
    record: dict[str, str], vintage: int, year: int,
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

    Subclasses define requires_county_geography, get_geography_params, and transform_census_record_to_output
    to customize for county vs. state geography.
    """

    requires_county_geography: ClassVar[bool]

    @property
    def path(self) -> str:
        """Not used — requests are built in get_records."""
        return ""

    @property
    def partitions(self) -> list[dict[str, Any]]:
        """One partition per configured year that has available PEP data."""
        default = [2015, 2016, 2017, 2018, 2019, 2020, 2021]
        years = self.config.get("years", default)
        return [{"year": y} for y in years if _find_vintage(y, self.requires_county_geography)]

    def get_geography_params(self) -> tuple[str, str | None]:
        """Return (geo_for, geo_in) clauses. Overridden by subclasses."""
        raise NotImplementedError

    def transform_census_record_to_output(
        self, record: dict, year: int, vintage: int,
    ) -> dict:
        """Transform a parsed Census record to the output schema."""
        raise NotImplementedError

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Fetch PEP population for a single year partition."""
        if context is None:
            return

        year = context["year"]
        vintage = _find_vintage(year, self.requires_county_geography)
        if vintage is None:
            self.logger.warning("No PEP vintage for year %d, skipping", year)
            return

        geo_for, geo_in = self.get_geography_params()
        api_key = self.config.get("api_key")
        url, params = _build_pep_params(vintage, year, geo_for, geo_in, api_key)

        records = self._fetch_census_data(
            url, params, f"PEP population year={year} vintage={vintage}",
        )
        if not records:
            return

        for record in records:
            yield self.transform_census_record_to_output(record, year, vintage)


# ---------------------------------------------------------------------------
# Concrete PEP streams
# ---------------------------------------------------------------------------

class CountyPopulationStream(PepPopulationBaseStream):
    """County-level population estimates from PEP (2010-2019).

    County PEP data is only available from the 2019 vintage.
    For 2020 county population, use DecennialPopulationStream.
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

    def get_geography_params(self) -> tuple[str, str | None]:
        states = self.config.get("states", ["*"])
        geo_in = "state:*" if states == ["*"] else f"state:{','.join(states)}"
        return "county:*", geo_in

    def transform_census_record_to_output(
        self, record: dict, year: int, vintage: int,
    ) -> dict:
        state_fips = record.get("state", "")
        county_fips = record.get("county", "")
        pop, density = _extract_pop_density(record, vintage, year)
        return {
            "year": year,
            "state_fips": state_fips,
            "county_fips": county_fips,
            "fips": make_fips(state_fips, county_fips),
            "name": record.get("name"),
            "population": pop,
            "density": density,
        }


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

    def get_geography_params(self) -> tuple[str, str | None]:
        states = self.config.get("states", ["*"])
        geo_for = "state:*" if states == ["*"] else f"state:{','.join(states)}"
        return geo_for, None

    def transform_census_record_to_output(
        self, record: dict, year: int, vintage: int,
    ) -> dict:
        pop, density = _extract_pop_density(record, vintage, year)
        return {
            "year": year,
            "state_fips": record.get("state", ""),
            "name": record.get("name"),
            "population": pop,
            "density": density,
        }


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

    schema = th.PropertiesList(
        th.Property("year", th.IntegerType, required=True),
        th.Property("state_fips", th.StringType, required=True),
        th.Property("county_fips", th.StringType, required=True),
        th.Property("fips", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("population", th.IntegerType),
    ).to_dict()

    @property
    def path(self) -> str:
        """Not used — requests are built in get_records."""
        return ""

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

        params: dict[str, Any] = {
            "get": f"{pop_var},NAME",
            "for": "county:*",
            "in": "state:*",
        }
        api_key = self.config.get("api_key")
        if api_key:
            params["key"] = api_key
        states = self.config.get("states", ["*"])
        if states != ["*"]:
            params["in"] = f"state:{','.join(states)}"

        records = self._fetch_census_data(
            url, params, f"decennial population year={year}",
        )
        if not records:
            return

        for record in records:
            state_fips = record.get("state", "")
            county_fips = record.get("county", "")
            yield {
                "year": year,
                "state_fips": state_fips,
                "county_fips": county_fips,
                "fips": make_fips(state_fips, county_fips),
                "name": record.get("name"),
                "population": safe_int(record.get(pop_var.lower())),
            }
