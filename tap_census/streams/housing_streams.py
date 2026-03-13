"""Housing unit estimate streams for Census Bureau PEP housing data.

API documentation verified against live Census Bureau endpoint:
- PEP housing 2019 vintage: /data/2019/pep/housing
  Variables: HUEST (Housing Unit Estimate), DATE_CODE, NAME, state, county
  County support: YES (verified). State support: YES (verified).
  DATE_CODE mapping (same as PEP population):
    DATE_CODE 1 = 4/1/2010 Census housing unit count
    DATE_CODE 2 = 4/1/2010 housing unit estimates base
    DATE_CODE 3 = 7/1/2010 estimate  (year - 2007 = 3)
    DATE_CODE 4 = 7/1/2011 estimate  (year - 2007 = 4)
    ...
    DATE_CODE 12 = 7/1/2019 estimate (year - 2007 = 12)
  We use DATE_CODE 3-12 (July estimates), skipping 1-2 (Census base counts).
  Vintages 2013-2019 confirmed available (200). 2020+ return 404.
  Implementation uses 2019 vintage only (covers all years 2010-2019).
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th

from tap_census.client import CensusStream
from tap_census.helpers import safe_int

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

# Housing estimates use the 2019 vintage, covering years 2010-2019.
# DATE_CODE = year - 2007 for July estimates.
HOUSING_VINTAGE = 2019
HOUSING_YEARS = range(2010, 2020)


class HousingUnitsBaseStream(CensusStream):
    """Base stream for PEP housing unit estimates (2010-2019).

    Uses the 2019 pep/housing endpoint with DATE_CODE filtering.
    Subclasses set ``requires_county_geography`` and override
    ``_transform_housing_record``.
    """

    path = ""

    @property
    def partitions(self) -> list[dict[str, Any]]:
        """One partition per configured year that falls within 2010-2019."""
        default = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
        years = self.config.get("years", default)
        return [{"year": y} for y in years if y in HOUSING_YEARS]

    def _transform_housing_record(self, record: dict, year: int) -> dict:
        """Transform a parsed Census record to the output schema."""
        raise NotImplementedError

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Fetch housing unit estimates for a single year partition."""
        if context is None:
            return

        year = context["year"]
        if year not in HOUSING_YEARS:
            return

        date_code = year - 2007
        url = f"{self.url_base}/{HOUSING_VINTAGE}/pep/housing"
        params: dict[str, Any] = {
            "get": "HUEST,NAME",
            "DATE_CODE": str(date_code),
        }
        self._inject_geography(params)
        self._inject_api_key(params)

        records = self._fetch_census_data(
            url,
            params,
            f"housing units year={year} date_code={date_code}",
        )
        if not records:
            return

        yield from self._yield_with_schema_check(
            records,
            lambda r: self._transform_housing_record(r, year),
        )


class CountyHousingUnitsStream(HousingUnitsBaseStream):
    """County-level housing unit estimates from PEP (2010-2019).

    Verified via:
        GET /data/2019/pep/housing?get=HUEST,NAME&for=county:*&in=state:04&DATE_CODE=12
    """

    name = "county_housing_units"
    primary_keys: ClassVar[tuple[str, ...]] = ("year", "fips")
    replication_key = None
    requires_county_geography: ClassVar[bool] = True

    schema = th.PropertiesList(
        th.Property("year", th.IntegerType, required=True),
        th.Property("state_fips", th.StringType, required=True),
        th.Property("county_fips", th.StringType, required=True),
        th.Property("fips", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("housing_units", th.IntegerType),
    ).to_dict()

    def _transform_housing_record(self, record: dict, year: int) -> dict:
        return self._build_county_record(
            record,
            year,
            housing_units=safe_int(record.get("huest")),
        )


class StateHousingUnitsStream(HousingUnitsBaseStream):
    """State-level housing unit estimates from PEP (2010-2019).

    Verified via:
        GET /data/2019/pep/housing?get=HUEST,NAME&for=state:*&DATE_CODE=12
    """

    name = "state_housing_units"
    primary_keys: ClassVar[tuple[str, ...]] = ("year", "state_fips")
    replication_key = None
    requires_county_geography: ClassVar[bool] = False

    schema = th.PropertiesList(
        th.Property("year", th.IntegerType, required=True),
        th.Property("state_fips", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("housing_units", th.IntegerType),
    ).to_dict()

    def _transform_housing_record(self, record: dict, year: int) -> dict:
        return self._build_state_record(
            record,
            year,
            housing_units=safe_int(record.get("huest")),
        )
