"""Geography reference streams for Census Bureau data.

The county geography data is sourced from the Census Bureau Gazetteer files
(tab-delimited zip archive). Verified columns from the actual file:
    USPS, GEOID, ANSICODE, NAME, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI,
    INTPTLAT, INTPTLONG

Gazetteer availability verified via live HTTP status checks:
    2020-2025: ALL return 200 (available).
    The 2024 Gazetteer has 3,222 counties including the 9 new Connecticut
    planning regions (FIPS 09110-09190) which replaced the old 8 counties
    (09001-09015) starting in 2022.
"""

from __future__ import annotations

import csv
import io
import sys
import zipfile
from typing import TYPE_CHECKING, ClassVar

import backoff
import requests as requests_lib
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_census.helpers import safe_float

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

GAZETTEER_BASE_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"
DEFAULT_GAZETTEER_YEAR = 2024


class CountyGeographyStream(Stream):
    """FIPS code reference table mapping counties to lat/lon centroids.

    Sources data from the Census Bureau Gazetteer files (zip archive
    with a tab-delimited text file). This stream does NOT use the REST API
    and instead inherits from the base Stream class.

    The Gazetteer year is configurable via the ``gazetteer_year`` setting
    (default: 2024). Available years: 2020-2025.
    """

    name = "county_geography"
    primary_keys: ClassVar[tuple[str, ...]] = ("fips",)
    replication_key = None

    schema = th.PropertiesList(
        th.Property("fips", th.StringType, required=True),
        th.Property("state_abbrev", th.StringType),
        th.Property("name", th.StringType),
        th.Property("latitude", th.NumberType, required=True),
        th.Property("longitude", th.NumberType, required=True),
        th.Property("land_area_sqmi", th.NumberType),
        th.Property("water_area_sqmi", th.NumberType),
    ).to_dict()

    @property
    def _gazetteer_year(self) -> int:
        return self.config.get("gazetteer_year", DEFAULT_GAZETTEER_YEAR)

    @property
    def _gazetteer_zip_url(self) -> str:
        year = self._gazetteer_year
        return f"{GAZETTEER_BASE_URL}/{year}_Gazetteer/{year}_Gaz_counties_national.zip"

    @property
    def _gazetteer_filename(self) -> str:
        year = self._gazetteer_year
        return f"{year}_Gaz_counties_national.txt"

    @backoff.on_exception(
        backoff.expo,
        (requests_lib.exceptions.RequestException,),
        max_tries=5,
        max_time=120,
        factor=5,
        jitter=backoff.full_jitter,
    )
    def _download_gazetteer(self) -> str:
        """Download and extract the Gazetteer zip file with retry logic.

        Returns:
            The raw text content of the Gazetteer tab-delimited file.
        """
        url = self._gazetteer_zip_url
        self.logger.info(
            "Downloading county geography from %d Gazetteer",
            self._gazetteer_year,
        )
        response = requests_lib.get(url, timeout=(10, 60))
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Find the counties file in the archive (handles minor name variations)
            names = zf.namelist()
            target = self._gazetteer_filename
            filename = target if target in names else names[0]
            with zf.open(filename) as f:
                return f.read().decode("utf-8")

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Download and parse the Gazetteer file into records.

        Args:
            context: The stream context (unused).

        Yields:
            County geography records with FIPS, lat/lon, and area data.
        """
        try:
            text = self._download_gazetteer()
        except Exception:
            self.logger.exception("Failed to download Gazetteer file")
            if self.config.get("strict_mode", False):
                raise
            return

        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        for row in reader:
            # Gazetteer columns have trailing whitespace in headers
            cleaned = {k.strip(): v.strip() if v else v for k, v in row.items()}

            fips = cleaned.get("GEOID", "")
            if not fips:
                continue

            yield {
                "fips": fips,
                "state_abbrev": cleaned.get("USPS", ""),
                "name": cleaned.get("NAME", ""),
                "latitude": safe_float(cleaned.get("INTPTLAT")),
                "longitude": safe_float(cleaned.get("INTPTLONG")),
                "land_area_sqmi": safe_float(cleaned.get("ALAND_SQMI")),
                "water_area_sqmi": safe_float(cleaned.get("AWATER_SQMI")),
            }
