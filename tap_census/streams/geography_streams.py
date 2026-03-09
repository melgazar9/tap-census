"""Geography reference streams for Census Bureau data.

The county geography data is sourced from the Census Bureau's 2020 Gazetteer
files (tab-delimited zip archive). Verified columns from the actual file:
    USPS, GEOID, ANSICODE, NAME, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI,
    INTPTLAT, INTPTLONG

Source URL verified:
    https://www2.census.gov/geo/docs/maps-data/data/gazetteer/
    2020_Gazetteer/2020_Gaz_counties_national.zip
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

# 2020 Gazetteer county file (zip archive containing a tab-delimited txt).
# Verified via: curl -s -o /dev/null -w "%{http_code}" <url> → 200
GAZETTEER_ZIP_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2020_Gazetteer/2020_Gaz_counties_national.zip"
)
GAZETTEER_FILENAME = "2020_Gaz_counties_national.txt"


class CountyGeographyStream(Stream):
    """FIPS code reference table mapping counties to lat/lon centroids.

    Sources data from the Census Bureau 2020 Gazetteer files (zip archive
    with a tab-delimited text file). This stream does NOT use the REST API
    and instead inherits from the base Stream class.

    This data is critical for mapping weather grid points to counties
    for population-weighted calculations.
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
        self.logger.info("Downloading county geography data from Census Gazetteer")
        response = requests_lib.get(GAZETTEER_ZIP_URL, timeout=(10, 60))
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            with zf.open(GAZETTEER_FILENAME) as f:
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
