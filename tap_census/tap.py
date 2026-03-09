"""Census tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_census.streams import (
    CountyGeographyStream,
    CountyPopulationStream,
    DecennialPopulationStream,
    StatePopulationStream,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapCensus(Tap):
    """Singer tap for U.S. Census Bureau population and geographic data."""

    name = "tap-census"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            secret=True,
            description=(
                "Census API key (free at https://api.census.gov/data/key_signup.html). "
                "Without a key, requests are limited to 500/day."
            ),
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.census.gov/data",
            description="Base URL for the Census Bureau API.",
        ),
        th.Property(
            "years",
            th.ArrayType(th.IntegerType),
            default=[2015, 2016, 2017, 2018, 2019, 2020, 2021],
            description=(
                "Which years to pull population estimates for. "
                "PEP county data available: 2010-2019. "
                "PEP state data available: 2010-2021. "
                "Decennial: 2000, 2010, 2020."
            ),
        ),
        th.Property(
            "geography_levels",
            th.ArrayType(th.StringType),
            default=["county"],
            description='Geographic levels to extract. Options: "county", "state".',
        ),
        th.Property(
            "datasets",
            th.ArrayType(th.StringType),
            default=["pep"],
            description='Datasets to extract. Options: "pep", "decennial".',
        ),
        th.Property(
            "states",
            th.ArrayType(th.StringType),
            default=["*"],
            description=(
                'State FIPS codes to filter by. ["*"] = all states. '
                '["06", "36"] = California + New York.'
            ),
        ),
        th.Property(
            "max_requests_per_minute",
            th.IntegerType,
            default=60,
            description="Maximum API requests per minute for rate limiting.",
        ),
        th.Property(
            "strict_mode",
            th.BooleanType,
            default=False,
            description=(
                "If True, raise exceptions on API errors. "
                "If False, log warnings and skip failed partitions."
            ),
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list:
        """Return a list of discovered streams based on configuration.

        Returns:
            A list of stream instances.
        """
        streams = []
        datasets = self.config.get("datasets", ["pep"])
        geo_levels = self.config.get("geography_levels", ["county"])

        if "pep" in datasets:
            if "county" in geo_levels:
                streams.append(CountyPopulationStream(self))
            if "state" in geo_levels:
                streams.append(StatePopulationStream(self))

        if "decennial" in datasets:
            streams.append(DecennialPopulationStream(self))

        # County geography is always included — it's essential for
        # joining weather data to population weights via lat/lon centroids.
        streams.append(CountyGeographyStream(self))

        return streams


if __name__ == "__main__":
    TapCensus.cli()
