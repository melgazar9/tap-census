"""Census tap class."""

from __future__ import annotations

import sys
from typing import ClassVar

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_census.streams import (
    CountyGeographyStream,
    CountyHousingUnitsStream,
    CountyPopulationCharvStream,
    CountyPopulationStream,
    DecennialPopulationStream,
    StateHousingUnitsStream,
    StatePopulationCharvStream,
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
            default=[2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
            description=(
                "Which years to pull data for. "
                "PEP county population: 2010-2019. "
                "PEP state population: 2010-2021. "
                "PEP charv population: 2020-2023. "
                "PEP housing: 2010-2019. "
                "Decennial: 2000, 2010, 2020."
            ),
        ),
        th.Property(
            "geography_levels",
            th.ArrayType(th.StringType),
            default=["county", "state"],
            description='Geographic levels to extract. Options: "county", "state".',
        ),
        th.Property(
            "datasets",
            th.ArrayType(th.StringType),
            default=["pep", "charv"],
            description=(
                'Datasets to extract. Options: "pep", "charv", "decennial", "housing". '
                '"pep" = population estimates 2010-2021 (county 2010-2019, state 2010-2021). '
                '"charv" = population characteristics 2020-2023 (county + state). '
                '"decennial" = decennial census counts (2000, 2010, 2020). '
                '"housing" = housing unit estimates 2010-2019.'
            ),
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
        th.Property(
            "gazetteer_year",
            th.IntegerType,
            default=2024,
            description=(
                "Gazetteer file year for county geography (lat/lon centroids). "
                "Available: 2020-2025. Use 2024+ for CT planning region FIPS codes."
            ),
        ),
    ).to_dict()

    # Maps (dataset, geo_level | None) to stream class.
    # geo_level=None means the stream is always included for that dataset.
    _STREAM_REGISTRY: ClassVar[list[tuple[str, str | None, type]]] = [
        ("pep", "county", CountyPopulationStream),
        ("pep", "state", StatePopulationStream),
        ("charv", "county", CountyPopulationCharvStream),
        ("charv", "state", StatePopulationCharvStream),
        ("decennial", None, DecennialPopulationStream),
        ("housing", "county", CountyHousingUnitsStream),
        ("housing", "state", StateHousingUnitsStream),
    ]

    @override
    def discover_streams(self) -> list:
        """Return a list of discovered streams based on configuration.

        Returns:
            A list of stream instances.
        """
        datasets = set(self.config.get("datasets", ["pep", "charv"]))
        geo_levels = set(self.config.get("geography_levels", ["county", "state"]))

        streams = [
            cls(self)
            for dataset, geo, cls in self._STREAM_REGISTRY
            if dataset in datasets and (geo is None or geo in geo_levels)
        ]

        # County geography is always included — it's essential for
        # joining weather data to population weights via lat/lon centroids.
        streams.append(CountyGeographyStream(self))

        return streams


if __name__ == "__main__":
    TapCensus.cli()
