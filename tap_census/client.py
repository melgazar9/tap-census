"""REST client handling, including CensusStream base class."""

from __future__ import annotations

import sys
import time
from abc import ABC
from collections import deque
from typing import TYPE_CHECKING, Any, ClassVar

import backoff
import requests as requests_lib
from singer_sdk.pagination import SinglePagePaginator
from singer_sdk.streams import RESTStream

from tap_census.helpers import make_fips, parse_census_array

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    import requests
    from singer_sdk.helpers.types import Context
    from singer_sdk.pagination import BaseAPIPaginator


_RATE_LIMIT_WINDOW_SECS = 60
_HTTP_CLIENT_ERROR_MIN = 400
_HTTP_CLIENT_ERROR_MAX = 500
_HTTP_TOO_MANY_REQUESTS = 429


class CensusStream(RESTStream, ABC):
    """Base stream class for Census API endpoints.

    Handles the Census API's unique 2D array response format, rate limiting,
    retry logic with exponential backoff, and shared geography/record building.
    """

    records_jsonpath: ClassVar[str] = "$[*]"
    _request_timestamps: ClassVar[deque] = deque()

    # Subclasses set this to True for county-level streams.
    requires_county_geography: ClassVar[bool] = False

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", "https://api.census.gov/data")

    @override
    @property
    def authenticator(self) -> None:  # type: ignore[override]
        """Census API uses query parameter auth, not header auth."""
        return None

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        return {"User-Agent": "tap-census/0.1.0 (Singer SDK)"}

    @override
    def get_new_paginator(self) -> BaseAPIPaginator:
        """Census API returns all results in a single response."""
        return SinglePagePaginator()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL query parameters including the API key."""
        params: dict[str, Any] = {}
        api_key = self.config.get("api_key")
        if api_key:
            params["key"] = api_key
        return params

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Enforce rate limiting based on max_requests_per_minute config."""
        max_rpm = self.config.get("max_requests_per_minute", 60)
        now = time.monotonic()

        # Clean old timestamps outside the rate-limit window
        window = _RATE_LIMIT_WINDOW_SECS
        while self._request_timestamps and (now - self._request_timestamps[0]) > window:
            self._request_timestamps.popleft()

        if len(self._request_timestamps) >= max_rpm:
            wait_time = _RATE_LIMIT_WINDOW_SECS - (now - self._request_timestamps[0])
            if wait_time > 0:
                self.logger.info("Rate limit reached, waiting %.1f seconds", wait_time)
                time.sleep(wait_time)

        self._request_timestamps.append(time.monotonic())

    # ------------------------------------------------------------------
    # HTTP request / response handling
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        (requests_lib.exceptions.RequestException,),
        max_tries=5,
        max_time=120,
        factor=5,
        jitter=backoff.full_jitter,
        giveup=lambda e: (
            isinstance(e, requests_lib.exceptions.HTTPError)
            and e.response is not None
            and _HTTP_CLIENT_ERROR_MIN <= e.response.status_code < _HTTP_CLIENT_ERROR_MAX
            and e.response.status_code != _HTTP_TOO_MANY_REQUESTS
        ),
    )
    def _make_request(
        self,
        url: str,
        params: dict[str, Any],
    ) -> list[list[str]] | None:
        """Make an HTTP request to the Census API with retry logic.

        Args:
            url: The full URL to request.
            params: Query parameters.

        Returns:
            Parsed JSON response (2D array) or None on non-critical failure.
        """
        self._throttle()
        response = self.requests_session.get(url, params=params, timeout=(10, 30))
        response.raise_for_status()
        return response.json()

    def _fetch_census_data(
        self,
        url: str,
        params: dict[str, Any],
        description: str,
    ) -> list[dict] | None:
        """Fetch and parse Census data with standardized error handling.

        Consolidates the common pattern of: make request -> handle errors ->
        parse 2D array. Respects the strict_mode config setting.

        Args:
            url: The Census API URL.
            params: Query parameters.
            description: Human-readable description for log messages.

        Returns:
            Parsed list of record dicts, or None on failure.
        """
        try:
            data = self._make_request(url, params)
        except Exception:
            self.logger.warning("Failed to fetch %s", description, exc_info=True)
            if self.config.get("strict_mode", False):
                raise
            return None

        if not data:
            return None
        return parse_census_array(data)

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the Census 2D array response into dictionaries."""
        yield from parse_census_array(response.json())

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process a record. Override in subclasses for transformations."""
        return row

    # ------------------------------------------------------------------
    # Schema drift detection (following tap-massive pattern)
    # ------------------------------------------------------------------

    _schema_checked: ClassVar[set[str]] = set()

    def _check_output_schema_drift(self, output_record: dict) -> None:
        """Check a transformed output record against declared schema.

        Called once per partition to detect schema mismatches early.
        """
        cache_key = f"{self.name}:{sorted(output_record.keys())}"
        if cache_key in self._schema_checked:
            return
        self._schema_checked.add(cache_key)

        schema_fields = set(self.schema.get("properties", {}).keys())
        record_keys = set(output_record.keys())

        missing_in_record = schema_fields - record_keys
        extra_in_record = record_keys - schema_fields

        if extra_in_record:
            self.logger.critical(
                "SCHEMA DRIFT: %s has fields not in schema: %s. "
                "The Census API may have changed. Update the stream schema.",
                self.name,
                extra_in_record,
            )
        if missing_in_record:
            self.logger.debug(
                "%s: schema fields missing from record (may be optional): %s",
                self.name,
                missing_in_record,
            )

    def _yield_with_schema_check(
        self,
        records: Iterable[dict],
        transform: Callable[[dict], dict],
    ) -> Iterable[dict]:
        """Transform records and check schema drift on the first output.

        Consolidates the common pattern used by all stream ``get_records``
        implementations: transform each raw record and run the schema drift
        check exactly once per partition.
        """
        first = True
        for record in records:
            output = transform(record)
            if first:
                self._check_output_schema_drift(output)
                first = False
            yield output

    # ------------------------------------------------------------------
    # Shared geography & record-building helpers
    # ------------------------------------------------------------------

    def _build_geography_params(self) -> tuple[str, str | None]:
        """Build Census API geography clauses from config.

        Uses ``requires_county_geography`` to decide between county and
        state geography. Respects the ``states`` config for filtering.

        Returns:
            (geo_for, geo_in) tuple for Census API ``for``/``in`` params.
        """
        states = self.config.get("states", ["*"])
        if self.requires_county_geography:
            geo_in = "state:*" if states == ["*"] else f"state:{','.join(states)}"
            return "county:*", geo_in
        geo_for = "state:*" if states == ["*"] else f"state:{','.join(states)}"
        return geo_for, None

    def _build_county_record(self, record: dict, year: int, **extra: Any) -> dict:
        """Build a standard county-level output record with FIPS code.

        Args:
            record: Parsed Census API record (lowercased keys).
            year: The data year.
            **extra: Additional fields to include (e.g. population, density).

        Returns:
            Output record dict.
        """
        state_fips = record.get("state", "")
        county_fips = record.get("county", "")
        return {
            "year": year,
            "state_fips": state_fips,
            "county_fips": county_fips,
            "fips": make_fips(state_fips, county_fips),
            "name": record.get("name"),
            **extra,
        }

    def _build_state_record(self, record: dict, year: int, **extra: Any) -> dict:
        """Build a standard state-level output record.

        Args:
            record: Parsed Census API record (lowercased keys).
            year: The data year.
            **extra: Additional fields to include (e.g. population, density).

        Returns:
            Output record dict.
        """
        return {
            "year": year,
            "state_fips": record.get("state", ""),
            "name": record.get("name"),
            **extra,
        }

    def _inject_api_key(self, params: dict[str, Any]) -> None:
        """Add the API key to params if configured."""
        api_key = self.config.get("api_key")
        if api_key:
            params["key"] = api_key

    def _inject_geography(self, params: dict[str, Any]) -> None:
        """Add geography for/in params from config."""
        geo_for, geo_in = self._build_geography_params()
        params["for"] = geo_for
        if geo_in:
            params["in"] = geo_in
