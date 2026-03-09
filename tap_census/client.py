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

from tap_census.helpers import parse_census_array

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context
    from singer_sdk.pagination import BaseAPIPaginator


class CensusStream(RESTStream, ABC):
    """Base stream class for Census API endpoints.

    Handles the Census API's unique 2D array response format, rate limiting,
    and retry logic with exponential backoff.
    """

    records_jsonpath: ClassVar[str] = "$[*]"
    _request_timestamps: ClassVar[deque] = deque()

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", "https://api.census.gov/data")

    @override
    @property
    def authenticator(self) -> None:
        """Census API uses query parameter auth, not header auth."""
        return None

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        return {"User-Agent": "tap-census/0.0.1 (Singer SDK)"}

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

    def _throttle(self) -> None:
        """Enforce rate limiting based on max_requests_per_minute config."""
        max_rpm = self.config.get("max_requests_per_minute", 60)
        now = time.monotonic()

        # Clean old timestamps outside the 60-second window
        while self._request_timestamps and (now - self._request_timestamps[0]) > 60:
            self._request_timestamps.popleft()

        if len(self._request_timestamps) >= max_rpm:
            wait_time = 60 - (now - self._request_timestamps[0])
            if wait_time > 0:
                self.logger.info("Rate limit reached, waiting %.1f seconds", wait_time)
                time.sleep(wait_time)

        self._request_timestamps.append(time.monotonic())

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
            and 400 <= e.response.status_code < 500
            and e.response.status_code != 429
        ),
    )
    def _make_request(
        self, url: str, params: dict[str, Any],
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
        self, url: str, params: dict[str, Any], description: str,
    ) -> list[dict] | None:
        """Fetch and parse Census data with standardized error handling.

        Consolidates the common pattern of: make request → handle errors →
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
