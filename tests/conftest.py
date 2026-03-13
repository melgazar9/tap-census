"""Pytest configuration and shared fixtures."""

import warnings

# backoff <3.0 uses asyncio.iscoroutinefunction, deprecated in Python 3.14+.
# This must run before any backoff import to prevent DeprecationWarning errors.
# Remove once backoff releases a fix (https://github.com/litl/backoff/issues/219).
warnings.filterwarnings(
    "ignore",
    message=".*iscoroutinefunction.*",
    category=DeprecationWarning,
)
