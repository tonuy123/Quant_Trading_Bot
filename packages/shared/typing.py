"""Shared type definitions."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any

# Type aliases for common patterns
type JSON = dict[str, Any] | list[Any] | str | int | float | bool | None
type JSONObject = dict[str, JSON]
type JSONArray = list[JSON]

__all__ = [
    "JSON",
    "AsyncIterator",
    "Iterator",
    "JSONArray",
    "JSONObject",
]
