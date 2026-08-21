"""Pagination utilities."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass
class PaginatedResult[T]:
    """Result of a paginated query.

    Attributes:
        items: Items in this page
        total: Total number of items
        page: Current page number (1-indexed)
        page_size: Number of items per page
        total_pages: Total number of pages
        has_next: Whether there is a next page
        has_previous: Whether there is a previous page
    """

    items: list[T]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        """Calculate total pages."""
        if self.page_size == 0:
            return 0
        return (self.total + self.page_size - 1) // self.page_size

    @property
    def has_next(self) -> bool:
        """Check if there is a next page."""
        return self.page < self.total_pages

    @property
    def has_previous(self) -> bool:
        """Check if there is a previous page."""
        return self.page > 1

    @property
    def start_index(self) -> int:
        """Get start index (0-based) of this page."""
        return (self.page - 1) * self.page_size

    @property
    def end_index(self) -> int:
        """Get end index (exclusive) of this page."""
        return min(self.start_index + self.page_size, self.total)


async def paginate[T](
    items: list[T],
    page: int = 1,
    page_size: int = 20,
) -> PaginatedResult[T]:
    """Paginate a list of items.

    Args:
        items: All items
        page: Page number (1-indexed)
        page_size: Items per page

    Returns:
        Paginated result.
    """
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 20

    total = len(items)

    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    page_items = items[start_idx:end_idx]

    return PaginatedResult(
        items=page_items,
        total=total,
        page=page,
        page_size=page_size,
    )


def chunk[T](iterable: list[T], chunk_size: int) -> Iterator[list[T]]:
    """Split iterable into chunks.

    Args:
        iterable: Items to chunk
        chunk_size: Size of each chunk

    Yields:
        Chunks of items.
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


async def chunk_async[T](
    items: list[T],
    chunk_size: int,
) -> AsyncIterator[list[T]]:
    """Split iterable into chunks (async version).

    Args:
        items: Items to chunk
        chunk_size: Size of each chunk

    Yields:
        Chunks of items.
    """
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]
