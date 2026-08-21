"""Shared pytest fixtures for the quant trading bot test suite.

The autouse fixture below closes event loops that pytest-asyncio 0.26 leaves
in the global policy after tearing down its ``event_loop`` fixture
(``_provide_clean_event_loop`` creates a replacement loop it never closes).
On Windows this leaks a ProactorEventLoop plus its self-pipe socket pair.
The leak is invisible while the policy still references the loop; a sync test
that calls ``asyncio.run()`` resets the policy (``Runner.close`` sets the loop
to None), which lets the garbage collector finalize the loop and emit
``ResourceWarning: unclosed event loop`` / ``unclosed socket`` during pytest
teardown. Closing the stray loop here fixes the resource leak itself instead
of suppressing the warning.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.fixture(autouse=True)
def _close_stray_event_loops() -> None:
    yield
    policy = asyncio.get_event_loop_policy()
    try:
        loop = policy.get_event_loop()
    except RuntimeError:
        loop = None
    if loop is not None and not loop.is_closed() and not getattr(loop, "__pytest_asyncio", False):
        loop.close()
        policy.set_event_loop(None)
