"""MD-014 regression coverage for the deterministic fake-only local soak."""

from __future__ import annotations

from tests.soak.market_data_soak import render_report, run_controlled_soak


async def test_controlled_24_hour_soak_produces_complete_bounded_evidence(tmp_path) -> None:
    """The fixed 24-hour fixture must exercise all required fault classes locally."""
    result = await run_controlled_soak(tmp_path / "market_data_soak.sqlite3")

    assert result.passed is True
    assert (
        result.simulated_finished_at - result.simulated_started_at
        >= result.configuration.simulated_duration
    )
    assert result.metrics.raw_frame_count > 1_700
    assert result.metrics.accepted_count > 1_000
    assert result.metrics.duplicate_count > 0
    assert result.metrics.quarantined_count > 0
    assert result.metrics.ignored_count > 0
    assert result.metrics.gap_count == 3
    assert result.metrics.out_of_order_frame_count == 1
    assert result.metrics.disconnect_count == 1
    assert result.metrics.reconnect_count == 1
    assert result.metrics.recovery_attempts == 4
    assert result.metrics.recovery_successes == 3
    assert result.metrics.recovery_failures == 1
    assert result.metrics.rate_limit_429_count == 1
    assert result.metrics.rate_limit_blocked_attempts == 1
    assert result.metrics.stale_episode_count == 3
    assert result.metrics.stale_reminder_count == 3
    assert result.metrics.recovery_buffer_overflow_count == 1
    assert result.metrics.overflow_escalation_count == 1
    assert result.metrics.raw_capture_drop_count > 0
    assert result.storage.rows["raw_messages"] == result.configuration.raw_capacity
    assert result.storage.final_bytes > result.storage.initial_bytes
    assert all(evidence.passed for evidence in result.bounds)
    assert result.unexplained_critical_incidents == 0
    report = render_report(result, command="py -3.12 -m tests.soak.market_data_soak")
    assert "**Status:** PASS" in report
    assert "| Pre-normalization recovery-buffer drops | 1 |" in report
    assert "**Unexplained critical incidents:** 0" in report
