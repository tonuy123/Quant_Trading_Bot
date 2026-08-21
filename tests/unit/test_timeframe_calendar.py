"""DATA-TIME-001: calendar-month interval semantics and Timeframe parsing.

All expected epoch values are hardcoded literals derived from the UTC
calendar, never from a production duration map.
"""

from __future__ import annotations

import pytest

from packages.domain.enums.timeframe import (
    SUPPORTED_INTERVALS,
    Timeframe,
    interval_boundary_after,
)

# Hardcoded UTC calendar literals (independent of production maps).
JAN_1_2024 = 1_704_067_200_000
FEB_1_2024 = 1_706_745_600_000  # 2024 is a leap year
MAR_1_2024 = 1_709_251_200_000  # February 2024 has 29 days
FEB_1_2023 = 1_675_209_600_000  # normal year
MAR_1_2023 = 1_677_628_800_000  # February 2023 has 28 days
APR_1_2024 = 1_711_929_600_000
MAY_1_2024 = 1_714_521_600_000
DEC_1_2023 = 1_701_388_800_000
DEC_1_9999 = 253_399_622_400_000


class TestTimeframeParsing:
    def test_1m_is_minute(self) -> None:
        assert Timeframe.from_string("1m") is Timeframe.M1

    def test_1M_is_monthly(self) -> None:
        assert Timeframe.from_string("1M") is Timeframe.MO1

    def test_1m_and_1M_never_case_fold_together(self) -> None:
        assert Timeframe.from_string("1m") is not Timeframe.from_string("1M")

    def test_other_intervals_stay_case_insensitive(self) -> None:
        assert Timeframe.from_string("1H") is Timeframe.H1
        assert Timeframe.from_string("1d") is Timeframe.D1

    def test_unknown_interval_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown timeframe"):
            Timeframe.from_string("7m")

    def test_non_string_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown timeframe"):
            Timeframe.from_string(1)  # type: ignore[arg-type]

    def test_monthly_seconds_never_lies(self) -> None:
        with pytest.raises(ValueError, match="no fixed duration"):
            _ = Timeframe.MO1.seconds

    def test_fixed_seconds_unchanged(self) -> None:
        assert Timeframe.M1.seconds == 60
        assert Timeframe.D1.seconds == 86_400


class TestCalendarBoundaryFixedIntervals:
    def test_fixed_intervals_retain_existing_behavior(self) -> None:
        assert interval_boundary_after(0, "1m") == 60_000
        assert interval_boundary_after(1_000_000, "3m") == 1_180_000
        assert interval_boundary_after(1_000_000, "1h") == 4_600_000
        assert interval_boundary_after(1_000_000, "1d") == 87_400_000
        assert interval_boundary_after(1_000_000, "1w") == 605_800_000


class TestCalendarBoundaryMonthly:
    def test_january_to_february(self) -> None:
        assert interval_boundary_after(JAN_1_2024, "1M") == FEB_1_2024

    def test_february_to_march_normal_year(self) -> None:
        assert interval_boundary_after(FEB_1_2023, "1M") == MAR_1_2023

    def test_february_to_march_leap_year_2024(self) -> None:
        assert interval_boundary_after(FEB_1_2024, "1M") == MAR_1_2024

    def test_april_to_may(self) -> None:
        assert interval_boundary_after(APR_1_2024, "1M") == MAY_1_2024

    def test_december_to_january_next_year(self) -> None:
        assert interval_boundary_after(DEC_1_2023, "1M") == JAN_1_2024

    def test_monthly_open_not_aligned_rejected(self) -> None:
        with pytest.raises(ValueError, match="align"):
            interval_boundary_after(JAN_1_2024 + 60_000, "1M")

    def test_monthly_open_at_non_utc_midnight_rejected(self) -> None:
        with pytest.raises(ValueError, match="align"):
            interval_boundary_after(FEB_1_2024 + 3_600_000, "1M")


class TestCalendarBoundaryInputValidation:
    @pytest.mark.parametrize("value", [True, False, 1.5, "1", None, -1, object()])
    def test_malformed_open_ms_rejected(self, value: object) -> None:
        with pytest.raises(ValueError, match="open_ms"):
            interval_boundary_after(value, "1m")  # type: ignore[arg-type]

    def test_unsupported_interval_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported interval"):
            interval_boundary_after(0, "7m")

    def test_interval_not_string_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported interval"):
            interval_boundary_after(0, 1)  # type: ignore[arg-type]

    def test_overflow_open_ms_rejected(self) -> None:
        with pytest.raises(ValueError, match="out of supported datetime range"):
            interval_boundary_after(10**19, "1M")

    def test_next_month_beyond_datetime_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="out of supported datetime range"):
            interval_boundary_after(DEC_1_9999, "1M")

    def test_supported_intervals_contain_monthly(self) -> None:
        assert "1M" in SUPPORTED_INTERVALS
        assert "1m" in SUPPORTED_INTERVALS
        assert len(SUPPORTED_INTERVALS) == 15
