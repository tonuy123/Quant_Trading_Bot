"""Candle data validation."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from packages.market_data.contracts import CandleData


class CandleValidator:
    """Validates candle data for sanity."""

    MAX_CANDLE_GAP_HOURS = 24  # Max gap between candles
    MIN_CANDLE_VOLUME = Decimal("0")

    @classmethod
    def validate_candle(cls, candle: CandleData) -> list[str]:
        """Validate candle data.

        Args:
            candle: Candle to validate

        Returns:
            List of validation errors.
        """
        errors = []

        # Validate OHLC relationships
        if candle.high < candle.low:
            errors.append(f"High ({candle.high}) < Low ({candle.low})")

        if candle.open > candle.high:
            errors.append(f"Open ({candle.open}) > High ({candle.high})")

        if candle.open < candle.low:
            errors.append(f"Open ({candle.open}) < Low ({candle.low})")

        if candle.close > candle.high:
            errors.append(f"Close ({candle.close}) > High ({candle.high})")

        if candle.close < candle.low:
            errors.append(f"Close ({candle.close}) < Low ({candle.low})")

        # Validate volume
        if candle.volume < cls.MIN_CANDLE_VOLUME:
            errors.append(f"Invalid volume: {candle.volume}")

        # Validate time
        if candle.close_time <= candle.open_time:
            errors.append(f"Close time ({candle.close_time}) <= Open time ({candle.open_time})")

        return errors

    @classmethod
    def validate_candle_sequence(cls, candles: list[CandleData]) -> list[str]:
        """Validate a sequence of candles for continuity.

        Args:
            candles: List of candles in chronological order

        Returns:
            List of validation errors.
        """
        errors: list[str] = []

        for i, candle in enumerate(candles):
            # Validate individual candle
            errors.extend(f"[{i}] {err}" for err in cls.validate_candle(candle))

            # Check continuity with previous candle
            if i > 0:
                prev_candle = candles[i - 1]

                # Check gap
                gap = candle.open_time - prev_candle.close_time
                if gap > timedelta(hours=cls.MAX_CANDLE_GAP_HOURS):
                    errors.append(
                        f"[{i}] Large gap between candles: {gap.total_seconds() / 3600:.1f} hours"
                    )

                # Check for duplicate candles
                if candle.open_time == prev_candle.open_time:
                    errors.append(f"[{i}] Duplicate candle at {candle.open_time}")

        return errors

    @classmethod
    def calculate_candle_quality(cls, candle: CandleData) -> str:
        """Assess candle quality.

        Args:
            candle: Candle to assess

        Returns:
            Quality grade: "excellent", "good", "poor", "invalid".
        """
        errors = cls.validate_candle(candle)
        if errors:
            return "invalid"

        # Check volume
        if candle.volume < Decimal("0.001"):
            return "poor"

        # Check for suspicious patterns
        if candle.high == candle.low:
            return "poor"  # Flat candle

        # Check body to range ratio
        body = abs(candle.close - candle.open)
        range_hlc = candle.high - candle.low

        if range_hlc > 0:
            body_ratio = body / range_hlc
            if body_ratio < Decimal("0.1"):
                return "poor"  # Very small body (doji-like)

        return "good"

    @classmethod
    def is_consecutive(
        cls,
        candle1: CandleData,
        candle2: CandleData,
        expected_gap_seconds: int | None = None,
    ) -> bool:
        """Check if two candles are consecutive.

        Args:
            candle1: First candle
            candle2: Second candle
            expected_gap_seconds: Expected gap between candles

        Returns:
            True if consecutive.
        """
        # Check that candle2 starts when candle1 ends
        if expected_gap_seconds is not None:
            gap = (candle2.open_time - candle1.close_time).total_seconds()
            return abs(gap - expected_gap_seconds) < 60  # Allow 60s tolerance

        # Just check that candle2 starts after candle1
        return candle2.open_time >= candle1.close_time
