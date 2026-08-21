"""Price validation for risk."""

from __future__ import annotations

from decimal import Decimal


class PriceValidator:
    """Validates price-related risk parameters."""

    @staticmethod
    def validate_price_change(
        old_price: Decimal,
        new_price: Decimal,
        max_change_percent: Decimal = Decimal("50"),
    ) -> tuple[bool, str | None]:
        """Validate price change is reasonable."""
        if old_price <= 0:
            return True, None

        change_pct = abs(new_price - old_price) / old_price * 100
        if change_pct > max_change_percent:
            return False, f"Price change {change_pct}% exceeds max {max_change_percent}%"

        return True, None
