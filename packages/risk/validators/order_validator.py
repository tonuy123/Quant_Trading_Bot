"""Order risk validation."""

from __future__ import annotations

from decimal import Decimal

from packages.domain.value_objects import OrderIntent


class OrderRiskValidator:
    """Validates order risk parameters."""

    @staticmethod
    def validate_quantity(quantity: Decimal) -> tuple[bool, str | None]:
        """Validate order quantity."""
        if quantity <= 0:
            return False, "Quantity must be positive"
        if quantity < Decimal("0.0001"):
            return False, "Quantity too small"
        if quantity > Decimal("1000000"):
            return False, "Quantity too large"
        return True, None

    @staticmethod
    def validate_price(price: Decimal | None) -> tuple[bool, str | None]:
        """Validate order price."""
        if price is None:
            return True, None
        if price <= 0:
            return False, "Price must be positive"
        return True, None

    @staticmethod
    def validate_notional(
        notional: Decimal,
        min_notional: Decimal = Decimal("10"),
    ) -> tuple[bool, str | None]:
        """Validate order notional value."""
        if notional < min_notional:
            return False, f"Notional value {notional} below minimum {min_notional}"
        return True, None

    @staticmethod
    def validate_order_intent(intent: OrderIntent) -> list[str]:
        """Validate order intent completely."""
        errors = []

        valid, error = OrderRiskValidator.validate_quantity(intent.quantity.value)
        if not valid and error:
            errors.append(error)

        if intent.price:
            valid, error = OrderRiskValidator.validate_price(intent.price.value)
            if not valid and error:
                errors.append(error)

        notional = intent.notional_value
        if notional > 0:
            valid, error = OrderRiskValidator.validate_notional(notional)
            if not valid and error:
                errors.append(error)

        return errors
