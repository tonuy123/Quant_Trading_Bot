"""PnL calculator utilities."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class PnLCalculator:
    """Calculates profit and loss for positions."""

    @staticmethod
    def calculate_unrealized_pnl(
        entry_price: Decimal,
        current_price: Decimal,
        quantity: Decimal,
        side: str,
        commission: Decimal = Decimal("0"),
    ) -> Decimal:
        """Calculate unrealized PnL.

        Args:
            entry_price: Entry price
            current_price: Current market price
            quantity: Position quantity
            side: LONG or SHORT
            commission: Commission paid on entry

        Returns:
            Unrealized PnL.
        """
        if side == "LONG":
            pnl = (current_price - entry_price) * quantity
        else:  # SHORT
            pnl = (entry_price - current_price) * quantity

        return pnl - commission

    @staticmethod
    def calculate_realized_pnl(
        entry_price: Decimal,
        exit_price: Decimal,
        quantity: Decimal,
        side: str,
        entry_commission: Decimal = Decimal("0"),
        exit_commission: Decimal = Decimal("0"),
    ) -> Decimal:
        """Calculate realized PnL on close.

        Args:
            entry_price: Entry price
            exit_price: Exit price
            quantity: Position quantity
            side: LONG or SHORT
            entry_commission: Commission on entry
            exit_commission: Commission on exit

        Returns:
            Realized PnL.
        """
        if side == "LONG":
            gross_pnl = (exit_price - entry_price) * quantity
        else:  # SHORT
            gross_pnl = (entry_price - exit_price) * quantity

        return gross_pnl - entry_commission - exit_commission

    @staticmethod
    def calculate_pnl_percentage(pnl: Decimal, entry_value: Decimal) -> Decimal:
        """Calculate PnL as percentage.

        Args:
            pnl: Profit or loss
            entry_value: Entry position value

        Returns:
            PnL percentage.
        """
        if entry_value == 0:
            return Decimal("0")
        return (pnl / entry_value) * 100

    @staticmethod
    def calculate_total_return(
        initial_equity: Decimal,
        final_equity: Decimal,
        deposits: Decimal = Decimal("0"),
        withdrawals: Decimal = Decimal("0"),
    ) -> Decimal:
        """Calculate total return.

        Args:
            initial_equity: Starting equity
            final_equity: Ending equity
            deposits: Total deposits
            withdrawals: Total withdrawals

        Returns:
            Total return amount.
        """
        return final_equity - initial_equity - deposits + withdrawals

    @staticmethod
    def calculate_return_percentage(
        initial_equity: Decimal,
        final_equity: Decimal,
    ) -> Decimal:
        """Calculate return as percentage.

        Args:
            initial_equity: Starting equity
            final_equity: Ending equity

        Returns:
            Return percentage.
        """
        if initial_equity == 0:
            return Decimal("0")
        return ((final_equity - initial_equity) / initial_equity) * 100
