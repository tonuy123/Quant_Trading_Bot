"""Risk management errors."""

from typing import Any

from packages.domain.errors.base import DomainError


class RiskError(DomainError):
    """Base class for risk-related errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        """Initialize risk error."""
        super().__init__(message, details)


class RiskLimitExceededError(RiskError):
    """Error raised when a risk limit is exceeded.

    This is not necessarily an error - it may just indicate
    that risk controls are working as intended.
    """

    def __init__(
        self,
        limit_type: str,
        current_value: str,
        limit_value: str,
        portfolio_id: str | None = None,
    ) -> None:
        """Initialize risk limit exceeded error."""
        message = f"Risk limit exceeded: {limit_type} = {current_value}, limit = {limit_value}"
        details = {
            "limit_type": limit_type,
            "current_value": current_value,
            "limit_value": limit_value,
            "portfolio_id": portfolio_id,
        }
        RiskError.__init__(self, message, details)
        self.limit_type = limit_type
        self.current_value = current_value
        self.limit_value = limit_value
        self.portfolio_id = portfolio_id


class RiskRejectedError(RiskError):
    """Error raised when a trade is rejected by risk management."""

    def __init__(
        self,
        reason: str,
        symbol: str | None = None,
        strategy_id: str | None = None,
        risk_metrics: dict[str, Any] | None = None,
    ) -> None:
        """Initialize risk rejected error."""
        message = f"Trade rejected by risk management: {reason}"
        if symbol:
            message = f"Trade for {symbol} rejected by risk management: {reason}"
        details = {
            "reason": reason,
            "symbol": symbol,
            "strategy_id": strategy_id,
            "risk_metrics": risk_metrics,
        }
        RiskError.__init__(self, message, details)
        self.reason = reason
        self.symbol = symbol
        self.strategy_id = strategy_id
        self.risk_metrics = risk_metrics


class MaxDrawdownExceededError(RiskError):
    """Error raised when maximum drawdown is exceeded."""

    def __init__(
        self, current_drawdown: str, max_drawdown: str, portfolio_id: str | None = None
    ) -> None:
        """Initialize max drawdown exceeded error."""
        message = f"Maximum drawdown exceeded: {current_drawdown}% (limit: {max_drawdown}%)"
        details = {
            "current_drawdown": current_drawdown,
            "max_drawdown": max_drawdown,
            "portfolio_id": portfolio_id,
        }
        RiskError.__init__(self, message, details)
        self.current_drawdown = current_drawdown
        self.max_drawdown = max_drawdown
        self.portfolio_id = portfolio_id


class MaxDailyLossExceededError(RiskError):
    """Error raised when maximum daily loss is exceeded."""

    def __init__(self, current_loss: str, max_loss: str, portfolio_id: str | None = None) -> None:
        """Initialize max daily loss exceeded error."""
        message = f"Maximum daily loss exceeded: {current_loss} (limit: {max_loss})"
        details = {
            "current_loss": current_loss,
            "max_loss": max_loss,
            "portfolio_id": portfolio_id,
        }
        RiskError.__init__(self, message, details)


class InsufficientCapitalError(RiskError):
    """Error raised when there's insufficient capital for the trade."""

    def __init__(self, required: str, available: str, currency: str) -> None:
        """Initialize insufficient capital error."""
        message = f"Insufficient capital: required {required} {currency}, available {available} {currency}"
        details = {"required": required, "available": available, "currency": currency}
        RiskError.__init__(self, message, details)
