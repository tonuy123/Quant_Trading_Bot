"""Backtest CLI - Command line interface for running backtests."""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime
from decimal import Decimal

from packages.backtesting import BacktestEngine
from packages.backtesting.engine import BacktestConfig, BacktestResult
from packages.config import get_settings
from packages.observability import get_logger
from packages.observability.logging import setup_logging as _setup_logging


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run backtests for trading strategies")

    parser.add_argument(
        "--strategy",
        type=str,
        required=True,
        help="Strategy ID to backtest",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="BTCUSDT",
        help="Trading symbol",
    )
    parser.add_argument(
        "--timeframe",
        type=str,
        default="1h",
        help="Candle timeframe",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=10000.0,
        help="Initial capital",
    )
    parser.add_argument(
        "--commission",
        type=float,
        default=0.001,
        help="Commission rate",
    )
    parser.add_argument(
        "--slippage",
        type=float,
        default=0.0005,
        help="Slippage rate",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file for results",
    )

    return parser.parse_args()


def print_results(result: BacktestResult) -> None:
    """Print backtest results."""
    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    print(f"Initial Capital: ${result.initial_capital:,.2f}")
    print(f"Final Capital:   ${result.final_capital:,.2f}")
    print(f"Total Return:    ${result.total_return:,.2f} ({result.total_return_pct:.2f}%)")
    print()
    print(f"Total Trades:    {result.total_trades}")
    print(f"Winning Trades:  {result.winning_trades}")
    print(f"Losing Trades:   {result.losing_trades}")
    print(f"Win Rate:        {result.win_rate:.2f}%")
    print()
    print(f"Average Win:     ${result.avg_win:,.2f}")
    print(f"Average Loss:    ${result.avg_loss:,.2f}")
    print(f"Largest Win:     ${result.largest_win:,.2f}")
    print(f"Largest Loss:    ${result.largest_loss:,.2f}")
    print()
    print(f"Max Drawdown:    {result.max_drawdown_pct:.2f}%")
    print("=" * 60)


async def run_backtest(args: argparse.Namespace) -> BacktestResult:
    """Run the backtest."""
    settings = get_settings()
    _setup_logging(level=settings.logging.level)

    logger = get_logger(__name__)
    logger.info("Starting backtest", strategy=args.strategy, symbol=args.symbol)

    # Parse dates
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_date = datetime.utcnow()

    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        end_date = datetime.utcnow()

    # Create config
    config = BacktestConfig(
        start_date=start_date,
        end_date=end_date,
        initial_capital=Decimal(str(args.initial_capital)),
        commission=Decimal(str(args.commission)),
        slippage=Decimal(str(args.slippage)),
    )

    # Create engine
    engine = BacktestEngine(config)
    logger.debug("Backtest engine created", engine=engine)

    # TODO: Load historical data
    # TODO: Create strategy instance
    # TODO: Run backtest

    logger.info("Backtest complete")

    # Return dummy result
    return BacktestResult(
        initial_capital=Decimal(str(args.initial_capital)),
        final_capital=Decimal(str(args.initial_capital)),
        total_return=Decimal("0"),
        total_return_pct=Decimal("0"),
        total_trades=0,
        winning_trades=0,
        losing_trades=0,
        win_rate=Decimal("0"),
        avg_win=Decimal("0"),
        avg_loss=Decimal("0"),
        largest_win=Decimal("0"),
        largest_loss=Decimal("0"),
        max_drawdown=Decimal("0"),
        max_drawdown_pct=Decimal("0"),
    )


def main() -> None:
    """Main entry point."""
    args = parse_args()

    try:
        result = asyncio.run(run_backtest(args))
        print_results(result)

        # Save to file if specified
        if args.output:
            import json

            with open(args.output, "w") as f:
                json.dump(
                    {
                        "initial_capital": str(result.initial_capital),
                        "final_capital": str(result.final_capital),
                        "total_return": str(result.total_return),
                        "total_return_pct": str(result.total_return_pct),
                        "total_trades": result.total_trades,
                        "winning_trades": result.winning_trades,
                        "losing_trades": result.losing_trades,
                        "win_rate": str(result.win_rate),
                        "max_drawdown_pct": str(result.max_drawdown_pct),
                    },
                    f,
                    indent=2,
                )
            print(f"\nResults saved to {args.output}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
