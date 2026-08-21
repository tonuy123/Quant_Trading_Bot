"""Data download CLI - Historical public Binance Spot kline download (DATA-002).

Public market data only: the command never accepts credentials and rejects
private endpoint configuration. A failed download exits non-zero and leaves
an explicit ``incomplete`` manifest in the output directory.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime

from packages.market_data.adapters.binance_rest import (
    BINANCE_PUBLIC_REST_URL,
    PublicHttpTransport,
)
from packages.market_data.datasets.downloader import (
    DownloadManifest,
    HistoricalDownloader,
    HistoricalDownloadRequest,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Download historical public Binance Spot klines into a versioned dataset. "
            "Public market data only: no API key, no private endpoints, no trading."
        )
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Canonical BASE/QUOTE symbols, e.g. BTC/USDT ETH/USDT (commas also accepted)",
    )
    parser.add_argument(
        "--intervals",
        nargs="+",
        required=True,
        help="Kline intervals, e.g. 1m 5m 1h (commas also accepted)",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="UTC start (inclusive), ISO-8601 with offset, e.g. 2026-08-01T00:00:00Z",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="UTC end (exclusive), ISO-8601 with offset, e.g. 2026-08-08T00:00:00Z",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for the dataset files and manifest.json",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=1000,
        help="Maximum klines per request (1-1000)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume an incomplete download in the output directory",
    )
    parser.add_argument(
        "--base-url",
        default=BINANCE_PUBLIC_REST_URL,
        help="Public HTTPS base URL (private endpoints are rejected)",
    )
    return parser.parse_args(argv)


def _parse_utc(value: str, name: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{name} must be ISO-8601 with a UTC offset") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(f"{name} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _flatten(values: list[str]) -> list[str]:
    flat: list[str] = []
    for value in values:
        flat.extend(part.strip() for part in value.split(",") if part.strip())
    return flat


async def run_download(
    args: argparse.Namespace,
    transport: PublicHttpTransport | None,
    start: datetime,
    end: datetime,
) -> DownloadManifest:
    """Run one download and print a deterministic summary."""
    request = HistoricalDownloadRequest(
        symbols=tuple(_flatten(args.symbols)),
        intervals=tuple(_flatten(args.intervals)),
        start=start,
        end=end,
        output_dir=args.output,
        page_limit=args.page_limit,
        resume=args.resume,
        base_url=args.base_url,
    )
    downloader = HistoricalDownloader(transport=transport)
    manifest = await downloader.download(request)
    print(f"dataset_id:          {manifest.dataset_id}")
    print(f"dataset_version:     {manifest.dataset_version}")
    print(f"downloader_version:  {manifest.downloader_version}")
    print(f"source:              {manifest.source}")
    print(f"symbols:             {', '.join(manifest.symbols)}")
    print(f"intervals:           {', '.join(manifest.intervals)}")
    print(
        f"requested range:     {manifest.requested_start.isoformat()} .. "
        f"{manifest.requested_end.isoformat()}"
    )
    if manifest.actual_start is not None and manifest.actual_end is not None:
        print(
            f"actual range:        {manifest.actual_start.isoformat()} .. "
            f"{manifest.actual_end.isoformat()}"
        )
    print(f"record count:        {manifest.record_count}")
    for file_info in manifest.files:
        print(f"  file: {file_info.name} ({file_info.records} records)")
    print(f"completion status:   {manifest.completion_status}")
    if manifest.failure is not None:
        failure = manifest.failure
        print(
            f"failure:             {failure.error_type} "
            f"{failure.symbol} {failure.interval} {failure.endpoint} "
            f"{failure.range_start.isoformat()}..{failure.range_end.isoformat()} "
            f"attempts={failure.attempts}"
        )
    return manifest


def main(argv: list[str] | None = None, transport: PublicHttpTransport | None = None) -> None:
    """Main entry point (transport is injectable for fake/no-network mode)."""
    args = parse_args(argv)
    try:
        start = _parse_utc(args.start, "--start")
        end = _parse_utc(args.end, "--end")
    except argparse.ArgumentTypeError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
    try:
        manifest = asyncio.run(run_download(args, transport, start, end))
    except (ValueError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
    if manifest.completion_status != "complete":
        print("Download incomplete; see manifest for failure details.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
