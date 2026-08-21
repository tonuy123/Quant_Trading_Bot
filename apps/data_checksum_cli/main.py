"""Checksum verification CLI (DATA-003).

Verifies a dataset directory against a local checksum file (flat JSON object
mapping file name to a 64-character lowercase SHA-256 hex digest).  Read-only,
no network, no credentials.  Exit codes: 0 = all verified, 1 = mismatch /
missing / read failure / invalid path, 2 = invalid arguments or malformed
checksum file.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from packages.market_data.datasets.checksum import (
    DatasetChecksumReport,
    verify_dataset_directory,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Verify dataset file checksums against a local checksum file. "
            "Read-only: no network, no credentials, no data mutation."
        )
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset directory containing the files to verify",
    )
    parser.add_argument(
        "--checksums",
        required=True,
        help="JSON file mapping file names to 64-char lowercase SHA-256 hex digests",
    )
    return parser.parse_args(argv)


def _load_checksums(path: Path) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"cannot read checksum file: {error}") from error
    try:
        record = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError(f"checksum file is not valid JSON: {error}") from error
    if not isinstance(record, dict):
        raise ValueError("checksum file must be a JSON object mapping names to digests")
    checksums: dict[str, str] = {}
    for name, value in record.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError("checksum file entries must map file names to digest strings")
        checksums[name] = value
    if not checksums:
        raise ValueError("checksum file must contain at least one entry")
    return checksums


def _print_report(report: DatasetChecksumReport) -> None:
    print(f"overall status:     {report.status}")
    print(f"bytes read:         {report.bytes_read}")
    for file_report in report.files:
        actual = file_report.actual or "-"
        error = f" ({file_report.error})" if file_report.error else ""
        print(
            f"  {file_report.status:>12} {file_report.name} "
            f"expected={file_report.expected or '-'} actual={actual} "
            f"bytes={file_report.bytes_read}{error}"
        )
    if report.unexpected:
        print(f"unexpected files:   {', '.join(report.unexpected)}")


def main(argv: list[str] | None = None) -> None:
    """Entry point (exit codes documented in the module docstring)."""
    args = parse_args(argv)
    try:
        dataset_dir = Path(args.dataset)
        checksums = _load_checksums(Path(args.checksums))
        report = verify_dataset_directory(dataset_dir, checksums)
    except (ValueError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
    _print_report(report)
    if report.status == "verified":
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
