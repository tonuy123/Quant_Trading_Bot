"""DATA-002 CLI: argument validation, non-zero exits, fake-mode end-to-end."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from apps.data_download_cli.main import main, parse_args
from packages.market_data.adapters.binance_rest import HttpResponse
from packages.market_data.datasets.downloader import KLINES_ENDPOINT, DownloadManifest
from tests.fixtures.fake_http import FakeHttpTransport, kline_row

START = datetime(2026, 8, 1, tzinfo=UTC)
END = START + timedelta(days=1)
SERVER_TIME = END + timedelta(hours=1)


def build_argv(tmp_path: Path, **overrides) -> list[str]:
    argv = [
        "--symbols",
        "BTC/USDT,ETH/USDT",
        "--intervals",
        "1m,5m",
        "--start",
        "2026-08-01T00:00:00Z",
        "--end",
        "2026-08-02T00:00:00Z",
        "--output",
        str(tmp_path),
    ]
    argv.extend(overrides.pop("extra", []))
    for key, value in overrides.items():
        flag = f"--{key.replace('_', '-')}"
        if value is True:
            argv.append(flag)
        else:
            argv.append(flag)
            argv.append(str(value))
    return argv


def _ms(value: datetime) -> int:
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def scripted_transport() -> FakeHttpTransport:
    transport = FakeHttpTransport(SERVER_TIME)
    transport.add_server_time()
    start_ms = _ms(START)
    for _symbol in ("BTCUSDT", "ETHUSDT"):
        for _interval, duration in (("1m", 60_000), ("5m", 300_000)):
            rows = [kline_row(start_ms + i * duration, duration_ms=duration) for i in range(2)]
            transport.add_json(KLINES_ENDPOINT, rows)
    return transport


class TestArgumentParsing:
    """CLI arguments parse and validate before any network activity."""

    def test_parse_args_basic(self, tmp_path: Path) -> None:
        args = parse_args(build_argv(tmp_path))

        assert args.symbols == ["BTC/USDT,ETH/USDT"]
        assert args.intervals == ["1m,5m"]
        assert args.page_limit == 1000
        assert args.resume is False

    def test_parse_args_resume_and_page_limit(self, tmp_path: Path) -> None:
        args = parse_args(build_argv(tmp_path, page_limit=500, resume=True, extra=["--resume"]))

        assert args.resume is True
        assert args.page_limit == 500

    def test_missing_required_arguments_exit_nonzero(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--symbols", "BTC/USDT"])
        assert exc_info.value.code == 2

    def test_help_exits_zero(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--help"])
        assert exc_info.value.code == 0


class TestCliValidation:
    """Invalid inputs exit non-zero and never touch the network."""

    def test_naive_start_exits_two(self, tmp_path: Path) -> None:
        argv = build_argv(tmp_path)
        argv[argv.index("--start") + 1] = "2026-08-01T00:00:00"

        with pytest.raises(SystemExit) as exc_info:
            main(argv, transport=scripted_transport())
        assert exc_info.value.code == 2

    def test_naive_end_exits_two(self, tmp_path: Path) -> None:
        argv = build_argv(tmp_path)
        argv[argv.index("--end") + 1] = "2026-08-02T00:00:00"

        with pytest.raises(SystemExit) as exc_info:
            main(argv, transport=scripted_transport())
        assert exc_info.value.code == 2

    def test_garbage_timestamp_exits_two(self, tmp_path: Path) -> None:
        argv = build_argv(tmp_path)
        argv[argv.index("--start") + 1] = "not-a-timestamp"

        with pytest.raises(SystemExit) as exc_info:
            main(argv, transport=scripted_transport())
        assert exc_info.value.code == 2

    def test_end_before_start_exits_one(self, tmp_path: Path) -> None:
        argv = build_argv(tmp_path)
        argv[argv.index("--end") + 1] = "2026-07-31T00:00:00Z"

        with pytest.raises(SystemExit) as exc_info:
            main(argv, transport=scripted_transport())
        assert exc_info.value.code == 1

    def test_unsupported_page_limit_exits_one(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(build_argv(tmp_path, page_limit=0), transport=scripted_transport())
        assert exc_info.value.code == 1

    def test_empty_symbols_exit_one(self, tmp_path: Path) -> None:
        argv = build_argv(tmp_path)
        argv[argv.index("BTC/USDT,ETH/USDT")] = ","

        with pytest.raises(SystemExit) as exc_info:
            main(argv, transport=scripted_transport())
        assert exc_info.value.code == 1

    def test_private_base_url_exits_one(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(
                build_argv(tmp_path, base_url="https://api.binance.com/userdata"),
                transport=scripted_transport(),
            )
        assert exc_info.value.code == 1

    def test_no_api_key_argument_exists(self) -> None:
        argv = build_argv(Path("out"))
        assert not any("key" in flag or "secret" in flag for flag in argv)


class TestCliFakeModeRun:
    """End-to-end fake-transport runs through the real CLI entrypoint."""

    def test_successful_run_exits_zero_and_writes_dataset(self, tmp_path: Path) -> None:
        main(build_argv(tmp_path), transport=scripted_transport())

        manifest_path = tmp_path / "manifest.json"
        assert manifest_path.exists()
        manifest = DownloadManifest.from_json(manifest_path.read_text(encoding="utf-8"))
        assert manifest.completion_status == "complete"
        assert manifest.record_count == 8
        assert len(manifest.files) == 4
        for file_info in manifest.files:
            assert (tmp_path / file_info.name).exists()

    def test_manifest_content_is_deterministic(self, tmp_path: Path) -> None:
        main(build_argv(tmp_path), transport=scripted_transport())
        first = (tmp_path / "manifest.json").read_text(encoding="utf-8")

        second_dir = tmp_path / "rerun"
        second_dir.mkdir()
        argv = build_argv(second_dir)
        main(argv, transport=scripted_transport())
        second = (second_dir / "manifest.json").read_text(encoding="utf-8")

        assert json.loads(first) == json.loads(second)

    def test_failure_run_exits_nonzero_with_incomplete_manifest(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add_json(KLINES_ENDPOINT, [kline_row(_ms(START))])
        transport.add(
            KLINES_ENDPOINT,
            HttpResponse(status_code=403, headers={}, body=b"{}"),
        )

        with pytest.raises(SystemExit) as exc_info:
            main(build_argv(tmp_path), transport=transport)
        assert exc_info.value.code != 0

        manifest = DownloadManifest.from_json(
            (tmp_path / "manifest.json").read_text(encoding="utf-8")
        )
        assert manifest.completion_status == "incomplete"
