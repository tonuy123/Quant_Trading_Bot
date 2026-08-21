"""DATA-002 historical downloader: validation, pagination, retry, atomic I/O."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from packages.market_data.adapters.binance_rest import HttpResponse
from packages.market_data.datasets.downloader import (
    DATASET_DOWNLOAD_VERSION,
    DOWNLOADER_VERSION,
    KLINES_ENDPOINT,
    MAX_REQUEST_ATTEMPTS,
    DownloadFailure,
    DownloadManifest,
    HistoricalDownloader,
    HistoricalDownloadRequest,
)
from packages.market_data.services.rate_limit import BinanceRateLimitCoordinator
from tests.fixtures.fake_http import (
    FakeHttpTransport,
    kline_row,
    retry_after_response,
)

START = datetime(2026, 8, 1, tzinfo=UTC)
DURATION_MS = 60_000
INTERVAL = "1m"
END = START + timedelta(days=1)
SERVER_TIME = END + timedelta(hours=1)


class FakeClock:
    """Deterministic UTC clock whose value advances only when told to."""

    def __init__(self, start: datetime) -> None:
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


def build_request(**overrides) -> HistoricalDownloadRequest:
    values = {
        "symbols": ("BTC/USDT",),
        "intervals": (INTERVAL,),
        "start": START,
        "end": END,
        "output_dir": "out",
    }
    values.update(overrides)
    return HistoricalDownloadRequest(**values)


def make_downloader(transport: FakeHttpTransport) -> tuple[HistoricalDownloader, FakeClock]:
    clock = FakeClock(SERVER_TIME)
    limiter = BinanceRateLimitCoordinator(clock=clock)

    async def advance_sleep(seconds: float) -> None:
        clock.advance(seconds)

    downloader = HistoricalDownloader(
        transport=transport,
        limiter=limiter,
        clock=clock,
        sleeper=advance_sleep,
    )
    return downloader, clock


def script_full_klines(
    transport: FakeHttpTransport,
    *,
    page_limit: int = 1000,
    rows: list[list] | None = None,
) -> None:
    transport.add_server_time()
    if rows is None:
        rows = [kline_row(_to_ms(START) + i * DURATION_MS) for i in range(10)]
    for offset in range(0, len(rows), page_limit):
        transport.add_json(KLINES_ENDPOINT, rows[offset : offset + page_limit])


def _to_ms(value: datetime) -> int:
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def _read_lines(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


async def run_download(
    tmp_path: Path, transport: FakeHttpTransport, **overrides
) -> DownloadManifest:
    request = build_request(output_dir=str(tmp_path), **overrides)
    downloader, _ = make_downloader(transport)
    return await downloader.download(request)


class TestRequestValidation:
    """Reject invalid inputs before any network activity."""

    def test_empty_symbols_rejected(self) -> None:
        with pytest.raises(ValueError, match="symbols must be non-empty"):
            build_request(symbols=())

    def test_invalid_symbol_without_slash_rejected(self) -> None:
        with pytest.raises(ValueError, match="canonical BASE/QUOTE"):
            build_request(symbols=("BTCUSDT",))

    def test_empty_intervals_rejected(self) -> None:
        with pytest.raises(ValueError, match="intervals must be non-empty"):
            build_request(intervals=())

    def test_unsupported_interval_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported interval"):
            build_request(intervals=("7m",))

    def test_naive_start_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            build_request(start=datetime(2026, 8, 1))

    def test_naive_end_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            build_request(end=datetime(2026, 8, 2))

    def test_end_before_start_rejected(self) -> None:
        with pytest.raises(ValueError, match="end must follow start"):
            build_request(end=START - timedelta(minutes=1))

    @pytest.mark.parametrize("page_limit", [0, 1001, True])
    def test_unsupported_page_limit_rejected(self, page_limit: object) -> None:
        with pytest.raises(ValueError, match="page_limit"):
            build_request(page_limit=page_limit)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "base_url",
        [
            "http://api.binance.com",
            "https://api.binance.com/apikey=secret",
            "https://api.binance.com/listenkey=secret",
            "https://api.binance.com/userdata",
        ],
    )
    def test_private_or_insecure_base_url_rejected(self, base_url: str) -> None:
        with pytest.raises(ValueError):
            build_request(base_url=base_url)

    def test_output_path_that_is_a_file_rejected(self, tmp_path: Path) -> None:
        file_path = tmp_path / "not-a-dir"
        file_path.write_text("x", encoding="utf-8")

        with pytest.raises(ValueError, match="not a directory"):
            build_request(output_dir=str(file_path))


class TestUtcNormalization:
    """Aware timestamps normalize to UTC before identity and requests."""

    async def test_offset_aware_timestamps_normalize_to_utc(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)
        request = HistoricalDownloadRequest(
            symbols=("BTC/USDT",),
            intervals=(INTERVAL,),
            start=datetime(2026, 8, 1, 7, 0, tzinfo=UTC) - timedelta(hours=7),
            end=datetime(2026, 8, 2, 7, 0, tzinfo=UTC) - timedelta(hours=7),
            output_dir=str(tmp_path),
        )

        manifest = await make_downloader(transport)[0].download(request)

        assert manifest.requested_start == START
        assert manifest.requested_end == END
        assert manifest.requested_start.tzinfo is UTC

    async def test_offset_variants_produce_same_dataset_id(self, tmp_path: Path) -> None:
        request_utc = HistoricalDownloadRequest(
            symbols=("BTC/USDT",),
            intervals=(INTERVAL,),
            start=START,
            end=END,
            output_dir=str(tmp_path),
        )
        request_offset = HistoricalDownloadRequest(
            symbols=("BTC/USDT",),
            intervals=(INTERVAL,),
            start=datetime(2026, 8, 1, 7, 0, tzinfo=UTC) - timedelta(hours=7),
            end=datetime(2026, 8, 2, 7, 0, tzinfo=UTC) - timedelta(hours=7),
            output_dir=str(tmp_path),
        )
        assert request_utc.start == request_offset.start == START
        assert request_utc.end == request_offset.end == END


class TestHalfOpenRange:
    """[start, end): start inclusive, end exclusive, close time exclusive."""

    async def test_range_boundaries(self, tmp_path: Path) -> None:
        rows = [
            kline_row(_to_ms(START) - DURATION_MS),
            kline_row(_to_ms(START)),
            kline_row(_to_ms(START) + DURATION_MS),
            kline_row(_to_ms(END) - DURATION_MS),
            kline_row(_to_ms(END)),
            kline_row(_to_ms(END) + DURATION_MS),
        ]
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport, rows=rows)

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "complete"
        file_path = tmp_path / "BTC-USDT-1m.jsonl"
        records = _read_lines(file_path)
        assert [record["payload"][0] for record in records] == [
            _to_ms(START),
            _to_ms(START) + DURATION_MS,
            _to_ms(END) - DURATION_MS,
        ]


class TestClosedCandleFiltering:
    """In-progress candles (close time after server time) are excluded."""

    async def test_in_progress_candle_excluded(self, tmp_path: Path) -> None:
        server_time = START + timedelta(minutes=5)
        rows = [kline_row(_to_ms(START) + i * DURATION_MS) for i in range(8)]
        transport = FakeHttpTransport(server_time)
        script_full_klines(transport, rows=rows)

        manifest = await run_download(tmp_path, transport)

        records = _read_lines(tmp_path / "BTC-USDT-1m.jsonl")
        assert manifest.record_count == 5
        assert [record["payload"][0] for record in records] == [
            _to_ms(START) + i * DURATION_MS for i in range(5)
        ]


class TestPagination:
    """Pagination is deterministic and stops at the last page."""

    async def test_multiple_pages(self, tmp_path: Path) -> None:
        rows = [kline_row(_to_ms(START) + i * DURATION_MS) for i in range(5)]
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport, page_limit=2, rows=rows)

        manifest = await run_download(tmp_path, transport, page_limit=2)

        kline_requests = [r for r in transport.requests if "/api/v3/klines" in r[0]]
        assert len(kline_requests) == 3
        assert manifest.record_count == 5
        records = _read_lines(tmp_path / "BTC-USDT-1m.jsonl")
        assert [record["payload"][0] for record in records] == [
            _to_ms(START) + i * DURATION_MS for i in range(5)
        ]


class TestMultipleSymbolsAndIntervals:
    """Records never mix across symbols or intervals."""

    async def test_two_symbols_two_intervals(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        start_ms = _to_ms(START)
        for _symbol, _suffix in (("BTC/USDT", "BTCUSDT"), ("ETH/USDT", "ETHUSDT")):
            for _interval in ("1m", "5m"):
                duration = 60_000 if _interval == "1m" else 300_000
                rows = [kline_row(start_ms + i * duration, duration_ms=duration) for i in range(3)]
                transport.add_json(
                    KLINES_ENDPOINT,
                    rows,
                )

        manifest = await run_download(
            tmp_path, transport, symbols=("eth/usdt", "btc/usdt"), intervals=("5m", "1m")
        )

        assert manifest.symbols == ("BTC/USDT", "ETH/USDT")
        assert manifest.intervals == ("1m", "5m")
        assert len(manifest.files) == 4
        btc_1m = _read_lines(tmp_path / "BTC-USDT-1m.jsonl")
        eth_5m = _read_lines(tmp_path / "ETH-USDT-5m.jsonl")
        assert len(btc_1m) == 3
        assert all(
            record["symbol"] == "BTC/USDT" and record["interval"] == "1m" for record in btc_1m
        )
        assert len(eth_5m) == 3
        assert all(
            record["symbol"] == "ETH/USDT" and record["interval"] == "5m" for record in eth_5m
        )


class TestDeterministicOrdering:
    """Records are ordered by open time regardless of payload order."""

    async def test_records_sorted_by_open_time(self, tmp_path: Path) -> None:
        start_ms = _to_ms(START)
        rows = [
            kline_row(start_ms + 2 * DURATION_MS),
            kline_row(start_ms),
            kline_row(start_ms + DURATION_MS),
        ]
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport, rows=rows)

        await run_download(tmp_path, transport)

        records = _read_lines(tmp_path / "BTC-USDT-1m.jsonl")
        assert [record["payload"][0] for record in records] == [
            start_ms,
            start_ms + DURATION_MS,
            start_ms + 2 * DURATION_MS,
        ]


class TestRateLimitAndRetry:
    """429 honors Retry-After; 418 cooldown is bounded; 403/5xx never retried."""

    async def test_429_retry_after_honored(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add(KLINES_ENDPOINT, retry_after_response(429, "30"))
        transport.add_json(KLINES_ENDPOINT, [kline_row(_to_ms(START))])

        manifest = await run_download(tmp_path, transport)

        kline_requests = [r for r in transport.requests if "/api/v3/klines" in r[0]]
        assert len(kline_requests) == 2
        assert manifest.completion_status == "complete"
        assert manifest.record_count == 1

    async def test_418_cooldown_bounded(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add(KLINES_ENDPOINT, retry_after_response(418, "120"))
        transport.add_json(KLINES_ENDPOINT, [kline_row(_to_ms(START))])

        manifest = await run_download(tmp_path, transport)

        kline_requests = [r for r in transport.requests if "/api/v3/klines" in r[0]]
        assert len(kline_requests) == 2
        assert manifest.completion_status == "complete"

    async def test_429_exhaustion_fails_with_rate_limited(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        for _ in range(MAX_REQUEST_ATTEMPTS):
            transport.add(KLINES_ENDPOINT, retry_after_response(429, "30"))

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "rate_limited"
        assert manifest.failure.attempts == MAX_REQUEST_ATTEMPTS

    async def test_403_never_retried(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add(
            KLINES_ENDPOINT,
            HttpResponse(status_code=403, headers={}, body=b"{}"),
        )

        manifest = await run_download(tmp_path, transport)

        kline_requests = [r for r in transport.requests if "/api/v3/klines" in r[0]]
        assert len(kline_requests) == 1
        assert manifest.failure is not None
        assert manifest.failure.error_type == "forbidden"

    async def test_5xx_never_retried(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add(
            KLINES_ENDPOINT,
            HttpResponse(status_code=500, headers={}, body=b"{}"),
        )

        manifest = await run_download(tmp_path, transport)

        kline_requests = [r for r in transport.requests if "/api/v3/klines" in r[0]]
        assert len(kline_requests) == 1
        assert manifest.failure is not None
        assert manifest.failure.error_type == "server_error"

    async def test_timeout_failure_bounded(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.fail_after(2, TimeoutError("network down"))

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "network_timeout"
        assert manifest.failure.attempts == MAX_REQUEST_ATTEMPTS


class TestMalformedAndEmptyResponses:
    """Malformed bodies fail explicitly; empty pages end the range normally."""

    @pytest.mark.parametrize(
        "body",
        [b"not json", b"{}", b'{"ok": true}', b"[1, 2, 3]"],
    )
    async def test_malformed_response_fails(self, tmp_path: Path, body: bytes) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add(KLINES_ENDPOINT, HttpResponse(status_code=200, headers={}, body=body))

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "malformed_response"

    @pytest.mark.parametrize("close_delta", [-1, 1, 86_400_000])
    async def test_raw_close_boundary_mismatch_fails(
        self, tmp_path: Path, close_delta: int
    ) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        row = kline_row(_to_ms(START))
        row[6] += close_delta
        transport.add_json(KLINES_ENDPOINT, [row])

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "malformed_response"
        assert manifest.record_count == 0
        assert not (tmp_path / "BTC-USDT-1m.jsonl").exists()

    async def test_kline_with_extra_field_fails(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        row = kline_row(_to_ms(START))
        row.append("unexpected")
        transport.add_json(KLINES_ENDPOINT, [row])

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "malformed_response"

    async def test_empty_response_ends_download_cleanly(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add_json(KLINES_ENDPOINT, [])

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "complete"
        assert manifest.record_count == 0
        assert not (tmp_path / "BTC-USDT-1m.jsonl").exists()

    async def test_server_time_failure_marks_incomplete(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_json("/api/v3/time", {"serverTime": "not-an-int"}, status=200)

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "server_time_failure"


class TestAtomicOutputAndResume:
    """Atomic writes, no duplicates, deterministic reruns."""

    async def test_no_temp_files_remain(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)

        await run_download(tmp_path, transport)

        assert set(tmp_path.iterdir()) == {
            tmp_path / "BTC-USDT-1m.jsonl",
            tmp_path / "manifest.json",
        }
        assert not list(tmp_path.glob("*.tmp"))

    async def test_duplicate_rows_in_page_are_deduplicated(self, tmp_path: Path) -> None:
        start_ms = _to_ms(START)
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(
            transport,
            rows=[kline_row(start_ms), kline_row(start_ms), kline_row(start_ms + DURATION_MS)],
        )

        manifest = await run_download(tmp_path, transport)

        assert manifest.record_count == 2
        records = _read_lines(tmp_path / "BTC-USDT-1m.jsonl")
        assert len(records) == 2

    async def test_interrupted_download_leaves_incomplete_manifest(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add_json(
            KLINES_ENDPOINT,
            [kline_row(_to_ms(START) + i * DURATION_MS) for i in range(3)],
        )
        transport.add(
            KLINES_ENDPOINT,
            HttpResponse(status_code=403, headers={}, body=b"{}"),
        )

        manifest = await run_download(tmp_path, transport, symbols=("btc/usdt", "eth/usdt"))

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "forbidden"
        assert manifest.failure.symbol == "ETH/USDT"
        assert manifest.failure.interval == "1m"
        assert manifest.failure.endpoint == "/api/v3/klines"
        assert len(manifest.files) == 1
        assert manifest.files[0].name == "BTC-USDT-1m.jsonl"
        assert manifest.files[0].records == 3
        assert not list(tmp_path.glob("*.tmp"))

    async def test_resume_does_not_duplicate(self, tmp_path: Path) -> None:
        start_ms = _to_ms(START)
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add_json(
            KLINES_ENDPOINT,
            [kline_row(start_ms + i * DURATION_MS) for i in range(3)],
        )
        transport.add(
            KLINES_ENDPOINT,
            HttpResponse(status_code=500, headers={}, body=b"{}"),
        )

        manifest = await run_download(tmp_path, transport, symbols=("btc/usdt", "eth/usdt"))
        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.symbol == "ETH/USDT"

        resume_transport = FakeHttpTransport(SERVER_TIME)
        resume_transport.add_server_time()
        resume_transport.add_json(
            KLINES_ENDPOINT,
            [kline_row(start_ms + i * DURATION_MS) for i in range(3, 5)],
        )
        resume_transport.add_json(
            KLINES_ENDPOINT,
            [kline_row(start_ms + i * DURATION_MS) for i in range(3)],
        )

        resumed = await run_download(
            tmp_path, resume_transport, symbols=("btc/usdt", "eth/usdt"), resume=True
        )

        assert resumed.completion_status == "complete"
        btc_records = _read_lines(tmp_path / "BTC-USDT-1m.jsonl")
        eth_records = _read_lines(tmp_path / "ETH-USDT-1m.jsonl")
        btc_open_times = [record["payload"][0] for record in btc_records]
        eth_open_times = [record["payload"][0] for record in eth_records]
        assert len(btc_open_times) == len(set(btc_open_times)) == 5
        assert len(eth_open_times) == len(set(eth_open_times)) == 3
        assert resumed.record_count == 8

    async def test_already_complete_rerun_is_deterministic_noop(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)
        await run_download(tmp_path, transport)

        noop_transport = FakeHttpTransport(SERVER_TIME)
        manifest = await run_download(tmp_path, noop_transport)

        assert manifest.completion_status == "complete"
        assert manifest.record_count == 10
        assert noop_transport.requests == []

    async def test_completed_dataset_from_stale_producer_is_not_reused(
        self, tmp_path: Path
    ) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)
        await run_download(tmp_path, transport)
        manifest_path = tmp_path / "manifest.json"
        manifest_record = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_record["dataset_version"] = "1.0.0"
        manifest_record["downloader_version"] = "1.0.0"
        manifest_path.write_text(json.dumps(manifest_record), encoding="utf-8")

        noop_transport = FakeHttpTransport(SERVER_TIME)
        with pytest.raises(ValueError, match="incompatible version"):
            await run_download(tmp_path, noop_transport)

        assert noop_transport.requests == []

    async def test_incomplete_dataset_from_stale_producer_cannot_resume(
        self, tmp_path: Path
    ) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.fail_with_status(2, 500)
        manifest = await run_download(tmp_path, transport)
        assert manifest.completion_status == "incomplete"

        manifest_path = tmp_path / "manifest.json"
        manifest_record = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_record["dataset_version"] = "1.0.0"
        manifest_record["downloader_version"] = "1.0.0"
        manifest_path.write_text(json.dumps(manifest_record), encoding="utf-8")

        noop_transport = FakeHttpTransport(SERVER_TIME)
        with pytest.raises(ValueError, match="incompatible version"):
            await run_download(tmp_path, noop_transport, resume=True)

        assert noop_transport.requests == []

    async def test_resume_rejects_existing_file_with_invalid_close_boundary(
        self, tmp_path: Path
    ) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.add_json(KLINES_ENDPOINT, [kline_row(_to_ms(START))])
        transport.add(
            KLINES_ENDPOINT,
            HttpResponse(status_code=500, headers={}, body=b"{}"),
        )
        manifest = await run_download(tmp_path, transport, symbols=("BTC/USDT", "ETH/USDT"))
        assert manifest.completion_status == "incomplete"

        file_path = tmp_path / "BTC-USDT-1m.jsonl"
        records = _read_lines(file_path)
        records[0]["payload"][6] += 1
        file_path.write_text(
            "\n".join(json.dumps(record) for record in records) + "\n",
            encoding="utf-8",
        )

        resume_transport = FakeHttpTransport(SERVER_TIME)
        resume_transport.add_server_time()
        with pytest.raises(ValueError, match="malformed at line 1"):
            await run_download(
                tmp_path,
                resume_transport,
                symbols=("BTC/USDT", "ETH/USDT"),
                resume=True,
            )

        assert len(resume_transport.requests) == 1
        assert "/api/v3/time" in resume_transport.requests[0][0]

    async def test_completed_dataset_never_silently_overwritten(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)
        await run_download(tmp_path, transport)

        other_transport = FakeHttpTransport(SERVER_TIME)
        other_transport.add_server_time()
        other_transport.add_json(KLINES_ENDPOINT, [kline_row(_to_ms(START))])
        with pytest.raises(ValueError, match="different completed dataset"):
            await run_download(tmp_path, other_transport, symbols=("ETH/USDT",))

    async def test_resume_rejects_different_identity(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        transport.fail_with_status(2, 500)
        manifest = await run_download(tmp_path, transport)
        assert manifest.completion_status == "incomplete"

        with pytest.raises(ValueError, match="different identity"):
            await run_download(tmp_path, transport, symbols=("ETH/USDT",), resume=True)


class TestManifestContent:
    """Manifest is complete, deterministic, and round-trips exactly."""

    async def test_successful_manifest_generation(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "complete"
        assert manifest.record_count == 10
        assert manifest.actual_start == START
        assert manifest.actual_end == START + timedelta(minutes=10)
        assert manifest.source == "binance_public_rest"
        assert manifest.exchange == "binance"
        assert manifest.market_type == "spot"
        assert manifest.schema_version == 1
        assert manifest.dataset_version == "1.0.1"
        assert manifest.downloader_version == "1.0.1"
        assert manifest.dataset_version == DATASET_DOWNLOAD_VERSION
        assert manifest.downloader_version == DOWNLOADER_VERSION
        assert manifest.failure is None
        assert len(manifest.files) == 1
        file_info = manifest.files[0]
        assert file_info.name == "BTC-USDT-1m.jsonl"
        assert file_info.records == 10
        assert "checksum" not in manifest.to_record()

    async def test_manifest_round_trip_exact(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)
        manifest = await run_download(tmp_path, transport)

        restored = DownloadManifest.from_json(manifest.to_json())

        assert restored.to_json() == manifest.to_json()
        assert restored.dataset_id == manifest.dataset_id
        assert restored.failure is None

    def test_manifest_rejects_missing_field(self) -> None:
        record = {
            "dataset_id": "x",
            "dataset_version": "1.0.0",
            "downloader_version": "1.0.0",
            "schema_version": 1,
            "source": "binance_public_rest",
            "exchange": "binance",
            "market_type": "spot",
            "symbols": ["BTC/USDT"],
            "intervals": ["1m"],
            "requested_start": START.isoformat(),
            "requested_end": END.isoformat(),
            "actual_start": None,
            "actual_end": None,
            "record_count": 0,
            "files": [],
            "completion_status": "complete",
            "failure": None,
            "page_limit": 1000,
            "resume": False,
            "server_time": None,
        }

        with pytest.raises(ValueError, match="missing field"):
            DownloadManifest.from_record({k: v for k, v in record.items() if k != "files"})

    def test_failure_round_trip(self) -> None:
        failure = DownloadFailure(
            symbol="BTC/USDT",
            interval="1m",
            range_start=START,
            range_end=END,
            endpoint="/api/v3/klines",
            error_type="pagination_failure",
            message="no progress",
            attempts=1,
        )
        manifest = DownloadManifest(
            dataset_id="id",
            dataset_version="1.0.0",
            downloader_version="1.0.0",
            schema_version=1,
            source="binance_public_rest",
            exchange="binance",
            market_type="spot",
            symbols=("BTC/USDT",),
            intervals=("1m",),
            requested_start=START,
            requested_end=END,
            actual_start=None,
            actual_end=None,
            record_count=0,
            files=(),
            completion_status="incomplete",
            failure=failure,
            page_limit=1000,
            resume=False,
            server_time=None,
        )

        restored = DownloadManifest.from_json(manifest.to_json())

        assert restored.failure == failure


class TestPaginationFailure:
    """A page that never advances fails explicitly instead of looping."""

    async def test_no_progress_page_fails(self, tmp_path: Path) -> None:
        start_ms = _to_ms(START)
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(
            transport,
            rows=[kline_row(start_ms - DURATION_MS) for _ in range(3)],
        )

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "pagination_failure"


class TestLocalWriteFailures:
    """Local output write failures produce an incomplete manifest, never a leak."""

    def _fail_jsonl_replace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original_replace = os.replace

        def failing_replace(src: str, dst: str) -> None:
            if str(dst).endswith(".jsonl"):
                raise OSError("disk full")
            original_replace(src, dst)

        monkeypatch.setattr(os, "replace", failing_replace)

    async def test_write_failure_marks_incomplete_and_cleans_tmp(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)
        self._fail_jsonl_replace(monkeypatch)

        manifest = await run_download(tmp_path, transport)

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "write_failure"
        assert manifest.failure.endpoint == "local_output"
        assert not (tmp_path / "BTC-USDT-1m.jsonl").exists()
        assert not list(tmp_path.glob("*.tmp"))

    async def test_write_failure_keeps_prior_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        transport.add_server_time()
        start_ms = _to_ms(START)
        rows_1m = [kline_row(start_ms + i * DURATION_MS) for i in range(3)]
        rows_5m = [kline_row(start_ms + i * 300_000, duration_ms=300_000) for i in range(3)]
        for rows in (rows_1m, rows_5m, rows_1m):
            transport.add_json(KLINES_ENDPOINT, rows)
        original_replace = os.replace

        def failing_replace(src: str, dst: str) -> None:
            if "ETH-USDT-1m.jsonl" in str(dst):
                raise OSError("permission denied")
            original_replace(src, dst)

        monkeypatch.setattr(os, "replace", failing_replace)

        manifest = await run_download(
            tmp_path,
            transport,
            symbols=("BTC/USDT", "ETH/USDT"),
            intervals=("1m", "5m"),
        )

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "write_failure"
        assert manifest.failure.symbol == "ETH/USDT"
        assert [file_info.name for file_info in manifest.files] == [
            "BTC-USDT-1m.jsonl",
            "BTC-USDT-5m.jsonl",
        ]
        assert (tmp_path / "BTC-USDT-1m.jsonl").exists()
        assert not (tmp_path / "ETH-USDT-1m.jsonl").exists()
        assert not list(tmp_path.glob("*.tmp"))

    async def test_manifest_write_failure_propagates_original_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        transport = FakeHttpTransport(SERVER_TIME)
        script_full_klines(transport)
        original_replace = os.replace

        def failing_replace(src: str, dst: str) -> None:
            if str(dst).endswith("manifest.json"):
                raise OSError("permission denied")
            original_replace(src, dst)

        monkeypatch.setattr(os, "replace", failing_replace)

        with pytest.raises(OSError, match="permission denied"):
            await run_download(tmp_path, transport)

        assert not list(tmp_path.glob("*.tmp"))


class TestMonthlyCalendarSemantics:
    """1M must use UTC calendar-month boundaries, never a fixed 30 days."""

    JAN_1 = datetime(2024, 1, 1, tzinfo=UTC)
    FEB_1 = datetime(2024, 2, 1, tzinfo=UTC)
    MAR_1 = datetime(2024, 3, 1, tzinfo=UTC)
    APR_1 = datetime(2024, 4, 1, tzinfo=UTC)
    SERVER_2024 = datetime(2024, 3, 2, tzinfo=UTC)

    @staticmethod
    def _monthly_row(open_dt: datetime) -> list:
        if open_dt.month == 12:
            next_open = open_dt.replace(year=open_dt.year + 1, month=1, day=1)
        else:
            next_open = open_dt.replace(month=open_dt.month + 1, day=1)
        return kline_row(_to_ms(open_dt), close_ms=_to_ms(next_open) - 1)

    async def test_february_2024_leap_candle_not_dropped(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(self.SERVER_2024)
        transport.add_server_time()
        transport.add_json(KLINES_ENDPOINT, [self._monthly_row(self.FEB_1)])

        manifest = await run_download(
            tmp_path,
            transport,
            intervals=("1M",),
            start=self.FEB_1,
            end=self.MAR_1,
        )

        assert manifest.record_count == 1
        lines = _read_lines(tmp_path / "BTC-USDT-1M.jsonl")
        assert len(lines) == 1
        assert lines[0]["payload"][0] == _to_ms(self.FEB_1)
        assert lines[0]["close_time"] == self.MAR_1.isoformat()

    async def test_malformed_monthly_raw_close_is_rejected(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(self.SERVER_2024)
        transport.add_server_time()
        row = self._monthly_row(self.FEB_1)
        row[6] -= 86_400_000
        transport.add_json(KLINES_ENDPOINT, [row])

        manifest = await run_download(
            tmp_path,
            transport,
            intervals=("1M",),
            start=self.FEB_1,
            end=self.MAR_1,
        )

        assert manifest.completion_status == "incomplete"
        assert manifest.failure is not None
        assert manifest.failure.error_type == "malformed_response"
        assert manifest.record_count == 0

    async def test_pagination_reaches_march_exactly_once(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(self.SERVER_2024)
        transport.add_server_time()
        for row in (self._monthly_row(self.JAN_1), self._monthly_row(self.FEB_1)):
            transport.add_json(KLINES_ENDPOINT, [row])

        manifest = await run_download(
            tmp_path,
            transport,
            intervals=("1M",),
            start=self.JAN_1,
            end=self.MAR_1,
            page_limit=1,
        )

        assert manifest.record_count == 2
        kline_requests = [params for url, params in transport.requests if "/api/v3/klines" in url]
        assert len(kline_requests) == 2
        assert kline_requests[0]["startTime"] == str(_to_ms(self.JAN_1))
        assert kline_requests[1]["startTime"] == str(_to_ms(self.FEB_1))
        lines = _read_lines(tmp_path / "BTC-USDT-1M.jsonl")
        assert [line["payload"][0] for line in lines] == [
            _to_ms(self.JAN_1),
            _to_ms(self.FEB_1),
        ]

    async def test_march_candle_excluded_by_half_open_end(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(self.SERVER_2024)
        transport.add_server_time()
        transport.add_json(
            KLINES_ENDPOINT,
            [
                self._monthly_row(self.FEB_1),
                self._monthly_row(self.MAR_1),
            ],
        )

        manifest = await run_download(
            tmp_path,
            transport,
            intervals=("1M",),
            start=self.FEB_1,
            end=self.MAR_1,
        )

        assert manifest.record_count == 1
        lines = _read_lines(tmp_path / "BTC-USDT-1M.jsonl")
        assert [line["payload"][0] for line in lines] == [_to_ms(self.FEB_1)]

    async def test_resume_does_not_duplicate_monthly_candles(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(self.SERVER_2024)
        transport.add_server_time()
        transport.add_json(
            KLINES_ENDPOINT,
            [self._monthly_row(self.JAN_1), self._monthly_row(self.FEB_1)],
        )
        first = await run_download(
            tmp_path,
            transport,
            intervals=("1M",),
            start=self.JAN_1,
            end=self.MAR_1,
        )
        assert first.record_count == 2

        resumed_transport = FakeHttpTransport(self.SERVER_2024)
        resumed_transport.add_server_time()
        second = await run_download(
            tmp_path,
            resumed_transport,
            intervals=("1M",),
            start=self.JAN_1,
            end=self.MAR_1,
            resume=True,
        )

        assert second.record_count == 2
        kline_requests = [
            params for url, params in resumed_transport.requests if "/api/v3/klines" in url
        ]
        assert kline_requests == []
        lines = _read_lines(tmp_path / "BTC-USDT-1M.jsonl")
        assert len(lines) == 2

    async def test_manifest_range_end_is_calendar_boundary(self, tmp_path: Path) -> None:
        transport = FakeHttpTransport(self.SERVER_2024)
        transport.add_server_time()
        transport.add_json(KLINES_ENDPOINT, [self._monthly_row(self.FEB_1)])

        manifest = await run_download(
            tmp_path,
            transport,
            intervals=("1M",),
            start=self.FEB_1,
            end=self.MAR_1,
        )

        assert manifest.files[0].range_end == self.MAR_1
        assert manifest.files[0].range_start == self.FEB_1
