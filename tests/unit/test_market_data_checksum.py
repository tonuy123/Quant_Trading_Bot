"""DATA-003 checksum verification: determinism, streaming, read-only safety."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from apps.data_checksum_cli.main import main
from packages.market_data.datasets import checksum as checksum_module
from packages.market_data.datasets.checksum import (
    ChecksumVerificationResult,
    compute_file_checksum,
    validate_expected_checksum,
    verify_bytes_checksum,
    verify_dataset_directory,
    verify_file_checksum,
)
from packages.market_data.datasets.metadata import (
    DatasetMetadata,
    compute_dataset_checksum,
)

CHECKSUM = hashlib.sha256(b"payload-bytes").hexdigest()


def write_file(path: Path, content: bytes) -> Path:
    path.write_bytes(content)
    return path


class TestBytesAndFileDeterminism:
    """Deterministic SHA-256 over bytes and raw file bytes."""

    def test_deterministic_sha256_bytes(self) -> None:
        first = verify_bytes_checksum(b"payload-bytes", CHECKSUM)
        second = verify_bytes_checksum(b"payload-bytes", CHECKSUM)

        assert first.matched is True
        assert first.actual == CHECKSUM
        assert first == second

    def test_bytes_match_matches_metadata_content_checksum(self) -> None:
        payload = b'{"a": 1}\n{"b": 2}\n'
        digest = compute_dataset_checksum(payload)

        result = verify_bytes_checksum(payload, digest)

        assert result.matched is True
        assert result.bytes_read == len(payload)

    def test_deterministic_file_checksum(self, tmp_path: Path) -> None:
        path = write_file(tmp_path / "a.bin", b"raw-file-bytes")

        assert compute_file_checksum(path) == compute_file_checksum(path)
        assert compute_file_checksum(path) == hashlib.sha256(b"raw-file-bytes").hexdigest()

    def test_chunked_read_with_file_larger_than_chunk(self, tmp_path: Path) -> None:
        content = bytes(range(256)) * 1000
        path = write_file(tmp_path / "big.bin", content)

        result = verify_file_checksum(path, hashlib.sha256(content).hexdigest(), chunk_size=64)

        assert result.matched is True
        assert result.bytes_read == len(content)

    def test_empty_file_checksum(self, tmp_path: Path) -> None:
        path = write_file(tmp_path / "empty.bin", b"")

        result = verify_file_checksum(path, hashlib.sha256(b"").hexdigest())

        assert result.matched is True
        assert result.bytes_read == 0


class TestExpectedChecksumValidation:
    """Malformed expectations are rejected before any file I/O."""

    def test_uppercase_expected_rejected(self, tmp_path: Path) -> None:
        path = write_file(tmp_path / "a.bin", b"x")
        uppercase = CHECKSUM.upper()

        with pytest.raises(ValueError, match="lowercase"):
            verify_file_checksum(path, uppercase)
        with pytest.raises(ValueError, match="lowercase"):
            validate_expected_checksum(uppercase)

    def test_wrong_length_expected_rejected(self, tmp_path: Path) -> None:
        path = write_file(tmp_path / "a.bin", b"x")

        with pytest.raises(ValueError, match="64-character"):
            verify_file_checksum(path, "a" * 63)

    def test_non_hex_expected_rejected(self, tmp_path: Path) -> None:
        path = write_file(tmp_path / "a.bin", b"x")

        with pytest.raises(ValueError, match="hex"):
            verify_file_checksum(path, "z" * 64)

    def test_non_string_expected_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="string"):
            validate_expected_checksum(123)  # type: ignore[arg-type]

    def test_unsupported_algorithm_rejected(self, tmp_path: Path) -> None:
        path = write_file(tmp_path / "a.bin", b"x")

        with pytest.raises(ValueError, match="sha256"):
            verify_file_checksum(path, CHECKSUM, algorithm="md5")
        with pytest.raises(ValueError, match="sha256"):
            compute_file_checksum(path, algorithm="sha512")


class TestVerifyFileOutcomes:
    """Missing, directory, and read failures are reported, not raised."""

    def test_match(self, tmp_path: Path) -> None:
        path = write_file(tmp_path / "a.bin", b"content")

        result = verify_file_checksum(path, hashlib.sha256(b"content").hexdigest())

        assert isinstance(result, ChecksumVerificationResult)
        assert result.matched is True
        assert result.algorithm == "sha256"
        assert result.error is None

    def test_mismatch(self, tmp_path: Path) -> None:
        path = write_file(tmp_path / "a.bin", b"content")

        result = verify_file_checksum(path, "0" * 64)

        assert result.matched is False
        assert result.actual != result.expected
        assert result.bytes_read == 7

    def test_missing_file(self, tmp_path: Path) -> None:
        result = verify_file_checksum(tmp_path / "ghost.bin", CHECKSUM)

        assert result.matched is False
        assert result.actual is None
        assert result.bytes_read == 0
        assert result.error == "file not found"

    def test_path_is_directory(self, tmp_path: Path) -> None:
        result = verify_file_checksum(tmp_path, CHECKSUM)

        assert result.matched is False
        assert result.error == "path is a directory"

    def test_compute_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            compute_file_checksum(tmp_path / "ghost.bin")

    def test_compute_directory_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="directory"):
            compute_file_checksum(tmp_path)


class TestDatasetDirectoryVerification:
    """Dataset-level report: deterministic, complete, never crashes midway."""

    def _dataset(self, tmp_path: Path) -> tuple[Path, dict[str, str]]:
        files = {
            "BTC-USDT-1m.jsonl": b"line-1\nline-2\n",
            "BTC-USDT-5m.jsonl": b"line-3\n",
            "ETH-USDT-1m.jsonl": b"line-4\n",
        }
        for name, content in files.items():
            write_file(tmp_path / name, content)
        return tmp_path, {
            name: hashlib.sha256(content).hexdigest() for name, content in files.items()
        }

    def test_multiple_files_deterministic_ordering(self, tmp_path: Path) -> None:
        dataset, expected = self._dataset(tmp_path)

        report = verify_dataset_directory(dataset, expected)

        assert report.status == "verified"
        assert [file_report.name for file_report in report.files] == sorted(expected)
        assert report.bytes_read == sum(
            len(c) for c in (b"line-1\nline-2\n", b"line-3\n", b"line-4\n")
        )

    def test_one_mismatch_keeps_other_results(self, tmp_path: Path) -> None:
        dataset, expected = self._dataset(tmp_path)
        expected["BTC-USDT-5m.jsonl"] = "0" * 64

        report = verify_dataset_directory(dataset, expected)

        assert report.status == "mismatch"
        statuses = {file_report.status for file_report in report.files}
        assert statuses == {"verified", "mismatch"}
        assert report.missing == ()

    def test_missing_plus_mismatch_reports_missing(self, tmp_path: Path) -> None:
        dataset, expected = self._dataset(tmp_path)
        expected["BTC-USDT-5m.jsonl"] = "0" * 64
        (dataset / "ETH-USDT-1m.jsonl").unlink()

        report = verify_dataset_directory(dataset, expected)

        assert report.status == "missing"
        assert report.missing == ("ETH-USDT-1m.jsonl",)
        assert {f.status for f in report.files} == {"verified", "mismatch", "missing"}

    def test_manifest_json_excluded_from_unexpected(self, tmp_path: Path) -> None:
        dataset, expected = self._dataset(tmp_path)
        write_file(tmp_path / "manifest.json", b"{}")
        write_file(tmp_path / "stray.bin", b"x")

        report = verify_dataset_directory(dataset, expected)

        assert report.unexpected == ("stray.bin",)

    def test_empty_expected_mapping_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            verify_dataset_directory(tmp_path, {})

    def test_malformed_expected_rejected_before_read(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="hex"):
            verify_dataset_directory(tmp_path, {"a.jsonl": "ABC"})

    def test_path_traversal_name_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="file name"):
            verify_dataset_directory(tmp_path, {"../escape.jsonl": CHECKSUM})
        with pytest.raises(ValueError, match="file name"):
            verify_dataset_directory(tmp_path, {"sub/escape.jsonl": CHECKSUM})

    def test_symlink_escape_reported_invalid(self, tmp_path: Path) -> None:
        outside = write_file(tmp_path.parent / "outside.bin", b"secret")
        dataset = tmp_path / "dataset"
        dataset.mkdir()
        try:
            os.symlink(outside, dataset / "link.jsonl")
        except OSError:
            pytest.skip("symlink creation not permitted on this platform")

        report = verify_dataset_directory(dataset, {"link.jsonl": CHECKSUM})

        assert report.status == "invalid"
        assert report.files[0].status == "invalid"
        assert report.files[0].error == "path escapes the dataset directory"

    def test_directory_entry_reported_invalid(self, tmp_path: Path) -> None:
        dataset = tmp_path / "dataset"
        (dataset / "sub").mkdir(parents=True)

        report = verify_dataset_directory(dataset, {"sub": CHECKSUM})

        assert report.status == "invalid"
        assert report.files[0].status == "invalid"
        assert report.files[0].error == "path is a directory"

    def test_dataset_dir_missing_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="directory"):
            verify_dataset_directory(tmp_path / "ghost", {"a.bin": CHECKSUM})

    def test_no_tmp_files_created(self, tmp_path: Path) -> None:
        dataset, expected = self._dataset(tmp_path)
        before = sorted(p.name for p in tmp_path.iterdir())

        verify_dataset_directory(dataset, expected)

        assert sorted(p.name for p in tmp_path.iterdir()) == before
        assert not list(tmp_path.glob("*.tmp"))

    def test_files_unchanged_bytes_and_mtime(self, tmp_path: Path) -> None:
        dataset, expected = self._dataset(tmp_path)
        state_before = {
            name: (path.read_bytes(), path.stat().st_mtime_ns)
            for name, path in ((name, dataset / name) for name in expected)
        }

        verify_dataset_directory(dataset, expected)

        for name, (content, mtime) in state_before.items():
            path = dataset / name
            assert path.read_bytes() == content
            assert path.stat().st_mtime_ns == mtime


class TestReadFailureNeverPasses:
    """A read failure must never collapse into an overall ``verified``."""

    def _dataset(self, tmp_path: Path) -> tuple[Path, dict[str, str]]:
        files = {
            "BTC-USDT-1m.jsonl": b"line-1\nline-2\n",
            "BTC-USDT-5m.jsonl": b"line-3\n",
            "ETH-USDT-1m.jsonl": b"line-4\n",
        }
        for name, content in files.items():
            write_file(tmp_path / name, content)
        return tmp_path, {
            name: hashlib.sha256(content).hexdigest() for name, content in files.items()
        }

    def _raising_digest(self, *args: object, **kwargs: object) -> tuple[str, int]:
        raise PermissionError("simulated blocked read")

    def test_read_failure_reported_and_not_verified(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset, expected = self._dataset(tmp_path)
        monkeypatch.setattr(checksum_module, "_digest_file", self._raising_digest)

        report = verify_dataset_directory(dataset, expected)

        file_report = report.files[0]
        assert file_report.status == "read_failure"
        assert file_report.actual is None
        assert file_report.bytes_read == 0
        assert report.status == "read_failure"
        assert report.status != "verified"

    def test_error_sanitized_without_path_or_payload(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset, expected = self._dataset(tmp_path)
        monkeypatch.setattr(checksum_module, "_digest_file", self._raising_digest)

        report = verify_dataset_directory(dataset, expected)

        for file_report in report.files:
            assert file_report.error == "cannot read file (PermissionError)"
            assert file_report.actual is None
            assert str(dataset) not in (file_report.error or "")
            assert "payload" not in (file_report.error or "").lower()

    def test_mixed_verified_and_read_failure_overall_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset, expected = self._dataset(tmp_path)
        original = checksum_module._digest_file

        def selective_digest(path: Path, chunk_size: int) -> tuple[str, int]:
            if path.name == "ETH-USDT-1m.jsonl":
                raise OSError("simulated I/O failure")
            return original(path, chunk_size)

        monkeypatch.setattr(checksum_module, "_digest_file", selective_digest)

        report = verify_dataset_directory(dataset, expected)

        assert report.status == "read_failure"
        statuses = {file_report.status for file_report in report.files}
        assert statuses == {"verified", "read_failure"}
        verified = [f for f in report.files if f.status == "verified"]
        assert verified and verified[0].bytes_read > 0

    def test_missing_and_read_failure_priority_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset, expected = self._dataset(tmp_path)
        (dataset / "ETH-USDT-1m.jsonl").unlink()
        monkeypatch.setattr(checksum_module, "_digest_file", self._raising_digest)

        report = verify_dataset_directory(dataset, expected)

        assert report.status == "missing"
        assert report.missing == ("ETH-USDT-1m.jsonl",)

    def test_invalid_and_read_failure_priority_invalid(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset, expected = self._dataset(tmp_path)
        (dataset / "sub").mkdir()
        expected["sub"] = CHECKSUM
        monkeypatch.setattr(checksum_module, "_digest_file", self._raising_digest)

        report = verify_dataset_directory(dataset, expected)

        assert report.status == "invalid"

    def test_read_failure_no_tmp_and_files_unchanged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset, _expected = self._dataset(tmp_path)
        before = sorted(p.name for p in tmp_path.iterdir())
        mtime_before = {p.name: p.stat().st_mtime_ns for p in dataset.iterdir()}
        monkeypatch.setattr(checksum_module, "_digest_file", self._raising_digest)

        assert sorted(p.name for p in tmp_path.iterdir()) == before
        assert not list(tmp_path.glob("*.tmp"))
        assert {p.name: p.stat().st_mtime_ns for p in dataset.iterdir()} == mtime_before

    def test_cli_exit_one_on_read_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset, expected = self._dataset(tmp_path)
        checksums = tmp_path / "checksums.json"
        checksums.write_text(json.dumps(expected), encoding="utf-8")
        monkeypatch.setattr(checksum_module, "_digest_file", self._raising_digest)

        with pytest.raises(SystemExit) as exc_info:
            main(["--dataset", str(dataset), "--checksums", str(checksums)])

        assert exc_info.value.code == 1


class TestMetadataChecksumSemantics:
    """Content checksum vs metadata checksum are never mixed up."""

    def test_content_and_metadata_checksums_are_distinct(self) -> None:
        payload = b'{"a": 1}\n'
        content_digest = compute_dataset_checksum(payload)
        metadata = DatasetMetadata(
            dataset_id="dataset-id",
            dataset_version="1.0.0",
            source="binance_public_rest",
            exchange="binance",
            market_type="spot",
            symbols=("BTC/USDT",),
            intervals=("1m",),
            coverage_start=datetime(2026, 8, 1, tzinfo=UTC),
            coverage_end=datetime(2026, 8, 2, tzinfo=UTC),
            checksum=content_digest,
            record_count=1,
            quality_status="complete",
        )

        assert content_digest != metadata.metadata_checksum()
        assert verify_bytes_checksum(payload, content_digest).matched is True
        assert hashlib.sha256(metadata.to_json().encode("utf-8")).hexdigest() == (
            metadata.metadata_checksum()
        )

    def test_metadata_checksum_field_accepts_content_digest(self) -> None:
        digest = compute_dataset_checksum(b"record-payload-bytes")

        result = verify_bytes_checksum(b"record-payload-bytes", digest)

        assert result.matched is True
        assert len(digest) == 64
        assert digest.islower()


class TestChecksumCli:
    """data-checksum exit codes: 0 verified, 1 content failure, 2 bad arguments."""

    def _dataset(self, tmp_path: Path) -> tuple[Path, dict[str, str]]:
        content = b"kline-data\n"
        (tmp_path / "BTC-USDT-1m.jsonl").write_bytes(content)
        return tmp_path, {"BTC-USDT-1m.jsonl": hashlib.sha256(content).hexdigest()}

    def test_exit_zero_on_match(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        dataset, expected = self._dataset(tmp_path)
        checksums = tmp_path / "checksums.json"
        checksums.write_text(json.dumps(expected), encoding="utf-8")

        with pytest.raises(SystemExit) as exc_info:
            main(["--dataset", str(dataset), "--checksums", str(checksums)])

        assert exc_info.value.code == 0
        assert capsys.readouterr().out.startswith("overall status:     verified")

    def test_exit_one_on_mismatch(self, tmp_path: Path) -> None:
        dataset, _expected = self._dataset(tmp_path)
        checksums = tmp_path / "checksums.json"
        checksums.write_text(json.dumps({"BTC-USDT-1m.jsonl": "0" * 64}), encoding="utf-8")

        with pytest.raises(SystemExit) as exc_info:
            main(["--dataset", str(dataset), "--checksums", str(checksums)])

        assert exc_info.value.code == 1

    def test_exit_one_on_missing(self, tmp_path: Path) -> None:
        dataset, _ = self._dataset(tmp_path)
        checksums = tmp_path / "checksums.json"
        checksums.write_text(
            json.dumps({"BTC-USDT-1m.jsonl": "0" * 64, "ETH-USDT-1m.jsonl": "1" * 64}),
            encoding="utf-8",
        )

        with pytest.raises(SystemExit) as exc_info:
            main(["--dataset", str(dataset), "--checksums", str(checksums)])

        assert exc_info.value.code == 1

    def test_exit_two_on_malformed_checksum_file(self, tmp_path: Path) -> None:
        dataset, _ = self._dataset(tmp_path)
        checksums = tmp_path / "checksums.json"
        checksums.write_text("not json", encoding="utf-8")

        with pytest.raises(SystemExit) as exc_info:
            main(["--dataset", str(dataset), "--checksums", str(checksums)])

        assert exc_info.value.code == 2

    def test_exit_two_on_invalid_checksum_format(self, tmp_path: Path) -> None:
        dataset, _ = self._dataset(tmp_path)
        checksums = tmp_path / "checksums.json"
        checksums.write_text(json.dumps({"a.bin": "SHORT"}), encoding="utf-8")

        with pytest.raises(SystemExit) as exc_info:
            main(["--dataset", str(dataset), "--checksums", str(checksums)])

        assert exc_info.value.code == 2

    def test_exit_two_on_missing_dataset_dir(self, tmp_path: Path) -> None:
        checksums = tmp_path / "checksums.json"
        checksums.write_text(json.dumps({"a.bin": "0" * 64}), encoding="utf-8")

        with pytest.raises(SystemExit) as exc_info:
            main(["--dataset", str(tmp_path / "ghost"), "--checksums", str(checksums)])

        assert exc_info.value.code == 2

    def test_no_network_imports_in_verifier(self) -> None:
        source = Path(__file__).resolve().parents[2] / "packages/market_data/datasets/checksum.py"

        text = source.read_text(encoding="utf-8")

        for forbidden in ("import urllib", "import http", "import socket", "import requests"):
            assert forbidden not in text

    def test_cli_entry_point_help(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "apps.data_checksum_cli.main", "--help"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0
        assert "--checksums" in result.stdout
