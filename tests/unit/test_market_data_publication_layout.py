"""DATA-005 B-2B C1/5: tests for immutable, deterministic physical path layout.

Scope: pure lexical path derivation only. No filesystem I/O.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from dataclasses import FrozenInstanceError, dataclass, fields
from hashlib import sha256
from pathlib import Path

import pytest

from packages.market_data.datasets import (
    FAILURE_OUTPUT_DIRECTORY,
    RESEARCH_OUTPUT_DIRECTORY,
    STAGING_DIRECTORY,
    PublicationLayoutValidationError,
    ResearchPublicationLayout,
)
from packages.market_data.datasets.conversion_manifest import (
    ResearchFileArtifact,
    ResearchFilePlan,
)
from packages.market_data.datasets.conversion_stream import StreamConversionReport

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

_EMPTY_SHA256 = sha256(b"").hexdigest()
_OPEN_MS = 1_704_067_200_000
_CLOSE_MS = _OPEN_MS + 60_000
_RAW_SHA256 = "a" * 64


def _make_artifact(
    *,
    symbol: str = "BTC/USDT",
    interval: str = "1m",
    raw_bytes: int = 256,
    research_bytes: int = 128,
    **overrides: object,
) -> ResearchFileArtifact:
    prefix = f"{symbol.replace('/', '-')}-{interval}"
    raw_name = f"{prefix}.jsonl"
    research_name = f"{prefix}.jsonl"
    failure_name = f"{prefix}.failures.jsonl"
    report = StreamConversionReport(
        file=raw_name,
        lines_seen=1,
        records_written=1,
        records_quarantined=0,
        coverage_start_ms=_OPEN_MS,
        coverage_end_ms=_CLOSE_MS,
        research_sha256="c" * 64,
        failure_sha256=_EMPTY_SHA256,
        research_bytes=research_bytes,
        failure_bytes=0,
        status="success",
    )
    values: dict[str, object] = {
        "raw_name": raw_name,
        "research_name": research_name,
        "failure_name": failure_name,
        "symbol": symbol,
        "interval": interval,
        "raw_sha256": _RAW_SHA256,
        "raw_bytes": raw_bytes,
        "report": report,
    }
    values.update(overrides)
    return ResearchFileArtifact.from_stream_report(**values)  # type: ignore[arg-type]


def _abs(path: str) -> Path:
    """Make a platform-appropriate absolute path without touching the filesystem."""
    return Path(path)


# -------------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------------


class TestConstants:
    def test_research_output_directory_value(self) -> None:
        assert RESEARCH_OUTPUT_DIRECTORY == "research"

    def test_failure_output_directory_value(self) -> None:
        assert FAILURE_OUTPUT_DIRECTORY == "failures"

    def test_staging_directory_value(self) -> None:
        assert STAGING_DIRECTORY == ".staging"


# -------------------------------------------------------------------------
# Valid construction & deterministic properties
# -------------------------------------------------------------------------


class TestValidConstruction:
    def test_valid_windows_absolute_roots(self) -> None:
        raw = _abs(r"D:\data\raw")
        out = _abs(r"D:\data\output")
        layout = ResearchPublicationLayout(raw_dir=raw, output_dir=out)
        assert layout.raw_dir == raw
        assert layout.output_dir == out

    def test_deterministic_property_paths(self) -> None:
        raw = _abs(r"D:\research\raw")
        out = _abs(r"D:\research\published")
        layout = ResearchPublicationLayout(raw_dir=raw, output_dir=out)
        # Same instance: same paths
        assert layout.raw_manifest_path == layout.raw_manifest_path
        assert layout.research_manifest_path == layout.research_manifest_path
        # Cross-check structure
        assert layout.raw_manifest_path == raw / "manifest.json"
        assert layout.research_manifest_path == out / "research_manifest.json"
        assert layout.research_dir == out / "research"
        assert layout.failure_dir == out / "failures"
        assert layout.staging_dir == out / ".staging"
        assert layout.staging_research_dir == out / ".staging" / "research"
        assert layout.staging_failure_dir == out / ".staging" / "failures"
        assert layout.staging_manifest_path == out / ".staging" / "research_manifest.json.tmp"


# -------------------------------------------------------------------------
# Artifact path derivation
# -------------------------------------------------------------------------


class TestArtifactPaths:
    @pytest.fixture
    def layout(self) -> ResearchPublicationLayout:
        return ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )

    @pytest.fixture
    def artifact(self) -> ResearchFileArtifact:
        return _make_artifact(symbol="BTC/USDT", interval="1m")

    def test_raw_path(
        self, layout: ResearchPublicationLayout, artifact: ResearchFileArtifact
    ) -> None:
        result = layout.raw_path(artifact)
        expected = _abs(r"D:\data\raw\BTC-USDT-1m.jsonl")
        assert result == expected

    def test_research_path(
        self, layout: ResearchPublicationLayout, artifact: ResearchFileArtifact
    ) -> None:
        result = layout.research_path(artifact)
        expected = _abs(r"D:\data\output\research\BTC-USDT-1m.jsonl")
        assert result == expected

    def test_failure_path(
        self, layout: ResearchPublicationLayout, artifact: ResearchFileArtifact
    ) -> None:
        result = layout.failure_path(artifact)
        expected = _abs(r"D:\data\output\failures\BTC-USDT-1m.failures.jsonl")
        assert result == expected

    def test_staging_research_path_has_exactly_one_tmp_suffix(
        self, layout: ResearchPublicationLayout, artifact: ResearchFileArtifact
    ) -> None:
        result = layout.staging_research_path(artifact)
        name = result.name
        assert name.endswith(".tmp")
        assert name.count(".tmp") == 1
        assert result == _abs(r"D:\data\output\.staging\research\BTC-USDT-1m.jsonl.tmp")

    def test_staging_failure_path_has_exactly_one_tmp_suffix(
        self, layout: ResearchPublicationLayout, artifact: ResearchFileArtifact
    ) -> None:
        result = layout.staging_failure_path(artifact)
        name = result.name
        assert name.endswith(".tmp")
        assert name.count(".tmp") == 1
        assert result == _abs(r"D:\data\output\.staging\failures\BTC-USDT-1m.failures.jsonl.tmp")

    def test_staging_manifest_path(self, layout: ResearchPublicationLayout) -> None:
        result = layout.staging_manifest_path
        assert result == _abs(r"D:\data\output\.staging\research_manifest.json.tmp")


# -------------------------------------------------------------------------
# 1m vs 1M non-collision
# -------------------------------------------------------------------------


class TestIntervalNonCollision:
    def test_1m_and_1M_do_not_collide(self) -> None:
        layout = ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )
        artifact_1m = _make_artifact(symbol="BTC/USDT", interval="1m")
        artifact_1M = _make_artifact(symbol="BTC/USDT", interval="1M")

        # All path methods must differ as strings (Windows paths are case-insensitive)
        def as_str(p: Path) -> str:
            return p.as_posix()

        assert as_str(layout.raw_path(artifact_1m)) != as_str(layout.raw_path(artifact_1M))
        assert as_str(layout.research_path(artifact_1m)) != as_str(
            layout.research_path(artifact_1M)
        )
        assert as_str(layout.failure_path(artifact_1m)) != as_str(layout.failure_path(artifact_1M))
        assert as_str(layout.staging_research_path(artifact_1m)) != as_str(
            layout.staging_research_path(artifact_1M)
        )
        assert as_str(layout.staging_failure_path(artifact_1m)) != as_str(
            layout.staging_failure_path(artifact_1M)
        )


# -------------------------------------------------------------------------
# Root validation — type rejection
# -------------------------------------------------------------------------


class TestRootTypeRejection:
    def test_string_root_rejected(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=r"D:\data\raw",  # type: ignore
                output_dir=Path(r"D:\data\output"),
            )
        assert "must be a Path" in str(exc_info.value)

    def test_none_root_rejected(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=None,  # type: ignore
                output_dir=Path(r"D:\data\output"),
            )
        assert "must be a Path" in str(exc_info.value)

    def test_object_root_rejected(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=object(),  # type: ignore
                output_dir=Path(r"D:\data\output"),
            )
        assert "must be a Path" in str(exc_info.value)


# -------------------------------------------------------------------------
# Root validation — relative path rejection
# -------------------------------------------------------------------------


class TestRelativePathRejection:
    def test_relative_raw_dir_rejected(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=Path("data/raw"),  # relative
                output_dir=Path(r"D:\data\output"),
            )
        assert "must be absolute" in str(exc_info.value)

    def test_relative_output_dir_rejected(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=Path(r"D:\data\raw"),
                output_dir=Path("data/output"),  # relative
            )
        assert "must be absolute" in str(exc_info.value)


# -------------------------------------------------------------------------
# Root validation — filesystem anchor/root rejection
# -------------------------------------------------------------------------


class TestAnchorRejection:
    def test_windows_drive_root_rejected(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=Path("D:\\"),
                output_dir=Path(r"D:\data\output"),
            )
        assert "filesystem root" in str(exc_info.value) or "anchor" in str(exc_info.value)

    def test_anchor_error_is_detached_and_sanitized(self) -> None:
        raw_root = Path("//SYNTHETIC_SECRET_MARKER/share")
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=raw_root,
                output_dir=Path(r"D:\data\output"),
            )

        error = exc_info.value
        assert error.__cause__ is None
        assert error.__context__ is None
        for rendered in (str(error), repr(error), repr(vars(error))):
            assert str(raw_root) not in rendered
            assert "SYNTHETIC_SECRET_MARKER" not in rendered


# -------------------------------------------------------------------------
# Root validation — dangerous component rejection
# -------------------------------------------------------------------------


class TestDangerousComponentRejection:
    def test_parent_directory_rejection(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=_abs(r"D:\data\raw\.."),
                output_dir=Path(r"D:\data\output"),
            )
        assert (
            "NUL" in str(exc_info.value)
            or "CR" in str(exc_info.value)
            or ".." in str(exc_info.value)
        )

    @pytest.mark.parametrize(
        ("control_character", "reason_token"),
        (("\x00", "NUL"), ("\r", "CR"), ("\n", "LF")),
    )
    def test_control_character_rejected_through_public_layout(
        self,
        control_character: str,
        reason_token: str,
    ) -> None:
        raw = Path(r"D:\data\raw") / f"a{control_character}b"
        if not raw.is_absolute() or control_character not in str(raw):
            pytest.skip("platform Path does not preserve this Windows path component")

        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=raw,
                output_dir=Path(r"D:\data\output"),
            )
        assert reason_token in str(exc_info.value)


# -------------------------------------------------------------------------
# Root validation — equal and nested roots
# -------------------------------------------------------------------------


class TestRootRelationshipRejection:
    def test_equal_roots_rejected(self) -> None:
        root = _abs(r"D:\data\common")
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(raw_dir=root, output_dir=root)
        assert "must not equal" in str(exc_info.value)

    def test_raw_nested_in_output_rejected(self) -> None:
        # raw_dir is a descendant of output_dir
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=_abs(r"D:\data\output\staging"),
                output_dir=_abs(r"D:\data\output"),
            )
        assert "descendant" in str(exc_info.value)

    def test_output_nested_in_raw_rejected(self) -> None:
        # output_dir is a descendant of raw_dir
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=_abs(r"D:\data\raw"),
                output_dir=_abs(r"D:\data\raw\published"),
            )
        assert "descendant" in str(exc_info.value)

    def test_raw_nested_in_output_rejected_case_insensitively(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=Path(r"D:\DATA\OUTPUT\raw"),
                output_dir=Path(r"d:\data\output"),
            )
        assert exc_info.value.field == "raw_dir"

    def test_output_nested_in_raw_rejected_case_insensitively(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=Path(r"D:\DATA\RAW"),
                output_dir=Path(r"d:\data\raw\published"),
            )
        assert exc_info.value.field == "output_dir"


# -------------------------------------------------------------------------
# Artifact type enforcement
# -------------------------------------------------------------------------


class TestArtifactTypeEnforcement:
    @pytest.fixture
    def layout(self) -> ResearchPublicationLayout:
        return ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )

    def test_string_rejected(self, layout: ResearchPublicationLayout) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            layout.raw_path("not an artifact")  # type: ignore
        assert exc_info.value.reason == "must be an exact ResearchFileArtifact"

    def test_none_rejected(self, layout: ResearchPublicationLayout) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            layout.research_path(None)  # type: ignore
        assert exc_info.value.reason == "must be an exact ResearchFileArtifact"

    def test_object_rejected(self, layout: ResearchPublicationLayout) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            layout.failure_path(object())
        assert exc_info.value.reason == "must be an exact ResearchFileArtifact"

    def test_wrong_dataclass_rejected(self, layout: ResearchPublicationLayout) -> None:
        # A frozen dataclass that is NOT ResearchFileArtifact must be rejected
        @dataclass(frozen=True)
        class FakeArtifact:
            raw_name: str = "BTC-USDT-1m.jsonl"
            research_name: str = "BTC-USDT-1m.jsonl"
            failure_name: str = "BTC-USDT-1m.failures.jsonl"
            symbol: str = "BTC/USDT"
            interval: str = "1m"
            raw_sha256: str = "a" * 64
            raw_bytes: int = 256
            lines_seen: int = 1
            records_written: int = 1
            records_quarantined: int = 0
            coverage_start_ms: int | None = None
            coverage_end_ms: int | None = None
            research_sha256: str = "c" * 64
            failure_sha256: str = _EMPTY_SHA256
            research_bytes: int = 128
            failure_bytes: int = 0
            status: str = "success"

        fake = FakeArtifact()
        with pytest.raises(PublicationLayoutValidationError):
            layout.staging_research_path(fake)

    @pytest.mark.parametrize(
        "method_name",
        (
            "raw_path",
            "research_path",
            "failure_path",
            "staging_research_path",
            "staging_failure_path",
        ),
    )
    def test_research_file_artifact_subclass_rejected(
        self,
        layout: ResearchPublicationLayout,
        method_name: str,
    ) -> None:
        class ResearchFileArtifactSubclass(ResearchFileArtifact):
            pass

        artifact = _make_artifact()
        subclass = ResearchFileArtifactSubclass(
            **{field.name: getattr(artifact, field.name) for field in fields(artifact)}
        )

        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            getattr(layout, method_name)(subclass)
        assert exc_info.value.field == "artifact"
        assert exc_info.value.reason == "must be an exact ResearchFileArtifact"


# -------------------------------------------------------------------------
# Frozen dataclass enforcement
# -------------------------------------------------------------------------


class TestFrozenDataclass:
    def test_frozen_prevents_attribute_mutation(self) -> None:
        layout = ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )
        with pytest.raises(FrozenInstanceError):
            layout.raw_dir = Path(r"D:\other\raw")  # type: ignore

    def test_frozen_prevents_post_init_mutation(self) -> None:
        # Verify frozen dataclass raises FrozenInstanceError on attribute assignment.
        # We test this via direct attribute assignment on the frozen instance.
        layout = ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )
        # This should raise FrozenInstanceError (tested in sibling test above).
        # We also verify that the dataclass fields cannot be set via the
        # public attribute syntax.
        with pytest.raises(FrozenInstanceError):
            layout.output_dir = _abs(r"D:\other\output")  # type: ignore[misc]


# -------------------------------------------------------------------------
# Error sanitization
# -------------------------------------------------------------------------


class TestErrorSanitization:
    def test_error_contains_field_and_reason(self) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=Path("relative/path"),  # relative → must be absolute
                output_dir=Path(r"D:\data\output"),
            )
        err = exc_info.value
        assert err.field == "raw_dir"
        assert "absolute" in err.reason
        # Message format: "field: reason"
        assert str(err) == f"{err.field}: {err.reason}"

    def test_error_has_no_raw_path(self) -> None:
        """Error message must not leak the actual path value."""
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=Path("relative/path"),  # relative → triggers validation error
                output_dir=Path(r"D:\data\output"),
            )
        error_str = str(exc_info.value)
        # Must not contain the raw path string "relative/path"
        assert "relative" not in error_str
        assert "sensitive" not in error_str

    def test_error_has_no_original_exception(self) -> None:
        """Error must not chain or embed the original exception."""
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchPublicationLayout(
                raw_dir=Path(r"D:\data\raw"),
                output_dir=Path(r"D:\data\raw"),  # equal → rejected
            )
        error_str = str(exc_info.value)
        # Should be a simple "field: reason" format
        assert error_str.count(":") == 1
        assert "raise" not in error_str.lower()
        assert "traceback" not in error_str.lower()


# -------------------------------------------------------------------------
# Python -O (assertion stripping)
# -------------------------------------------------------------------------


class TestPythonOptimizationMode:
    def test_validation_uses_raise_not_assert(self) -> None:
        """__post_init__ must use explicit raise statements, not assert.

        Python -O strips assert statements. If validation relied on asserts,
        the layout would be constructible with invalid inputs in optimized mode.
        """
        import packages.market_data.datasets.publication_layout as mod

        source = Path(mod.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)

        # Find __post_init__ method
        post_init_has_assert = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "__post_init__":
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Assert):
                        post_init_has_assert = True

        assert not post_init_has_assert, (
            "__post_init__ must not use assert; use explicit raise instead "
            "so validation works in python -O mode"
        )

    def test_relative_root_rejected_by_optimized_subprocess(self) -> None:
        program = """
from pathlib import Path
from packages.market_data.datasets import (
    PublicationLayoutValidationError,
    ResearchPublicationLayout,
)

try:
    ResearchPublicationLayout(
        raw_dir=Path("relative/raw"),
        output_dir=Path("D:/data/output"),
    )
except PublicationLayoutValidationError:
    raise SystemExit(0)
raise SystemExit(1)
"""
        result = subprocess.run(
            [sys.executable, "-O", "-c", program],
            capture_output=True,
            check=False,
            text=True,
        )
        assert result.returncode == 0, result.stderr


# -------------------------------------------------------------------------
# Filesystem-call audit via AST
# -------------------------------------------------------------------------


class TestNoFilesystemCalls:
    """Verify the module performs NO filesystem operations."""

    _FORBIDDEN_CALLS: frozenset[str] = frozenset(
        {
            # file operations
            "open",
            "read",
            "write",
            "stat",
            "exists",
            "is_file",
            "is_dir",
            "is_symlink",
            "isdir",
            "lexists",
            # path operations that hit filesystem
            "resolve",
            "readlink",
            "read_bytes",
            "read_text",
            "write_bytes",
            "write_text",
            # directory operations
            "mkdir",
            "rmdir",
            "unlink",
            "remove",
            "rmtree",
            "makedirs",
            "renames",
            "replace",
            # os/shutil/tempfile
            "os_path_exists",
            "os_path_isfile",
            "os_path_isdir",
            # hashlib / network / logging / env
            "getenv",
            "environ",
        }
    )

    _FORBIDDEN_MODULES: frozenset[str] = frozenset(
        {
            "os",
            "shutil",
            "tempfile",
            "pathlib",  # pathlib used only for Path type
        }
    )

    def test_no_forbidden_calls_in_source(self) -> None:
        import packages.market_data.datasets.publication_layout as mod

        source = Path(mod.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)

        violations: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in self._FORBIDDEN_CALLS:
                        violations.append(f"forbidden call: {node.func.id}")
                elif isinstance(node.func, ast.Attribute):
                    full = node.func.attr
                    if full in self._FORBIDDEN_CALLS:
                        violations.append(f"forbidden attribute: {full}")

        assert violations == [], f"Found filesystem calls: {violations}"

    def test_no_resolve_in_source(self) -> None:
        import packages.market_data.datasets.publication_layout as mod

        source = Path(mod.__file__).read_text(encoding="utf-8")
        assert ".resolve()" not in source
        assert "resolve(" not in source


# -------------------------------------------------------------------------
# Import smoke test — five lazy exports
# -------------------------------------------------------------------------


class TestFiveLazyExports:
    """Verify the __init__.py lazy-export contract."""

    def test_failure_output_directory_exported(self) -> None:
        from packages.market_data.datasets import FAILURE_OUTPUT_DIRECTORY

        assert FAILURE_OUTPUT_DIRECTORY == "failures"

    def test_research_output_directory_exported(self) -> None:
        from packages.market_data.datasets import RESEARCH_OUTPUT_DIRECTORY

        assert RESEARCH_OUTPUT_DIRECTORY == "research"

    def test_staging_directory_exported(self) -> None:
        from packages.market_data.datasets import STAGING_DIRECTORY

        assert STAGING_DIRECTORY == ".staging"

    def test_publication_layout_validation_error_exported(self) -> None:
        from packages.market_data.datasets import PublicationLayoutValidationError

        with pytest.raises(PublicationLayoutValidationError):
            raise PublicationLayoutValidationError(field="test", reason="smoke")

    def test_research_publication_layout_exported(self) -> None:
        from packages.market_data.datasets import ResearchPublicationLayout

        layout = ResearchPublicationLayout(
            raw_dir=_abs(r"D:\smoke\raw"),
            output_dir=_abs(r"D:\smoke\output"),
        )
        assert layout.raw_dir == _abs(r"D:\smoke\raw")


# -------------------------------------------------------------------------
# Path invariants summary
# -------------------------------------------------------------------------


class TestPathInvariants:
    """Structural invariants that must hold for all valid layouts."""

    @pytest.fixture
    def layout(self) -> ResearchPublicationLayout:
        return ResearchPublicationLayout(
            raw_dir=_abs(r"D:\market\raw"),
            output_dir=_abs(r"D:\market\output"),
        )

    @pytest.fixture
    def artifact(self) -> ResearchFileArtifact:
        return _make_artifact(symbol="ETH/USDT", interval="1h")

    def test_raw_manifest_is_research_manifest_in_raw(
        self, layout: ResearchPublicationLayout
    ) -> None:
        assert layout.raw_manifest_path == layout.raw_dir / "manifest.json"

    def test_research_manifest_is_research_manifest_in_output(
        self, layout: ResearchPublicationLayout
    ) -> None:
        assert layout.research_manifest_path == layout.output_dir / "research_manifest.json"

    def test_staging_is_inside_output(self, layout: ResearchPublicationLayout) -> None:
        assert layout.staging_dir == layout.output_dir / ".staging"
        # All staging paths must start with staging_dir
        assert layout.staging_research_path(_make_artifact()).parent == layout.staging_research_dir
        assert layout.staging_failure_path(_make_artifact()).parent == layout.staging_failure_dir

    def test_artifact_paths_preserve_research_name_format(
        self, layout: ResearchPublicationLayout, artifact: ResearchFileArtifact
    ) -> None:
        # Research artifact path name must match artifact.research_name exactly
        assert layout.research_path(artifact).name == artifact.research_name
        assert layout.staging_research_path(artifact).name == f"{artifact.research_name}.tmp"

    def test_artifact_paths_preserve_failure_name_format(
        self, layout: ResearchPublicationLayout, artifact: ResearchFileArtifact
    ) -> None:
        assert layout.failure_path(artifact).name == artifact.failure_name
        assert layout.staging_failure_path(artifact).name == f"{artifact.failure_name}.tmp"

    def test_no_symlink_or_resolve_in_property_paths(
        self, layout: ResearchPublicationLayout
    ) -> None:
        # All properties must be simple Path joins
        for attr_name in dir(layout):
            if attr_name.startswith("_"):
                continue
            attr = getattr(layout, attr_name)
            if callable(attr):
                continue
            # Paths must be Path instances
            assert isinstance(attr, Path), f"{attr_name} must be a Path"
            # Must not contain resolved/mounted paths
            assert "resolved" not in str(attr).lower()


# =============================================================================
# ResearchArtifactPaths + artifact_paths_for() tests — DATA-005 C3B-1
# =============================================================================


class TestResearchArtifactPathsValidConstruction:
    """Valid ResearchArtifactPaths construction."""

    def test_five_canonical_paths(self) -> None:
        from packages.market_data.datasets.publication_layout import (
            ResearchArtifactPaths,
        )

        paths = ResearchArtifactPaths(
            raw_path=Path(r"D:\data\raw\BTC-USDT-1m.jsonl"),
            research_path=Path(r"D:\data\output\research\BTC-USDT-1m.jsonl"),
            failure_path=Path(r"D:\data\output\failures\BTC-USDT-1m.failures.jsonl"),
            staging_research_path=Path(r"D:\data\output\.staging\research\BTC-USDT-1m.jsonl.tmp"),
            staging_failure_path=Path(
                r"D:\data\output\.staging\failures\BTC-USDT-1m.failures.jsonl.tmp"
            ),
        )
        assert paths.raw_path == Path(r"D:\data\raw\BTC-USDT-1m.jsonl")
        assert paths.research_path == Path(r"D:\data\output\research\BTC-USDT-1m.jsonl")
        assert paths.failure_path == Path(r"D:\data\output\failures\BTC-USDT-1m.failures.jsonl")
        assert paths.staging_research_path == Path(
            r"D:\data\output\.staging\research\BTC-USDT-1m.jsonl.tmp"
        )
        assert paths.staging_failure_path == Path(
            r"D:\data\output\.staging\failures\BTC-USDT-1m.failures.jsonl.tmp"
        )


class TestArtifactPathsForMethod:
    """artifact_paths_for() derives correct paths from a plan."""

    @pytest.fixture
    def layout(self) -> ResearchPublicationLayout:
        return ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )

    @pytest.fixture
    def plan(self) -> ResearchFilePlan:
        return ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1m.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )

    def test_artifact_paths_for_returns_correct_raw_path(
        self, layout: ResearchPublicationLayout, plan: ResearchFilePlan
    ) -> None:
        paths = layout.artifact_paths_for(plan)
        assert paths.raw_path == layout.raw_path(_make_artifact(symbol="BTC/USDT", interval="1m"))

    def test_artifact_paths_for_returns_correct_research_path(
        self, layout: ResearchPublicationLayout, plan: ResearchFilePlan
    ) -> None:
        paths = layout.artifact_paths_for(plan)
        assert paths.research_path == layout.research_path(
            _make_artifact(symbol="BTC/USDT", interval="1m")
        )

    def test_artifact_paths_for_returns_correct_failure_path(
        self, layout: ResearchPublicationLayout, plan: ResearchFilePlan
    ) -> None:
        paths = layout.artifact_paths_for(plan)
        assert paths.failure_path == layout.failure_path(
            _make_artifact(symbol="BTC/USDT", interval="1m")
        )

    def test_artifact_paths_for_returns_correct_staging_research_path(
        self, layout: ResearchPublicationLayout, plan: ResearchFilePlan
    ) -> None:
        paths = layout.artifact_paths_for(plan)
        assert paths.staging_research_path == layout.staging_research_path(
            _make_artifact(symbol="BTC/USDT", interval="1m")
        )

    def test_artifact_paths_for_returns_correct_staging_failure_path(
        self, layout: ResearchPublicationLayout, plan: ResearchFilePlan
    ) -> None:
        paths = layout.artifact_paths_for(plan)
        assert paths.staging_failure_path == layout.staging_failure_path(
            _make_artifact(symbol="BTC/USDT", interval="1m")
        )

    def test_artifact_paths_for_and_old_methods_identical(
        self, layout: ResearchPublicationLayout
    ) -> None:
        # For equivalent artifact/plan, old methods and new must agree.
        plan = ResearchFilePlan.from_raw_identity(
            raw_name="ETH-USDT-1h.jsonl",
            symbol="ETH/USDT",
            interval="1h",
        )
        artifact = _make_artifact(symbol="ETH/USDT", interval="1h")

        paths = layout.artifact_paths_for(plan)
        assert paths.raw_path == layout.raw_path(artifact)
        assert paths.research_path == layout.research_path(artifact)
        assert paths.failure_path == layout.failure_path(artifact)
        assert paths.staging_research_path == layout.staging_research_path(artifact)
        assert paths.staging_failure_path == layout.staging_failure_path(artifact)

    def test_artifact_paths_for_is_deterministic(self, layout: ResearchPublicationLayout) -> None:
        plan = ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1m.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )
        paths1 = layout.artifact_paths_for(plan)
        paths2 = layout.artifact_paths_for(plan)
        assert paths1 == paths2
        assert paths1.raw_path == paths2.raw_path

    def test_staging_path_has_exactly_one_tmp_suffix(
        self, layout: ResearchPublicationLayout
    ) -> None:
        plan = ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1m.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )
        paths = layout.artifact_paths_for(plan)
        assert paths.staging_research_path.name.endswith(".tmp")
        assert paths.staging_research_path.name.count(".tmp") == 1
        assert paths.staging_failure_path.name.endswith(".tmp")
        assert paths.staging_failure_path.name.count(".tmp") == 1


class TestArtifactPathsForTypeEnforcement:
    """artifact_paths_for() requires exact ResearchFilePlan."""

    @pytest.fixture
    def layout(self) -> ResearchPublicationLayout:
        return ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )

    def test_string_rejected(self, layout: ResearchPublicationLayout) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            layout.artifact_paths_for("not a plan")  # type: ignore
        assert exc_info.value.field == "plan"
        assert "exact ResearchFilePlan" in exc_info.value.reason

    def test_none_rejected(self, layout: ResearchPublicationLayout) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            layout.artifact_paths_for(None)  # type: ignore
        assert exc_info.value.field == "plan"

    def test_object_rejected(self, layout: ResearchPublicationLayout) -> None:
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            layout.artifact_paths_for(object())
        assert exc_info.value.field == "plan"

    def test_subclass_rejected(self, layout: ResearchPublicationLayout) -> None:
        class ResearchFilePlanSubclass(ResearchFilePlan):
            pass

        plan = ResearchFilePlanSubclass(
            raw_name="BTC-USDT-1m.jsonl",
            research_name="BTC-USDT-1m.jsonl",
            failure_name="BTC-USDT-1m.failures.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )
        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            layout.artifact_paths_for(plan)
        assert exc_info.value.field == "plan"
        assert "exact ResearchFilePlan" in exc_info.value.reason


class TestResearchArtifactPathsStrictTypes:
    """ResearchArtifactPaths requires exact Path types, absolute paths."""

    def test_string_path_rejected(self) -> None:
        from packages.market_data.datasets.publication_layout import (
            ResearchArtifactPaths,
        )

        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchArtifactPaths(
                raw_path="D:\\data\\raw\\file.jsonl",  # type: ignore
                research_path=Path(r"D:\data\output\research\file.jsonl"),
                failure_path=Path(r"D:\data\output\failures\file.failures.jsonl"),
                staging_research_path=Path(r"D:\data\output\.staging\research\file.jsonl.tmp"),
                staging_failure_path=Path(
                    r"D:\data\output\.staging\failures\file.failures.jsonl.tmp"
                ),
            )
        assert exc_info.value.field == "raw_path"
        assert "Path" in exc_info.value.reason

    def test_none_path_rejected(self) -> None:
        from packages.market_data.datasets.publication_layout import (
            ResearchArtifactPaths,
        )

        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchArtifactPaths(
                raw_path=Path(r"D:\data\raw\file.jsonl"),
                research_path=None,  # type: ignore
                failure_path=Path(r"D:\data\output\failures\file.failures.jsonl"),
                staging_research_path=Path(r"D:\data\output\.staging\research\file.jsonl.tmp"),
                staging_failure_path=Path(
                    r"D:\data\output\.staging\failures\file.failures.jsonl.tmp"
                ),
            )
        assert exc_info.value.field == "research_path"

    def test_relative_path_rejected(self) -> None:
        from packages.market_data.datasets.publication_layout import (
            ResearchArtifactPaths,
        )

        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchArtifactPaths(
                raw_path=Path("data/raw/file.jsonl"),  # relative
                research_path=Path(r"D:\data\output\research\file.jsonl"),
                failure_path=Path(r"D:\data\output\failures\file.failures.jsonl"),
                staging_research_path=Path(r"D:\data\output\.staging\research\file.jsonl.tmp"),
                staging_failure_path=Path(
                    r"D:\data\output\.staging\failures\file.failures.jsonl.tmp"
                ),
            )
        assert exc_info.value.field == "raw_path"
        assert "absolute" in exc_info.value.reason

    def test_dangerous_component_rejected(self) -> None:
        from packages.market_data.datasets.publication_layout import (
            ResearchArtifactPaths,
        )

        with pytest.raises(PublicationLayoutValidationError) as exc_info:
            ResearchArtifactPaths(
                raw_path=Path(r"D:\data\raw\..\etc\file.jsonl"),
                research_path=Path(r"D:\data\output\research\file.jsonl"),
                failure_path=Path(r"D:\data\output\failures\file.failures.jsonl"),
                staging_research_path=Path(r"D:\data\output\.staging\research\file.jsonl.tmp"),
                staging_failure_path=Path(
                    r"D:\data\output\.staging\failures\file.failures.jsonl.tmp"
                ),
            )
        assert exc_info.value.field == "raw_path"
        assert ".." in exc_info.value.reason or "NUL" in exc_info.value.reason


class TestArtifactPathsForIntervalDistinction:
    """1m and 1M produce distinct paths."""

    def test_1m_and_1M_paths_distinct(self) -> None:
        layout = ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )
        plan_1m = ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1m.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )
        plan_1M = ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1M.jsonl",
            symbol="BTC/USDT",
            interval="1M",
        )
        paths_1m = layout.artifact_paths_for(plan_1m)
        paths_1M = layout.artifact_paths_for(plan_1M)

        def as_str(p: Path) -> str:
            return p.as_posix()

        assert as_str(paths_1m.raw_path) != as_str(paths_1M.raw_path)
        assert as_str(paths_1m.research_path) != as_str(paths_1M.research_path)
        assert as_str(paths_1m.failure_path) != as_str(paths_1M.failure_path)


class TestResearchArtifactPathsFrozen:
    """ResearchArtifactPaths is frozen."""

    def test_frozen_prevents_mutation(self) -> None:
        from packages.market_data.datasets.publication_layout import (
            ResearchArtifactPaths,
        )

        paths = ResearchArtifactPaths(
            raw_path=Path(r"D:\data\raw\BTC-USDT-1m.jsonl"),
            research_path=Path(r"D:\data\output\research\BTC-USDT-1m.jsonl"),
            failure_path=Path(r"D:\data\output\failures\BTC-USDT-1m.failures.jsonl"),
            staging_research_path=Path(r"D:\data\output\.staging\research\BTC-USDT-1m.jsonl.tmp"),
            staging_failure_path=Path(
                r"D:\data\output\.staging\failures\BTC-USDT-1m.failures.jsonl.tmp"
            ),
        )
        with pytest.raises(FrozenInstanceError):
            paths.raw_path = Path(r"D:\other\path")  # type: ignore[misc]


class TestResearchArtifactPathsNoFilesystemCalls:
    """ResearchArtifactPaths performs no filesystem I/O."""

    _FORBIDDEN: frozenset[str] = frozenset(
        {
            "open",
            "stat",
            "exists",
            "is_file",
            "is_dir",
            "resolve",
            "read_bytes",
            "read_text",
            "write_bytes",
            "write_text",
            "mkdir",
            "unlink",
        }
    )

    def test_no_filesystem_calls_in_artifact_paths_source(self) -> None:
        import packages.market_data.datasets.publication_layout as mod

        source = Path(mod.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)

        violations: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in self._FORBIDDEN:
                        violations.append(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    if node.func.attr in self._FORBIDDEN:
                        violations.append(node.func.attr)

        assert violations == [], f"Found filesystem calls: {violations}"

    def test_artifact_paths_for_produces_result_without_filesystem(
        self,
    ) -> None:
        layout = ResearchPublicationLayout(
            raw_dir=_abs(r"D:\data\raw"),
            output_dir=_abs(r"D:\data\output"),
        )
        plan = ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1m.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )
        # This must not touch the filesystem.
        paths = layout.artifact_paths_for(plan)
        assert paths.raw_path == Path(r"D:\data\raw\BTC-USDT-1m.jsonl")
        assert paths.research_path == Path(r"D:\data\output\research\BTC-USDT-1m.jsonl")


class TestResearchArtifactPathsLazyExport:
    """ResearchArtifactPaths is exported from datasets.__init__."""

    def test_paths_exported(self) -> None:
        from packages.market_data.datasets import ResearchArtifactPaths

        assert ResearchArtifactPaths is not None

    def test_plan_exported(self) -> None:
        from packages.market_data.datasets import ResearchFilePlan

        assert ResearchFilePlan is not None
