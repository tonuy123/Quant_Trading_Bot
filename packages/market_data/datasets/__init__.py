"""Dataset metadata and deterministic version format (DATA-001).

Exports are lazy so canonical event contracts never depend on dataset types
at import time, matching the convention of the persistence package.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "DATASET_SCHEMA_VERSION": "metadata",
    "SUPPORTED_SCHEMA_VERSIONS": "metadata",
    "DatasetMetadata": "metadata",
    "compute_dataset_checksum": "metadata",
    "derive_dataset_id": "metadata",
    "CHUNK_SIZE": "checksum",
    "ChecksumAlgorithm": "checksum",
    "ChecksumVerificationResult": "checksum",
    "DatasetChecksumReport": "checksum",
    "FileChecksumReport": "checksum",
    "compute_file_checksum": "checksum",
    "validate_expected_checksum": "checksum",
    "verify_bytes_checksum": "checksum",
    "verify_dataset_directory": "checksum",
    "verify_file_checksum": "checksum",
    "DATASET_DOWNLOAD_VERSION": "downloader",
    "DOWNLOADER_VERSION": "downloader",
    "DownloadFailure": "downloader",
    "DownloadManifest": "downloader",
    "HistoricalDownloadRequest": "downloader",
    "HistoricalDownloader": "downloader",
    "OutputFileInfo": "downloader",
    "NormalizedTimestampRange": "timestamps",
    "TimestampNormalizationError": "timestamps",
    "TimestampUnit": "timestamps",
    "normalize_datetime_to_utc": "timestamps",
    "normalize_epoch_to_utc": "timestamps",
    "normalize_range_to_utc": "timestamps",
    "normalize_timestamp_to_utc": "timestamps",
    "utc_to_epoch_ms": "timestamps",
    "RESEARCH_SCHEMA_VERSION": "research_format",
    "RESEARCH_SOURCE": "research_format",
    "DecimalInvalidError": "research_format",
    "ResearchCandle": "research_format",
    "ResearchCandleValidationError": "research_format",
    "canonical_decimal": "research_format",
    "research_candle_from_binance_kline": "research_format",
    "ConversionFailure": "converter",
    "ConversionFailureType": "converter",
    "LineConversionResult": "converter",
    "RawConversionContext": "converter",
    "convert_raw_archive_line": "converter",
    "ConversionStatus": "conversion_stream",
    "ConversionStreamError": "conversion_stream",
    "StreamConversionReport": "conversion_stream",
    "conversion_failure_to_ndjson_line": "conversion_stream",
    "convert_raw_archive_stream": "conversion_stream",
    "PublicationStatus": "conversion_manifest",
    "RESEARCH_CONVERTER_VERSION": "conversion_manifest",
    "RESEARCH_DATASET_VERSION": "conversion_manifest",
    "RESEARCH_MANIFEST_FILE": "conversion_manifest",
    "ResearchDatasetManifest": "conversion_manifest",
    "ResearchFileArtifact": "conversion_manifest",
    "ResearchFilePlan": "conversion_manifest",
    "ResearchManifestValidationError": "conversion_manifest",
    "build_research_manifest": "conversion_manifest",
    "FAILURE_OUTPUT_DIRECTORY": "publication_layout",
    "RESEARCH_OUTPUT_DIRECTORY": "publication_layout",
    "STAGING_DIRECTORY": "publication_layout",
    "PublicationLayoutValidationError": "publication_layout",
    "ResearchArtifactPaths": "publication_layout",
    "ResearchPublicationLayout": "publication_layout",
    "OutputDirectoryState": "publication_preflight",
    "PublicationPreflightError": "publication_preflight",
    "PublicationPreflightResult": "publication_preflight",
    "preflight_research_publication": "publication_preflight",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Load an exported dataset symbol only when a caller asks for it."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value
