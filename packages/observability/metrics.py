"""Prometheus-compatible metrics collection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


@dataclass
class Counter:
    """Counter metric."""

    name: str
    description: str = ""
    labels: dict[str, Any] = field(default_factory=dict)
    _value: float = 0.0

    def inc(self, value: float = 1.0) -> None:
        """Increment counter."""
        self._value += value

    @property
    def value(self) -> float:
        """Get current value."""
        return self._value


@dataclass
class Gauge:
    """Gauge metric."""

    name: str
    description: str = ""
    labels: dict[str, Any] = field(default_factory=dict)
    _value: float = 0.0

    def set(self, value: float) -> None:
        """Set gauge value."""
        self._value = value

    def inc(self, value: float = 1.0) -> None:
        """Increment gauge."""
        self._value += value

    def dec(self, value: float = 1.0) -> None:
        """Decrement gauge."""
        self._value -= value

    @property
    def value(self) -> float:
        """Get current value."""
        return self._value


@dataclass
class Histogram:
    """Histogram metric."""

    name: str
    description: str = ""
    buckets: list[float] = field(
        default_factory=lambda: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    labels: dict[str, Any] = field(default_factory=dict)
    _count: int = 0
    _sum: float = 0.0
    _buckets: dict[float, int] = field(default_factory=dict)

    def observe(self, value: float) -> None:
        """Observe a value."""
        self._count += 1
        self._sum += value
        for bucket in self.buckets:
            if value <= bucket:
                self._buckets[bucket] = self._buckets.get(bucket, 0) + 1

    @property
    def count(self) -> int:
        """Get observation count."""
        return self._count

    @property
    def sum(self) -> float:
        """Get sum of observations."""
        return self._sum


class MetricsCollector:
    """Prometheus-compatible metrics collector.

    Collects and exposes metrics for monitoring.
    """

    def __init__(self) -> None:
        """Initialize collector."""
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}
        self._histograms: dict[str, Histogram] = {}

    def counter(
        self, name: str, description: str = "", labels: dict[str, Any] | None = None
    ) -> Counter:
        """Get or create a counter.

        Args:
            name: Metric name
            description: Metric description
            labels: Metric labels

        Returns:
            Counter metric.
        """
        key = self._make_key(name, labels)
        if key not in self._counters:
            self._counters[key] = Counter(name, description, labels or {})
        return self._counters[key]

    def gauge(
        self, name: str, description: str = "", labels: dict[str, Any] | None = None
    ) -> Gauge:
        """Get or create a gauge.

        Args:
            name: Metric name
            description: Metric description
            labels: Metric labels

        Returns:
            Gauge metric.
        """
        key = self._make_key(name, labels)
        if key not in self._gauges:
            self._gauges[key] = Gauge(name, description, labels or {})
        return self._gauges[key]

    def histogram(
        self, name: str, description: str = "", labels: dict[str, Any] | None = None
    ) -> Histogram:
        """Get or create a histogram.

        Args:
            name: Metric name
            description: Metric description
            labels: Metric labels

        Returns:
            Histogram metric.
        """
        key = self._make_key(name, labels)
        if key not in self._histograms:
            self._histograms[key] = Histogram(name, description, labels=labels or {})
        return self._histograms[key]

    def _make_key(self, name: str, labels: dict[str, Any] | None = None) -> str:
        """Create metric key."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def export(self) -> str:
        """Export metrics in Prometheus format.

        Returns:
            Prometheus-formatted metrics.
        """
        lines = []

        # Export counters
        for counter in self._counters.values():
            lines.append(f"# HELP {counter.name} {counter.description}")
            lines.append(f"# TYPE {counter.name} counter")
            lines.append(f"{counter.name} {counter.value}")

        # Export gauges
        for gauge in self._gauges.values():
            lines.append(f"# HELP {gauge.name} {gauge.description}")
            lines.append(f"# TYPE {gauge.name} gauge")
            lines.append(f"{gauge.name} {gauge.value}")

        # Export histograms
        for histogram in self._histograms.values():
            lines.append(f"# HELP {histogram.name} {histogram.description}")
            lines.append(f"# TYPE {histogram.name} histogram")
            lines.append(f"{histogram.name}_count {histogram.count}")
            lines.append(f"{histogram.name}_sum {histogram.sum}")
            for bucket, count in sorted(histogram._buckets.items()):
                lines.append(f'{histogram.name}_bucket{{le="{bucket}"}} {count}')

        return "\n".join(lines) + "\n"


# Global metrics collector
_metrics: MetricsCollector | None = None


def get_metrics() -> MetricsCollector:
    """Get global metrics collector."""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector()
    return _metrics
