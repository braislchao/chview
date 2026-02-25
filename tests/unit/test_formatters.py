"""Unit tests for chview.core.formatters."""

from chview.core.formatters import (
    format_bytes,
    format_duration_ms,
    format_number,
    format_timestamp_ago,
)


class TestFormatNumber:
    def test_none_returns_dash(self) -> None:
        assert format_number(None) == "—"

    def test_nan_returns_dash(self) -> None:
        assert format_number(float("nan")) == "—"

    def test_small_integer(self) -> None:
        assert format_number(42) == "42"

    def test_exactly_999(self) -> None:
        assert format_number(999) == "999"

    def test_thousands(self) -> None:
        result = format_number(1_200)
        assert result == "1.2K"

    def test_millions(self) -> None:
        result = format_number(3_400_000)
        assert result == "3.4M"

    def test_billions(self) -> None:
        result = format_number(5_600_000_000)
        assert result == "5.6B"

    def test_negative_small(self) -> None:
        assert format_number(-50) == "-50"

    def test_negative_thousands(self) -> None:
        assert format_number(-1_500) == "-1.5K"

    def test_float_input(self) -> None:
        result = format_number(1_000.0)
        assert result == "1.0K"

    def test_zero(self) -> None:
        assert format_number(0) == "0"


class TestFormatBytes:
    def test_none_returns_dash(self) -> None:
        assert format_bytes(None) == "—"

    def test_nan_returns_dash(self) -> None:
        assert format_bytes(float("nan")) == "—"

    def test_bytes(self) -> None:
        assert format_bytes(512) == "512.0 B"

    def test_kilobytes(self) -> None:
        assert format_bytes(1024) == "1.0 KB"

    def test_megabytes(self) -> None:
        assert format_bytes(1024 * 1024) == "1.0 MB"

    def test_gigabytes(self) -> None:
        assert format_bytes(1024**3) == "1.0 GB"

    def test_terabytes(self) -> None:
        assert format_bytes(1024**4) == "1.0 TB"

    def test_partial_megabytes(self) -> None:
        result = format_bytes(1536 * 1024)  # 1.5 MB
        assert result == "1.5 MB"

    def test_zero(self) -> None:
        assert format_bytes(0) == "0.0 B"


class TestFormatDurationMs:
    def test_none_returns_na(self) -> None:
        assert format_duration_ms(None) == "N/A"

    def test_sub_second(self) -> None:
        assert format_duration_ms(123.4) == "123.4 ms"

    def test_exactly_one_second(self) -> None:
        assert format_duration_ms(1_000) == "1.0 s"

    def test_seconds(self) -> None:
        assert format_duration_ms(5_500) == "5.5 s"

    def test_minutes(self) -> None:
        result = format_duration_ms(120_000)  # 2 minutes
        assert result == "2.0 m"

    def test_just_below_second(self) -> None:
        assert format_duration_ms(999.9) == "999.9 ms"

    def test_just_below_minute(self) -> None:
        # 59_999 ms → 60.0 s  (59.999 rounds to 60.0)
        result = format_duration_ms(59_999)
        assert "s" in result

    def test_zero(self) -> None:
        assert format_duration_ms(0) == "0.0 ms"


class TestFormatTimestampAgo:
    def test_seconds(self) -> None:
        assert format_timestamp_ago(30) == "30s ago"

    def test_exactly_60_seconds(self) -> None:
        result = format_timestamp_ago(60)
        assert "m ago" in result

    def test_minutes(self) -> None:
        assert format_timestamp_ago(120) == "2.0m ago"

    def test_hours(self) -> None:
        assert format_timestamp_ago(3600) == "1.0h ago"

    def test_fractional_minutes(self) -> None:
        assert format_timestamp_ago(90) == "1.5m ago"

    def test_fractional_hours(self) -> None:
        assert format_timestamp_ago(5400) == "1.5h ago"

    def test_zero(self) -> None:
        assert format_timestamp_ago(0) == "0s ago"
