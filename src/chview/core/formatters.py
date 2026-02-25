"""Data formatting utilities."""

import math
from typing import Optional, Union


def format_number(n: Optional[Union[int, float]]) -> str:
    """Format a number in human-readable form (1.2K, 3.4M, etc.).

    Args:
        n: Number to format

    Returns:
        Formatted string like "1.2K", "3.4M", etc.
    """
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"

    n = float(n)
    if abs(n) < 1_000:
        return f"{n:,.0f}"

    for unit in ["", "K", "M", "B", "T"]:
        if abs(n) < 1_000:
            return f"{n:,.1f}{unit}"
        n /= 1_000

    return f"{n:,.1f}P"


def format_bytes(n: Optional[Union[int, float]]) -> str:
    """Format bytes in human-readable form.

    Args:
        n: Number of bytes

    Returns:
        Formatted string like "1.5 GB", "256 MB", etc.
    """
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"

    n = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024

    return f"{n:.1f} PB"


def format_duration_ms(ms: Optional[float]) -> str:
    """Format duration in milliseconds.

    Args:
        ms: Duration in milliseconds

    Returns:
        Formatted string like "1.2 ms", "3.4 s", etc.
    """
    if ms is None:
        return "N/A"

    if ms < 1_000:
        return f"{ms:.1f} ms"
    elif ms < 60_000:
        return f"{ms / 1_000:.1f} s"
    else:
        return f"{ms / 60_000:.1f} m"


def format_timestamp_ago(seconds: int) -> str:
    """Format seconds ago as human-readable string.

    Args:
        seconds: Number of seconds ago

    Returns:
        Formatted string like "5s ago", "2m ago", etc.
    """
    if seconds < 60:
        return f"{seconds:.0f}s ago"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m ago"
    else:
        return f"{seconds / 3600:.1f}h ago"
