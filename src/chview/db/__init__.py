"""Database layer public API."""

from chview.db.client import ClickHouseClient
from chview.db.repository import ClickHouseRepository

get_client = ClickHouseClient.get_client

__all__ = [
    "ClickHouseClient",
    "ClickHouseRepository",
    "get_client",
]
