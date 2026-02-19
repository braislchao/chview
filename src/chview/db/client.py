"""ClickHouse database client and connection management."""

import os
from typing import Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from chview.core.models import ConnectionInfo


class ClickHouseClient:
    """Manages ClickHouse database connections."""

    _instance: Optional["ClickHouseClient"] = None
    _client_kwargs: Optional[dict] = None

    def __new__(cls) -> "ClickHouseClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def _get_connection_params(cls) -> dict:
        """Parse connection parameters from environment variables."""
        if cls._client_kwargs is None:
            cls._client_kwargs = {
                "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
                "port": int(os.getenv("CLICKHOUSE_PORT", "8443")),
                "username": os.getenv("CLICKHOUSE_USER", "default"),
                "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
                "database": os.getenv("CLICKHOUSE_DATABASE", "default"),
                "secure": os.getenv("CLICKHOUSE_SECURE", "True").lower()
                in ("true", "1", "yes"),
                "connect_timeout": 10,
                "send_receive_timeout": 120,
            }
        return cls._client_kwargs

    @classmethod
    def get_client(cls) -> Client:
        """Create a new ClickHouse client instance."""
        params = cls._get_connection_params()
        return clickhouse_connect.get_client(**params)

    @classmethod
    def test_connection(cls) -> tuple[bool, str, str]:
        """Test the ClickHouse connection.

        Returns:
            Tuple of (success, version_or_error, host)
        """
        params = cls._get_connection_params()
        host = f"{params['host']}:{params['port']}"

        try:
            client = cls.get_client()
            result = client.query("SELECT version()")
            version = result.first_row[0]
            return True, version, host
        except Exception as e:
            return False, str(e), host

    @classmethod
    def get_connection_info(cls) -> ConnectionInfo:
        """Get current connection information."""
        params = cls._get_connection_params()
        return ConnectionInfo(
            host=params["host"],
            port=params["port"],
            username=params["username"],
            password=params["password"],
            database=params["database"],
            secure=params["secure"],
        )
