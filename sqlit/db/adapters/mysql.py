"""MySQL adapter using mysql-connector-python."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import MySQLBaseAdapter

if TYPE_CHECKING:
    from ...config import ConnectionConfig


class MySQLAdapter(MySQLBaseAdapter):
    """Adapter for MySQL using mysql-connector-python."""

    @property
    def name(self) -> str:
        return "MySQL"

    def connect(self, config: "ConnectionConfig") -> Any:
        """Connect to MySQL database."""
        import mysql.connector

        port = int(config.port) if config.port else 3306
        return mysql.connector.connect(
            host=config.server,
            port=port,
            database=config.database or None,
            user=config.username,
            password=config.password,
            connection_timeout=10,
        )
