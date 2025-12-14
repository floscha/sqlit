"""CockroachDB adapter using psycopg2 (PostgreSQL wire-compatible)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import PostgresBaseAdapter

if TYPE_CHECKING:
    from ...config import ConnectionConfig


class CockroachDBAdapter(PostgresBaseAdapter):
    """Adapter for CockroachDB using psycopg2 (PostgreSQL wire-compatible)."""

    @property
    def name(self) -> str:
        return "CockroachDB"

    @property
    def supports_stored_procedures(self) -> bool:
        return False  # CockroachDB has limited stored procedure support

    def connect(self, config: "ConnectionConfig") -> Any:
        """Connect to CockroachDB database."""
        import psycopg2

        port = int(config.port) if config.port else 26257
        conn = psycopg2.connect(
            host=config.server,
            port=port,
            database=config.database or "defaultdb",
            user=config.username,
            password=config.password,
            sslmode="disable",  # default container runs insecure; disable TLS for compatibility
            connect_timeout=10,
        )
        # Enable autocommit to avoid transaction issues
        conn.autocommit = True
        return conn

    def get_databases(self, conn: Any) -> list[str]:
        """Get list of databases from CockroachDB."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT database_name FROM [SHOW DATABASES] ORDER BY database_name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_procedures(self, conn: Any, database: str | None = None) -> list[str]:
        """CockroachDB has limited stored procedure support - return empty list."""
        return []
