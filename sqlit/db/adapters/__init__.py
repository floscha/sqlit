"""Database adapters for sqlit - abstraction layer for different database types."""

from .base import ColumnInfo, DatabaseAdapter, TableInfo
from .cockroachdb import CockroachDBAdapter
from .duckdb import DuckDBAdapter
from .mariadb import MariaDBAdapter
from .mssql import SQLServerAdapter
from .mysql import MySQLAdapter
from .oracle import OracleAdapter
from .postgresql import PostgreSQLAdapter
from .sqlite import SQLiteAdapter

__all__ = [
    # Base
    "ColumnInfo",
    "DatabaseAdapter",
    "TableInfo",
    # Adapters
    "CockroachDBAdapter",
    "DuckDBAdapter",
    "MariaDBAdapter",
    "MySQLAdapter",
    "OracleAdapter",
    "PostgreSQLAdapter",
    "SQLiteAdapter",
    "SQLServerAdapter",
    # Factory
    "get_adapter",
]


def get_adapter(db_type: str) -> DatabaseAdapter:
    """Get the appropriate adapter for a database type."""
    adapters = {
        "mssql": SQLServerAdapter(),
        "sqlite": SQLiteAdapter(),
        "postgresql": PostgreSQLAdapter(),
        "mysql": MySQLAdapter(),
        "oracle": OracleAdapter(),
        "mariadb": MariaDBAdapter(),
        "duckdb": DuckDBAdapter(),
        "cockroachdb": CockroachDBAdapter(),
    }
    adapter = adapters.get(db_type)
    if not adapter:
        raise ValueError(f"Unknown database type: {db_type}")
    return adapter
