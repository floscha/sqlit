"""Database abstraction layer for sqlit."""

from .adapters import (
    ColumnInfo,
    CockroachDBAdapter,
    DatabaseAdapter,
    DuckDBAdapter,
    MariaDBAdapter,
    MySQLAdapter,
    OracleAdapter,
    PostgreSQLAdapter,
    SQLiteAdapter,
    SQLServerAdapter,
    get_adapter,
)
from .schema import (
    ConnectionSchema,
    FieldType,
    SchemaField,
    SelectOption,
    get_all_schemas,
    get_connection_schema,
    get_supported_db_types,
)
from .tunnel import create_ssh_tunnel

__all__ = [
    # Base
    "ColumnInfo",
    "DatabaseAdapter",
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
    # Schema
    "ConnectionSchema",
    "FieldType",
    "SchemaField",
    "SelectOption",
    "get_all_schemas",
    "get_connection_schema",
    "get_supported_db_types",
    # Tunnel
    "create_ssh_tunnel",
]
