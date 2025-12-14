"""Oracle Database adapter using oracledb."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import ColumnInfo, DatabaseAdapter, TableInfo

if TYPE_CHECKING:
    from ...config import ConnectionConfig


class OracleAdapter(DatabaseAdapter):
    """Adapter for Oracle Database using oracledb.

    Note: Oracle uses schemas extensively, but user_tables/user_views return
    only objects owned by the current user (which acts as the default schema).
    """

    @property
    def name(self) -> str:
        return "Oracle"

    @property
    def supports_multiple_databases(self) -> bool:
        # Oracle uses schemas within a single database, not multiple databases
        return False

    @property
    def supports_stored_procedures(self) -> bool:
        return True

    def connect(self, config: "ConnectionConfig") -> Any:
        """Connect to Oracle database."""
        import oracledb

        port = int(config.port) if config.port else 1521
        # Use Easy Connect string format: host:port/service_name
        dsn = f"{config.server}:{port}/{config.database}"
        return oracledb.connect(
            user=config.username,
            password=config.password,
            dsn=dsn,
        )

    def get_databases(self, conn: Any) -> list[str]:
        """Oracle doesn't support multiple databases - return empty list."""
        return []

    def get_tables(self, conn: Any, database: str | None = None) -> list[TableInfo]:
        """Get list of tables from Oracle. Returns (schema, name) with empty schema."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT table_name FROM user_tables ORDER BY table_name"
        )
        # user_tables returns only current user's tables, so no schema prefix needed
        return [("", row[0]) for row in cursor.fetchall()]

    def get_views(self, conn: Any, database: str | None = None) -> list[TableInfo]:
        """Get list of views from Oracle. Returns (schema, name) with empty schema."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT view_name FROM user_views ORDER BY view_name"
        )
        return [("", row[0]) for row in cursor.fetchall()]

    def get_columns(
        self, conn: Any, table: str, database: str | None = None, schema: str | None = None
    ) -> list[ColumnInfo]:
        """Get columns for a table from Oracle. Schema parameter is ignored."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT column_name, data_type FROM user_tab_columns "
            "WHERE table_name = :1 ORDER BY column_id",
            (table.upper(),),
        )
        return [ColumnInfo(name=row[0], data_type=row[1]) for row in cursor.fetchall()]

    def get_procedures(self, conn: Any, database: str | None = None) -> list[str]:
        """Get stored procedures from Oracle."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT object_name FROM user_procedures "
            "WHERE object_type = 'PROCEDURE' ORDER BY object_name"
        )
        return [row[0] for row in cursor.fetchall()]

    def quote_identifier(self, name: str) -> str:
        """Quote identifier using double quotes for Oracle.

        Escapes embedded double quotes by doubling them.
        """
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def build_select_query(
        self, table: str, limit: int, database: str | None = None, schema: str | None = None
    ) -> str:
        """Build SELECT query with FETCH FIRST for Oracle 12c+. Schema parameter is ignored."""
        return f'SELECT * FROM "{table}" FETCH FIRST {limit} ROWS ONLY'

    def execute_query(
        self, conn: Any, query: str, max_rows: int | None = None
    ) -> tuple[list[str], list[tuple], bool]:
        """Execute a query on Oracle with optional row limit."""
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            if max_rows is not None:
                rows = cursor.fetchmany(max_rows + 1)
                truncated = len(rows) > max_rows
                if truncated:
                    rows = rows[:max_rows]
            else:
                rows = cursor.fetchall()
                truncated = False
            return columns, [tuple(row) for row in rows], truncated
        return [], [], False

    def execute_non_query(self, conn: Any, query: str) -> int:
        """Execute a non-query on Oracle."""
        cursor = conn.cursor()
        cursor.execute(query)
        rowcount = cursor.rowcount
        conn.commit()
        return rowcount
