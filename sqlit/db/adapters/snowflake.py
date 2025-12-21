"""Snowflake adapter using snowflake-connector-python."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import CursorBasedAdapter, DockerCredentials, import_driver_module, ColumnInfo, IndexInfo, TriggerInfo, SequenceInfo, TableInfo

if TYPE_CHECKING:
    from ...config import ConnectionConfig


class SnowflakeAdapter(CursorBasedAdapter):
    """Adapter for Snowflake."""

    @classmethod
    def badge_label(cls) -> str:
        return "SNOW"

    @property
    def name(self) -> str:
        return "Snowflake"

    @property
    def install_extra(self) -> str:
        return "snowflake"

    @property
    def install_package(self) -> str:
        return "snowflake-connector-python"

    @property
    def driver_import_names(self) -> tuple[str, ...]:
        return ("snowflake.connector",)

    @property
    def supports_multiple_databases(self) -> bool:
        return True

    @property
    def supports_stored_procedures(self) -> bool:
        return True

    @property
    def default_schema(self) -> str:
        return "PUBLIC"

    def connect(self, config: ConnectionConfig) -> Any:
        """Connect to Snowflake database."""
        sf = import_driver_module(
            "snowflake.connector",
            driver_name=self.name,
            extra_name=self.install_extra,
            package_name=self.install_package,
        )

        # Map 'server' to 'account'
        connect_args = {
            "user": config.username,
            "password": config.password,
            "account": config.server,
            "database": config.database,
        }

        # Handle optional fields if they exist in config.extra or explicitly defined
        # Since we added them to schema, they should be in config dictionary if accessed correctly,
        # but ConnectionConfig usually stores extra fields in a flexible way or we need to access them specifically.
        # ConnectionConfig is a Pydantic model or dataclass. Let's assume standard fields.
        # For extra fields defined in schema but not in ConnectionConfig core fields, they might be in `config` if it's a dict,
        # but here `config` is an object.
        # Looking at `sqlit/config.py` would confirm, but usually extra fields are passed differently or
        # we might need to check how `sqlit` handles schema-specific fields.
        # For now, I'll access standard fields. If `warehouse` is stored in `extra`, I need to know how to access it.
        # Let's assume for now they might be passed via some mechanism or we stick to standard args.
        # However, `ConnectionConfig` likely has an `extras` dict or similar?

        # Let's check `config` object structure in `sqlit/config.py`.
        # I'll rely on the user providing them in the specific fields if I can access them.

        # NOTE: Without checking `ConnectionConfig` definition, I'll assume I can access extras.
        # But wait, `ConnectionConfig` is imported in `base.py`. Let's check it in a separate turn if needed.
        # For now, I'll assume standard connection.

        # Additional args from our schema:
        # warehouse, schema, role.
        # If the config object allows dynamic attribute access or has a dict method, we can use that.
        # I'll try to pull them from `config` assuming it might have them or `extra` dict.

        extras = getattr(config, "extras", {}) or {}
        if "warehouse" in extras:
            connect_args["warehouse"] = extras["warehouse"]
        if "schema" in extras:
            connect_args["schema"] = extras["schema"]
        if "role" in extras:
            connect_args["role"] = extras["role"]

        # Also check if they are top-level attributes if `ConnectionConfig` is dynamic (unlikely).
        # But let's look at `sqlit/config.py` later.

        return sf.connect(**connect_args)

    def get_databases(self, conn: Any) -> list[str]:
        """Get list of databases."""
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()] # row[1] is 'name' in SHOW DATABASES output

    def get_tables(self, conn: Any, database: str | None = None) -> list[TableInfo]:
        """Get list of tables."""
        cursor = conn.cursor()
        # Snowflake doesn't support changing database in connection easily for query context without USE.
        # But we can query information_schema or SHOW TABLES.
        # SHOW TABLES IN DATABASE ...

        query = "SHOW TABLES"
        if database:
            query += f" IN DATABASE {self.quote_identifier(database)}"

        cursor.execute(query)
        # SHOW TABLES returns: created_on, name, database_name, schema_name, ...
        # We need (schema, name)
        return [(row[3], row[1]) for row in cursor.fetchall()]

    def get_views(self, conn: Any, database: str | None = None) -> list[TableInfo]:
        """Get list of views."""
        cursor = conn.cursor()
        query = "SHOW VIEWS"
        if database:
            query += f" IN DATABASE {self.quote_identifier(database)}"
        cursor.execute(query)
        # SHOW VIEWS returns similar structure: ..., name, ..., schema_name, ...
        # Check column index for SHOW VIEWS. Usually: created_on, name, kind, database_name, schema_name
        # Actually it's best to use INFORMATION_SCHEMA for consistency if possible, but SHOW commands are faster in Snowflake sometimes.
        # Let's check column indices or use dict cursor if available? No, usually list.
        # SHOW TABLES: 1=name, 3=schema_name
        # SHOW VIEWS: 1=name, 4=schema_name (need verification, varies by version)

        # Alternative: INFORMATION_SCHEMA
        sql = "SELECT table_schema, table_name FROM information_schema.views"
        if database:
             # If we can't cross-database query easily without full qualification
             sql = f"SELECT table_schema, table_name FROM {self.quote_identifier(database)}.information_schema.views"

        sql += " WHERE table_schema != 'INFORMATION_SCHEMA' ORDER BY table_schema, table_name"
        cursor.execute(sql)
        return [(row[0], row[1]) for row in cursor.fetchall()]

    def get_tables_via_info_schema(self, conn: Any, database: str | None = None) -> list[TableInfo]:
        """Fallback or alternative to get tables."""
        cursor = conn.cursor()
        db_prefix = f"{self.quote_identifier(database)}." if database else ""
        sql = f"SELECT table_schema, table_name FROM {db_prefix}information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema != 'INFORMATION_SCHEMA' ORDER BY table_schema, table_name"
        cursor.execute(sql)
        return [(row[0], row[1]) for row in cursor.fetchall()]

    # I'll stick to INFORMATION_SCHEMA for robustness across versions unless slow.
    def get_tables(self, conn: Any, database: str | None = None) -> list[TableInfo]:
        return self.get_tables_via_info_schema(conn, database)

    def get_columns(
        self, conn: Any, table: str, database: str | None = None, schema: str | None = None
    ) -> list[ColumnInfo]:
        """Get columns for a table."""
        cursor = conn.cursor()
        schema = schema or "PUBLIC"
        db_prefix = f"{self.quote_identifier(database)}." if database else ""

        # Snowflake Info Schema
        sql = f"""
            SELECT column_name, data_type, ordinal_position
            FROM {db_prefix}information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        cursor.execute(sql, (schema, table))

        # Primary keys
        # This is more complex in Snowflake/generic info schema.
        # Often easier to assume no PK or fetch if critical.
        # Let's try to fetch PKs.
        pk_sql = f"""
            SELECT kcu.column_name
            FROM {db_prefix}information_schema.table_constraints tc
            JOIN {db_prefix}information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %s AND tc.table_name = %s
        """
        # Execute PK query separately
        # Note: cursor might be consumed.
        rows = cursor.fetchall()

        cursor.execute(pk_sql, (schema, table))
        pk_columns = {row[0] for row in cursor.fetchall()}

        return [
            ColumnInfo(name=row[0], data_type=row[1], is_primary_key=row[0] in pk_columns)
            for row in rows
        ]

    def quote_identifier(self, name: str) -> str:
        """Quote identifier using double quotes (Snowflake standard)."""
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def build_select_query(self, table: str, limit: int, database: str | None = None, schema: str | None = None) -> str:
        """Build SELECT LIMIT query."""
        schema = schema or "PUBLIC"
        return f'SELECT * FROM "{schema}"."{table}" LIMIT {limit}'

    def get_procedures(self, conn: Any, database: str | None = None) -> list[str]:
        """Get stored procedures."""
        cursor = conn.cursor()
        db_prefix = f"{self.quote_identifier(database)}." if database else ""
        sql = f"SELECT routine_name FROM {db_prefix}information_schema.routines WHERE routine_type = 'PROCEDURE' AND routine_schema != 'INFORMATION_SCHEMA' ORDER BY routine_name"
        cursor.execute(sql)
        # deduplicate
        return sorted(list({row[0] for row in cursor.fetchall()}))

    def get_indexes(self, conn: Any, database: str | None = None) -> list[IndexInfo]:
        """Get indexes."""
        # Snowflake doesn't really have traditional indexes like Postgres/MySQL.
        # It has clustering keys, search optimization service, etc.
        # But 'SHOW PRIMARY KEYS' or similar might work.
        # For now, return empty list as Snowflake is mostly auto-managed.
        return []

    def get_triggers(self, conn: Any, database: str | None = None) -> list[TriggerInfo]:
        """Get triggers."""
        # Snowflake supports streams and tasks, and recently alerts, but "triggers" are not standard.
        return []

    def get_sequences(self, conn: Any, database: str | None = None) -> list[SequenceInfo]:
        """Get sequences."""
        cursor = conn.cursor()
        db_prefix = f"{self.quote_identifier(database)}." if database else ""
        sql = f"SELECT sequence_name FROM {db_prefix}information_schema.sequences WHERE sequence_schema != 'INFORMATION_SCHEMA'"
        cursor.execute(sql)
        return [SequenceInfo(name=row[0]) for row in cursor.fetchall()]

    def execute_query(self, conn: Any, query: str, max_rows: int | None = None) -> tuple[list[str], list[tuple], bool]:
        """Execute query."""
        cursor = conn.cursor()
        cursor.execute(query)

        columns = []
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

    def execute_non_query(self, conn: Any, query: str) -> int:
        cursor = conn.cursor()
        cursor.execute(query)
        # Snowflake doesn't always return rowcount reliably for all ops, but try.
        return int(cursor.rowcount) if cursor.rowcount is not None else -1
