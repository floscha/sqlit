"""CLI command handlers for sqlit."""

from __future__ import annotations

import csv
import json
import sys

from .adapters import create_ssh_tunnel, get_adapter
from .config import (
    AUTH_TYPE_LABELS,
    AuthType,
    ConnectionConfig,
    DATABASE_TYPE_LABELS,
    DatabaseType,
    load_connections,
    save_connections,
)


def cmd_connection_list(args) -> int:
    """List all saved connections."""
    connections = load_connections()
    if not connections:
        print("No saved connections.")
        return 0

    print(f"{'Name':<20} {'Type':<10} {'Connection Info':<40} {'Auth Type':<25}")
    print("-" * 95)
    for conn in connections:
        db_type_label = DATABASE_TYPE_LABELS.get(conn.get_db_type(), conn.db_type)
        # File-based databases (SQLite, DuckDB)
        if conn.db_type in ("sqlite", "duckdb"):
            conn_info = conn.file_path[:38] + ".." if len(conn.file_path) > 40 else conn.file_path
            auth_label = "N/A"
        # Server-based databases with simple auth
        elif conn.db_type in ("postgresql", "mysql", "mariadb", "oracle", "cockroachdb"):
            conn_info = f"{conn.server}@{conn.database}" if conn.database else conn.server
            conn_info = conn_info[:38] + ".." if len(conn_info) > 40 else conn_info
            auth_label = f"User: {conn.username}" if conn.username else "N/A"
        else:  # mssql (SQL Server)
            conn_info = f"{conn.server}@{conn.database}" if conn.database else conn.server
            conn_info = conn_info[:38] + ".." if len(conn_info) > 40 else conn_info
            auth_label = AUTH_TYPE_LABELS.get(conn.get_auth_type(), conn.auth_type)
        print(
            f"{conn.name:<20} {db_type_label:<10} {conn_info:<40} {auth_label:<25}"
        )
    return 0


def cmd_connection_create(args) -> int:
    """Create a new connection."""
    connections = load_connections()

    if any(c.name == args.name for c in connections):
        print(f"Error: Connection '{args.name}' already exists. Use 'edit' to modify it.")
        return 1

    # Determine database type
    db_type = getattr(args, "db_type", "mssql") or "mssql"
    try:
        DatabaseType(db_type)
    except ValueError:
        valid_types = ", ".join(t.value for t in DatabaseType)
        print(f"Error: Invalid database type '{db_type}'. Valid types: {valid_types}")
        return 1

    # File-based databases (SQLite, DuckDB)
    if db_type in ("sqlite", "duckdb"):
        file_path = getattr(args, "file_path", None)
        if not file_path:
            print(f"Error: --file-path is required for {db_type.upper()} connections.")
            return 1

        config = ConnectionConfig(
            name=args.name,
            db_type=db_type,
            file_path=file_path,
        )
    # Server-based databases with simple auth (PostgreSQL, MySQL, MariaDB, Oracle, CockroachDB)
    elif db_type in ("postgresql", "mysql", "mariadb", "oracle", "cockroachdb"):
        if not args.server:
            db_label = DATABASE_TYPE_LABELS.get(DatabaseType(db_type), db_type.upper())
            print(f"Error: --server is required for {db_label} connections.")
            return 1

        default_ports = {
            "postgresql": "5432",
            "mysql": "3306",
            "mariadb": "3306",
            "oracle": "1521",
            "cockroachdb": "26257",
        }
        config = ConnectionConfig(
            name=args.name,
            db_type=db_type,
            server=args.server,
            port=args.port or default_ports.get(db_type, "1433"),
            database=args.database or "",
            username=args.username or "",
            password=args.password or "",
            ssh_enabled=getattr(args, "ssh_enabled", False) or False,
            ssh_host=getattr(args, "ssh_host", "") or "",
            ssh_port=getattr(args, "ssh_port", "22") or "22",
            ssh_username=getattr(args, "ssh_username", "") or "",
            ssh_auth_type=getattr(args, "ssh_auth_type", "key") or "key",
            ssh_key_path=getattr(args, "ssh_key_path", "") or "",
            ssh_password=getattr(args, "ssh_password", "") or "",
        )
    else:
        # SQL Server connection (mssql)
        if not args.server:
            print("Error: --server is required for SQL Server connections.")
            return 1

        auth_type_str = getattr(args, "auth_type", "sql") or "sql"
        try:
            auth_type = AuthType(auth_type_str)
        except ValueError:
            valid_types = ", ".join(t.value for t in AuthType)
            print(f"Error: Invalid auth type '{auth_type_str}'. Valid types: {valid_types}")
            return 1

        config = ConnectionConfig(
            name=args.name,
            db_type=db_type,
            server=args.server,
            port=args.port or "1433",
            database=args.database or "",
            username=args.username or "",
            password=args.password or "",
            auth_type=auth_type.value,
            trusted_connection=(auth_type == AuthType.WINDOWS),
            ssh_enabled=getattr(args, "ssh_enabled", False) or False,
            ssh_host=getattr(args, "ssh_host", "") or "",
            ssh_port=getattr(args, "ssh_port", "22") or "22",
            ssh_username=getattr(args, "ssh_username", "") or "",
            ssh_auth_type=getattr(args, "ssh_auth_type", "key") or "key",
            ssh_key_path=getattr(args, "ssh_key_path", "") or "",
            ssh_password=getattr(args, "ssh_password", "") or "",
        )

    connections.append(config)
    save_connections(connections)
    print(f"Connection '{args.name}' created successfully.")
    return 0


def cmd_connection_edit(args) -> int:
    """Edit an existing connection."""
    connections = load_connections()

    conn_idx = None
    for i, c in enumerate(connections):
        if c.name == args.connection_name:
            conn_idx = i
            break

    if conn_idx is None:
        print(f"Error: Connection '{args.connection_name}' not found.")
        return 1

    conn = connections[conn_idx]

    if args.name:
        if args.name != conn.name and any(c.name == args.name for c in connections):
            print(f"Error: Connection '{args.name}' already exists.")
            return 1
        conn.name = args.name

    # SQL Server fields
    if args.server:
        conn.server = args.server
    if args.port:
        conn.port = args.port
    if args.database:
        conn.database = args.database
    if args.auth_type:
        try:
            auth_type = AuthType(args.auth_type)
            conn.auth_type = auth_type.value
            conn.trusted_connection = auth_type == AuthType.WINDOWS
        except ValueError:
            valid_types = ", ".join(t.value for t in AuthType)
            print(f"Error: Invalid auth type '{args.auth_type}'. Valid types: {valid_types}")
            return 1
    if args.username is not None:
        conn.username = args.username
    if args.password is not None:
        conn.password = args.password

    # SQLite fields
    file_path = getattr(args, "file_path", None)
    if file_path is not None:
        conn.file_path = file_path

    save_connections(connections)
    print(f"Connection '{conn.name}' updated successfully.")
    return 0


def cmd_connection_delete(args) -> int:
    """Delete a connection."""
    connections = load_connections()

    conn_idx = None
    for i, c in enumerate(connections):
        if c.name == args.connection_name:
            conn_idx = i
            break

    if conn_idx is None:
        print(f"Error: Connection '{args.connection_name}' not found.")
        return 1

    deleted = connections.pop(conn_idx)
    save_connections(connections)
    print(f"Connection '{deleted.name}' deleted successfully.")
    return 0


def cmd_query(args) -> int:
    """Execute a SQL query against a connection."""
    connections = load_connections()

    config = None
    for c in connections:
        if c.name == args.connection:
            config = c
            break

    if config is None:
        print(f"Error: Connection '{args.connection}' not found.")
        return 1

    # Override database if specified (only for SQL Server)
    if args.database and config.db_type == "mssql":
        config.database = args.database

    if args.query:
        query = args.query
    elif args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                query = f.read()
        except FileNotFoundError:
            print(f"Error: File '{args.file}' not found.")
            return 1
        except IOError as e:
            print(f"Error reading file: {e}")
            return 1
    else:
        print("Error: Either --query or --file must be provided.")
        return 1

    tunnel = None
    try:
        from dataclasses import replace

        # Create SSH tunnel if enabled
        tunnel, host, port = create_ssh_tunnel(config)
        if tunnel:
            connect_config = replace(config, server=host, port=str(port))
        else:
            connect_config = config

        adapter = get_adapter(config.db_type)
        db_conn = adapter.connect(connect_config)

        # Detect query type to avoid executing non-SELECT statements twice
        query_type = query.strip().upper().split()[0] if query.strip() else ""
        is_select_query = query_type in ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN", "PRAGMA")

        if is_select_query:
            columns, rows, _truncated = adapter.execute_query(db_conn, query)
        else:
            columns, rows = [], []

        if columns:
            if args.format == "csv":
                # Use proper CSV writer for correct quoting/escaping
                writer = csv.writer(sys.stdout)
                writer.writerow(columns)
                for row in rows:
                    writer.writerow(str(val) if val is not None else "" for val in row)
            elif args.format == "json":
                result = []
                for row in rows:
                    result.append(
                        dict(
                            zip(
                                columns,
                                [val if val is not None else None for val in row],
                            )
                        )
                    )
                print(json.dumps(result, indent=2, default=str))
            else:
                col_widths = [len(col) for col in columns]
                for row in rows:
                    for i, val in enumerate(row):
                        col_widths[i] = max(
                            col_widths[i], len(str(val) if val is not None else "NULL")
                        )

                header = " | ".join(
                    col.ljust(col_widths[i]) for i, col in enumerate(columns)
                )
                print(header)
                print("-" * len(header))

                for row in rows:
                    row_str = " | ".join(
                        (str(val) if val is not None else "NULL").ljust(col_widths[i])
                        for i, val in enumerate(row)
                    )
                    print(row_str)

            print(f"\n({len(rows)} row(s) returned)")
        else:
            affected = adapter.execute_non_query(db_conn, query)
            print(f"Query executed successfully. Rows affected: {affected}")

        db_conn.close()
        if tunnel:
            tunnel.stop()
        return 0

    except ImportError as e:
        print(f"Error: Required module not installed: {e}")
        if tunnel:
            tunnel.stop()
        return 1
    except Exception as e:
        print(f"Error: {e}")
        if tunnel:
            tunnel.stop()
        return 1
