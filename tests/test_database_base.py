"""Base test class for parameterized database integration tests.

This module provides a base class with common test cases that can be
parameterized across different database types, reducing test duplication.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class DatabaseTestConfig:
    """Configuration for a database test suite."""

    db_type: str  # e.g., "sqlite", "postgresql"
    display_name: str  # e.g., "SQLite", "PostgreSQL"
    connection_fixture: str  # Name of the connection fixture
    db_fixture: str  # Name of the database fixture
    # Connection creation args (as fixture function)
    create_connection_args: Callable[..., list[str]]
    # Whether this DB uses LIMIT syntax (False for MSSQL TOP, Oracle FETCH FIRST)
    uses_limit: bool = True


class BaseDatabaseTests(ABC):
    """Base class for database integration tests.

    Subclasses must define the `config` class attribute with a DatabaseTestConfig.
    """

    @property
    @abstractmethod
    def config(self) -> DatabaseTestConfig:
        """Return the database test configuration."""
        pass

    def test_list_connections_shows_db(self, request, cli_runner):
        """Test that connection list shows connections correctly."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner("connection", "list")
        assert result.returncode == 0
        assert connection in result.stdout
        assert self.config.display_name in result.stdout

    def test_query_select(self, request, cli_runner):
        """Test executing SELECT query."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "SELECT * FROM test_users ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "Charlie" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_with_where(self, request, cli_runner):
        """Test executing SELECT with WHERE clause."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "SELECT name, email FROM test_users WHERE id = 1",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "alice@example.com" in result.stdout
        assert "1 row(s) returned" in result.stdout

    def test_query_json_format(self, request, cli_runner):
        """Test query output in JSON format."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "SELECT id, name FROM test_users ORDER BY id LIMIT 2",
            "--format", "json",
        )
        assert result.returncode == 0

        # Parse JSON output (exclude the row count message)
        lines = result.stdout.strip().split("\n")
        json_output = "\n".join(lines[:-1])
        data = json.loads(json_output)

        assert len(data) == 2
        assert data[0]["name"] == "Alice"
        assert data[1]["name"] == "Bob"

    def test_query_csv_format(self, request, cli_runner):
        """Test query output in CSV format."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "SELECT id, name FROM test_users ORDER BY id LIMIT 2",
            "--format", "csv",
        )
        assert result.returncode == 0
        assert "id,name" in result.stdout
        assert "1,Alice" in result.stdout
        assert "2,Bob" in result.stdout

    def test_query_view(self, request, cli_runner):
        """Test querying a view."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "SELECT * FROM test_user_emails ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_aggregate(self, request, cli_runner):
        """Test aggregate query."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "SELECT COUNT(*) as user_count FROM test_users",
        )
        assert result.returncode == 0
        assert "3" in result.stdout

    def test_query_insert(self, request, cli_runner):
        """Test INSERT statement."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "INSERT INTO test_users (id, name, email) VALUES (4, 'David', 'david@example.com')",
        )
        assert result.returncode == 0

        # Verify the insert
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "SELECT * FROM test_users WHERE id = 4",
        )
        assert "David" in result.stdout


class BaseDatabaseTestsWithLimit(BaseDatabaseTests):
    """Base tests for databases that support LIMIT syntax."""

    def test_query_limit(self, request, cli_runner):
        """Test query with LIMIT clause."""
        connection = request.getfixturevalue(self.config.connection_fixture)
        result = cli_runner(
            "query",
            "-c", connection,
            "-q", "SELECT * FROM test_users ORDER BY id LIMIT 2",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "2 row(s) returned" in result.stdout
