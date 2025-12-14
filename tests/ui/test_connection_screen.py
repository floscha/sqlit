"""UI tests for the ConnectionScreen using Textual Pilot."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from textual.app import App

from sqlit.config import ConnectionConfig
from sqlit.ui.screens import ConnectionScreen

SNAPSHOTS_DIR = Path(__file__).parent / "snapshots"


@pytest.fixture
def temp_db_file():
    """Create a temporary SQLite database file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        yield f.name
    Path(f.name).unlink(missing_ok=True)


class ConnectionScreenTestApp(App):
    """Test app wrapper for ConnectionScreen testing."""

    def __init__(self, config: ConnectionConfig | None = None, editing: bool = False):
        super().__init__()
        self._config = config
        self._editing = editing
        self.screen_result = None

    async def on_mount(self) -> None:
        screen = ConnectionScreen(self._config, editing=self._editing)
        await self.push_screen(screen, self._capture_result)

    def _capture_result(self, result) -> None:
        self.screen_result = result


class TestConnectionScreen:
    """Test creating connections via the ConnectionScreen."""

    @pytest.mark.asyncio
    async def test_create_mssql_connection(self):
        """Create an MSSQL connection (default type) and verify the config."""
        app = ConnectionScreenTestApp()

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen

            # Fill in the form (MSSQL is default)
            screen.query_one("#conn-name").value = "my-mssql"
            screen.query_one("#field-server").value = "localhost"
            screen.query_one("#field-port").value = "1433"
            screen.query_one("#field-database").value = "mydb"
            screen.query_one("#field-username").value = "sa"
            screen.query_one("#field-password").value = "secret"

            # Save
            await pilot.press("ctrl+s")
            await pilot.pause()

        # Verify the result
        assert app.screen_result is not None
        action, config = app.screen_result
        assert action == "save"
        assert config.name == "my-mssql"
        assert config.db_type == "mssql"
        assert config.server == "localhost"
        assert config.port == "1433"
        assert config.database == "mydb"
        assert config.username == "sa"
        assert config.password == "secret"

    @pytest.mark.asyncio
    async def test_edit_sqlite_connection(self, temp_db_file):
        """Edit an existing SQLite connection and verify changes."""
        original = ConnectionConfig(
            name="old-name",
            db_type="sqlite",
            file_path=temp_db_file,
        )
        app = ConnectionScreenTestApp(original, editing=True)

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen

            # Verify original values loaded
            assert screen.query_one("#conn-name").value == "old-name"
            assert screen.query_one("#field-file_path").value == temp_db_file

            # Change name (keep same file since it must exist)
            screen.query_one("#conn-name").value = "new-name"

            # Save
            await pilot.press("ctrl+s")
            await pilot.pause()

        # Verify the result
        assert app.screen_result is not None
        action, config = app.screen_result
        assert action == "save"
        assert config.name == "new-name"
        assert config.db_type == "sqlite"
        assert config.file_path == temp_db_file

    @pytest.mark.asyncio
    async def test_edit_postgresql_connection(self):
        """Edit an existing PostgreSQL connection and verify changes."""
        original = ConnectionConfig(
            name="prod-db",
            db_type="postgresql",
            server="old-server",
            port="5432",
            database="olddb",
            username="olduser",
            password="oldpass",
        )
        app = ConnectionScreenTestApp(original, editing=True)

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen

            # Verify original values loaded
            assert screen.query_one("#conn-name").value == "prod-db"
            assert screen.query_one("#field-server").value == "old-server"

            # Change values
            screen.query_one("#conn-name").value = "new-prod-db"
            screen.query_one("#field-server").value = "new-server"
            screen.query_one("#field-database").value = "newdb"

            # Save
            await pilot.press("ctrl+s")
            await pilot.pause()

        # Verify the result
        assert app.screen_result is not None
        action, config = app.screen_result
        assert action == "save"
        assert config.name == "new-prod-db"
        assert config.db_type == "postgresql"
        assert config.server == "new-server"
        assert config.database == "newdb"

    @pytest.mark.asyncio
    async def test_cancel_connection(self):
        """Cancel returns None."""
        app = ConnectionScreenTestApp()

        async with app.run_test(size=(100, 35)) as pilot:
            await pilot.press("escape")

        assert app.screen_result is None
