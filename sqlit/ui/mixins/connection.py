"""Connection management mixin for SSMSTUI."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from textual.widgets import Static

if TYPE_CHECKING:
    from ...config import ConnectionConfig


class ConnectionMixin:
    """Mixin providing connection management functionality."""

    # These attributes are defined in the main app class
    connections: list
    current_connection: Any
    current_config: "ConnectionConfig | None"
    current_adapter: Any
    current_ssh_tunnel: Any
    _connection_health: dict[str, bool]

    def _set_connection_health(self, name: str, ok: bool | None) -> None:
        """Record a connection health status to affect tree coloring."""
        if ok is None:
            self._connection_health.pop(name, None)
            return
        self._connection_health[name] = ok

    def _apply_connection_health(self, name: str, ok: bool | None) -> None:
        """Apply connection health update and refresh tree."""
        self._set_connection_health(name, ok)
        try:
            self.refresh_tree()
        except Exception:
            pass

    def action_test_connections(self) -> None:
        """Test all configured connections and mark failures in the tree."""
        self.notify("Testing connections…")
        self._test_connection_health()

    def _test_connection_health(self) -> None:
        """Test all configured connections in the background and mark failures."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from ...db import get_adapter

        connections = list(self.connections)

        def test_one(config: "ConnectionConfig") -> tuple[str, bool | None]:
            if (
                getattr(config, "db_type", "") == "mssql"
                and getattr(config, "auth_type", "") == "ad_interactive"
            ):
                return config.name, None

            try:
                adapter = get_adapter(config.db_type)
                conn = adapter.connect(config)
                try:
                    close = getattr(conn, "close", None)
                    if callable(close):
                        close()
                except Exception:
                    pass
                return config.name, True
            except (ModuleNotFoundError, ImportError):
                return config.name, None
            except Exception:
                return config.name, False

        def work() -> None:
            max_workers = min(32, max(1, len(connections)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(test_one, c) for c in connections]
                for future in as_completed(futures):
                    try:
                        name, ok = future.result()
                    except Exception:
                        continue
                    self.call_from_thread(self._apply_connection_health, name, ok)

        self.run_worker(
            work,
            group="connection-health",
            name="connection-health",
            description="Testing connections",
            thread=True,
            exclusive=True,
        )

    def connect_to_server(self, config: "ConnectionConfig") -> None:
        """Connect to a database."""
        from dataclasses import replace

        from ...db import create_ssh_tunnel, get_adapter

        # Check for pyodbc only if it's a SQL Server connection
        try:
            import pyodbc
            PYODBC_AVAILABLE = True
        except ImportError:
            PYODBC_AVAILABLE = False

        if config.db_type == "mssql" and not PYODBC_AVAILABLE:
            self.notify("pyodbc not installed. Run: pip install pyodbc", severity="error")
            return

        try:
            # Close any existing SSH tunnel
            if self.current_ssh_tunnel:
                try:
                    self.current_ssh_tunnel.stop()
                except Exception:
                    pass
                self.current_ssh_tunnel = None

            # Create SSH tunnel if enabled
            tunnel, host, port = create_ssh_tunnel(config)
            self.current_ssh_tunnel = tunnel

            # If SSH tunnel was created, use the tunnel's local address
            if tunnel:
                connect_config = replace(config, server=host, port=str(port))
            else:
                connect_config = config

            adapter = get_adapter(config.db_type)
            self.current_connection = adapter.connect(connect_config)
            self.current_config = config  # Store original config (not tunneled)
            self.current_adapter = adapter
            self._set_connection_health(config.name, True)

            status = self.query_one("#status-bar", Static)
            display_info = config.get_display_info()
            ssh_indicator = " [SSH]" if tunnel else ""
            status.update(f"[#90EE90]Connected to {config.name}[/] ({display_info}){ssh_indicator}")

            self.refresh_tree()
            self._load_schema_cache()
            self.notify(f"Connected to {config.name}")

        except Exception as e:
            # Clean up SSH tunnel on failure
            if self.current_ssh_tunnel:
                try:
                    self.current_ssh_tunnel.stop()
                except Exception:
                    pass
                self.current_ssh_tunnel = None
            self._set_connection_health(config.name, False)
            self.refresh_tree()
            self.notify(f"Connection failed: {e}", severity="error")

    def _disconnect_silent(self) -> None:
        """Disconnect from current database without notification."""
        if self.current_connection:
            try:
                self.current_connection.close()
            except Exception:
                pass
            self.current_connection = None
            self.current_config = None
            self.current_adapter = None

        # Close SSH tunnel if active
        if self.current_ssh_tunnel:
            try:
                self.current_ssh_tunnel.stop()
            except Exception:
                pass
            self.current_ssh_tunnel = None

    def action_disconnect(self) -> None:
        """Disconnect from current database."""
        if self.current_connection:
            self._disconnect_silent()

            status = self.query_one("#status-bar", Static)
            status.update("Disconnected")

            self.refresh_tree()
            self.notify("Disconnected")

    def action_new_connection(self) -> None:
        """Show new connection dialog."""
        from ..screens import ConnectionScreen

        self._set_connection_screen_footer()
        self.push_screen(ConnectionScreen(), self._wrap_connection_result)

    def action_edit_connection(self) -> None:
        """Edit the selected connection."""
        from textual.widgets import Tree

        from ..screens import ConnectionScreen

        tree = self.query_one("#object-tree", Tree)
        node = tree.cursor_node

        if not node or not node.data:
            return

        data = node.data
        if data[0] != "connection":
            return

        config = data[1]
        self._set_connection_screen_footer()
        self.push_screen(
            ConnectionScreen(config, editing=True), self._wrap_connection_result
        )

    def _set_connection_screen_footer(self) -> None:
        """Set footer bindings for connection screen."""
        from ...widgets import ContextFooter

        try:
            footer = self.query_one(ContextFooter)
        except Exception:
            return
        footer.set_bindings([], [])

    def _wrap_connection_result(self, result: tuple | None) -> None:
        """Wrapper to restore footer after connection dialog."""
        self._update_footer_bindings()
        self.handle_connection_result(result)

    def handle_connection_result(self, result: tuple | None) -> None:
        """Handle result from connection dialog."""
        from ...config import save_connections

        if not result:
            return

        action, config = result

        if action == "save":
            self.connections = [c for c in self.connections if c.name != config.name]
            self.connections.append(config)
            save_connections(self.connections)
            self.refresh_tree()
            self.notify(f"Connection '{config.name}' saved")

    def action_delete_connection(self) -> None:
        """Delete the selected connection."""
        from textual.widgets import Tree

        from ..screens import ConfirmScreen
        from ...config import ConnectionConfig

        tree = self.query_one("#object-tree", Tree)
        node = tree.cursor_node

        if not node or not node.data:
            return

        data = node.data
        if data[0] != "connection":
            return

        config = data[1]

        if self.current_config and self.current_config.name == config.name:
            self.notify("Disconnect first before deleting", severity="warning")
            return

        self.push_screen(
            ConfirmScreen(f"Delete '{config.name}'?"),
            lambda confirmed: self._do_delete_connection(config) if confirmed else None,
        )

    def _do_delete_connection(self, config: "ConnectionConfig") -> None:
        """Actually delete the connection after confirmation."""
        from ...config import save_connections

        self.connections = [c for c in self.connections if c.name != config.name]
        save_connections(self.connections)
        self.refresh_tree()
        self.notify(f"Connection '{config.name}' deleted")

    def action_connect_selected(self) -> None:
        """Connect to the selected connection."""
        from textual.widgets import Tree

        tree = self.query_one("#object-tree", Tree)
        node = tree.cursor_node

        if not node or not node.data:
            return

        data = node.data
        if data[0] == "connection":
            config = data[1]
            if self.current_config and self.current_config.name == config.name:
                return
            if self.current_connection:
                self._disconnect_silent()
            self.connect_to_server(config)
