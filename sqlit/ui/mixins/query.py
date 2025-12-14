"""Query execution mixin for SSMSTUI."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from textual.timer import Timer
from textual.widgets import DataTable, TextArea
from textual.worker import Worker

if TYPE_CHECKING:
    from ...config import ConnectionConfig
    from ...widgets import VimMode

# Spinner frames for loading animation
SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]


class QueryMixin:
    """Mixin providing query execution functionality."""

    # These attributes are defined in the main app class
    current_connection: Any
    current_config: "ConnectionConfig | None"
    current_adapter: Any
    vim_mode: "VimMode"
    _last_result_columns: list[str]
    _last_result_rows: list[tuple]
    _last_result_row_count: int
    _query_worker: Worker | None
    _query_executing: bool
    _spinner_index: int
    _spinner_timer: Timer | None

    def action_execute_query(self) -> None:
        """Execute the current query."""
        self._execute_query_common(keep_insert_mode=False)

    def action_execute_query_insert(self) -> None:
        """Execute query in INSERT mode without leaving it."""
        self._execute_query_common(keep_insert_mode=True)

    def _execute_query_common(self, keep_insert_mode: bool) -> None:
        """Common query execution logic."""
        if not self.current_connection or not self.current_adapter:
            self.notify("Not connected to a database", severity="warning")
            return

        query_input = self.query_one("#query-input", TextArea)
        query = query_input.text.strip()

        if not query:
            self.notify("No query to execute", severity="warning")
            return

        # Cancel any existing query worker
        if hasattr(self, "_query_worker") and self._query_worker is not None:
            self._query_worker.cancel()

        results_table = self.query_one("#results-table", DataTable)
        results_table.clear(columns=True)
        results_table.add_column("Status")
        results_table.add_row("Executing query...")

        # Start spinner animation
        self._start_query_spinner()

        # Run query in background thread
        self._query_worker = self.run_worker(
            self._run_query_async(query, keep_insert_mode),
            name="query_execution",
            exclusive=True,
        )

    def _start_query_spinner(self) -> None:
        """Start the query execution spinner animation."""
        self._query_executing = True
        self._spinner_index = 0
        self._update_status_bar()
        # Start timer to animate spinner
        if hasattr(self, "_spinner_timer") and self._spinner_timer is not None:
            self._spinner_timer.stop()
        self._spinner_timer = self.set_interval(0.1, self._animate_spinner)

    def _stop_query_spinner(self) -> None:
        """Stop the query execution spinner animation."""
        self._query_executing = False
        if hasattr(self, "_spinner_timer") and self._spinner_timer is not None:
            self._spinner_timer.stop()
            self._spinner_timer = None
        self._update_status_bar()

    def _animate_spinner(self) -> None:
        """Update spinner animation frame."""
        if not self._query_executing:
            return
        self._spinner_index = (self._spinner_index + 1) % len(SPINNER_FRAMES)
        self._update_status_bar()

    async def _run_query_async(self, query: str, keep_insert_mode: bool) -> None:
        """Run query asynchronously in a worker thread."""
        import asyncio

        adapter = self.current_adapter
        connection = self.current_connection
        config = self.current_config

        query_type = query.strip().upper().split()[0] if query.strip() else ""
        is_select_query = query_type in (
            "SELECT",
            "WITH",
            "SHOW",
            "DESCRIBE",
            "EXPLAIN",
            "PRAGMA",
        )

        try:
            if is_select_query:
                # Run blocking DB call in thread pool
                max_fetch_rows = 10000
                columns, rows, truncated = await asyncio.to_thread(
                    adapter.execute_query, connection, query, max_fetch_rows
                )
                row_count = len(rows)

                # Update UI (we're back on main thread after await)
                self._display_query_results(columns, list(rows), row_count, truncated)
            else:
                affected = await asyncio.to_thread(
                    adapter.execute_non_query, connection, query
                )

                # Update UI
                self._display_non_query_result(affected)

            # Save to history
            if config:
                from ...config import save_query_to_history
                save_query_to_history(config.name, query)

            if keep_insert_mode:
                self._restore_insert_mode()

        except Exception as e:
            self._display_query_error(str(e))
        finally:
            # Always stop the spinner when done
            self._stop_query_spinner()

    def _display_query_results(
        self, columns: list[str], rows: list[tuple], row_count: int, truncated: bool
    ) -> None:
        """Display query results in the results table (called on main thread)."""
        self._last_result_columns = columns
        self._last_result_rows = rows
        self._last_result_row_count = row_count

        results_table = self.query_one("#results-table", DataTable)
        results_table.clear(columns=True)
        results_table.add_columns(*columns)

        # Only display first 1000 rows in the table
        for row in rows[:1000]:
            str_row = tuple(str(v) if v is not None else "NULL" for v in row)
            results_table.add_row(*str_row)

        if truncated:
            self.notify(f"Query returned {row_count}+ rows (truncated)", severity="warning")
        else:
            self.notify(f"Query returned {row_count} rows")

    def _display_non_query_result(self, affected: int) -> None:
        """Display non-query result (called on main thread)."""
        self._last_result_columns = ["Result"]
        self._last_result_rows = [(f"{affected} row(s) affected",)]
        self._last_result_row_count = 1

        results_table = self.query_one("#results-table", DataTable)
        results_table.clear(columns=True)
        results_table.add_column("Result")
        results_table.add_row(f"{affected} row(s) affected")
        self.notify(f"Query executed: {affected} row(s) affected")

    def _display_query_error(self, error_message: str) -> None:
        """Display query error (called on main thread)."""
        self._last_result_columns = ["Error"]
        self._last_result_rows = [(error_message,)]
        self._last_result_row_count = 1

        results_table = self.query_one("#results-table", DataTable)
        results_table.clear(columns=True)
        results_table.add_column("Error")
        results_table.add_row(error_message)
        self.notify(f"Query error: {error_message}", severity="error")

    def _restore_insert_mode(self) -> None:
        """Restore INSERT mode after query execution (called on main thread)."""
        from ...widgets import VimMode

        query_input = self.query_one("#query-input", TextArea)
        self.vim_mode = VimMode.INSERT
        query_input.read_only = False
        query_input.focus()
        self._update_footer_bindings()
        self._update_status_bar()

    def action_cancel_query(self) -> None:
        """Cancel the currently running query."""
        if not getattr(self, "_query_executing", False):
            self.notify("No query running")
            return

        if hasattr(self, "_query_worker") and self._query_worker is not None:
            self._query_worker.cancel()
            self._query_worker = None

        self._stop_query_spinner()

        # Update results table to show cancelled state
        results_table = self.query_one("#results-table", DataTable)
        results_table.clear(columns=True)
        results_table.add_column("Status")
        results_table.add_row("Query cancelled")

        self.notify("Query cancelled", severity="warning")

    def action_cancel_operation(self) -> None:
        """Cancel any running operation (query or schema indexing)."""
        cancelled = False

        # Cancel query if running
        if getattr(self, "_query_executing", False):
            if hasattr(self, "_query_worker") and self._query_worker is not None:
                self._query_worker.cancel()
                self._query_worker = None
            self._stop_query_spinner()

            # Update results table to show cancelled state
            results_table = self.query_one("#results-table", DataTable)
            results_table.clear(columns=True)
            results_table.add_column("Status")
            results_table.add_row("Query cancelled")
            cancelled = True

        # Cancel schema indexing if running
        if getattr(self, "_schema_indexing", False):
            if hasattr(self, "_schema_worker") and self._schema_worker is not None:
                self._schema_worker.cancel()
                self._schema_worker = None
            self._stop_schema_spinner()
            cancelled = True

        if cancelled:
            self.notify("Operation cancelled", severity="warning")
        else:
            self.notify("No operation running")

    def action_clear_query(self) -> None:
        """Clear the query input."""
        query_input = self.query_one("#query-input", TextArea)
        query_input.text = ""

    def action_new_query(self) -> None:
        """Start a new query (clear input and results)."""
        query_input = self.query_one("#query-input", TextArea)
        query_input.text = ""
        results_table = self.query_one("#results-table", DataTable)
        results_table.clear(columns=True)

    def action_show_history(self) -> None:
        """Show query history for the current connection."""
        if not self.current_config:
            self.notify("Not connected to a database", severity="warning")
            return

        from ...config import load_query_history
        from ..screens import QueryHistoryScreen

        history = load_query_history(self.current_config.name)
        self.push_screen(
            QueryHistoryScreen(history, self.current_config.name),
            self._handle_history_result,
        )

    def _handle_history_result(self, result) -> None:
        """Handle the result from the history screen."""
        if result is None:
            return

        action, data = result
        if action == "select":
            query_input = self.query_one("#query-input", TextArea)
            query_input.text = data
        elif action == "delete":
            self._delete_history_entry(data)
            self.action_show_history()

    def _delete_history_entry(self, timestamp: str) -> None:
        """Delete a specific history entry by timestamp."""
        from ...config import delete_query_from_history

        delete_query_from_history(self.current_config.name, timestamp)
