"""Modal screens for sqlit.

This module re-exports from sqlit.ui.screens for backward compatibility.
New code should import directly from sqlit.ui.screens.
"""

# Re-export everything from the new location for backward compatibility
from .ui.screens import (
    ConfirmScreen,
    ConnectionScreen,
    DriverSetupScreen,
    HelpScreen,
    QueryHistoryScreen,
    ValueViewScreen,
)

__all__ = [
    "ConfirmScreen",
    "ConnectionScreen",
    "DriverSetupScreen",
    "HelpScreen",
    "QueryHistoryScreen",
    "ValueViewScreen",
]
