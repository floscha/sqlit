"""Confirmation dialog screen."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import OptionList
from textual.widgets.option_list import Option

from ...widgets import Dialog


class ConfirmScreen(ModalScreen):
    """Modal screen for confirmation dialogs."""

    BINDINGS = [
        Binding("y", "confirm", "Yes"),
        Binding("n", "cancel", "No"),
        Binding("escape", "cancel", "Cancel"),
        Binding("enter", "select_option", "Select"),
    ]

    CSS = """
    ConfirmScreen {
        align: center middle;
        background: transparent;
    }

    #confirm-dialog {
        width: 36;
    }

    #confirm-list {
        height: auto;
        border: none;
    }

    #confirm-list > .option-list--option {
        padding: 0;
    }
    """

    def __init__(self, title: str):
        super().__init__()
        self.title_text = title

    def compose(self) -> ComposeResult:
        shortcuts = [("Yes", "Y"), ("No", "N"), ("Cancel", "Esc")]
        with Dialog(id="confirm-dialog", title=self.title_text, shortcuts=shortcuts):
            option_list = OptionList(
                Option("Yes", id="yes"),
                Option("No", id="no"),
                id="confirm-list",
            )
            yield option_list

    def on_mount(self) -> None:
        self.query_one("#confirm-list", OptionList).focus()

    def on_option_list_option_selected(self, event) -> None:
        self.dismiss(event.option.id == "yes")

    def action_select_option(self) -> None:
        option_list = self.query_one("#confirm-list", OptionList)
        if option_list.highlighted is not None:
            self.dismiss(
                option_list.get_option_at_index(option_list.highlighted).id == "yes"
            )

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)
