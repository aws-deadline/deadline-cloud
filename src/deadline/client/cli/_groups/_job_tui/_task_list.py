# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Task list TUI screen."""

from __future__ import annotations

from typing import Any, Optional

from rich.table import Table

from ._common import (
    clear_screen,
    console,
    copy_to_clipboard,
    format_short_id,
    get_status_style,
    get_terminal_page_size,
    open_feedback_url,
    read_key,
    render_header,
    render_help_bar,
)


class TaskListTUI:
    """Paginated task list browser for a step."""

    def __init__(
        self,
        farm_id: str,
        queue_id: str,
        job_id: str,
        job_name: str,
        step_id: str,
        step_name: str,
        deadline_client: Any,
    ) -> None:
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.job_id = job_id
        self.job_name = job_name
        self.step_id = step_id
        self.step_name = step_name
        self.deadline = deadline_client
        self.page_size = get_terminal_page_size()
        self.tasks: list[dict] = []
        self.cursor: int = 0
        self.next_token: Optional[str] = None
        self.prev_tokens: list[Optional[str]] = []
        self.page: int = 0
        self.message: str = ""
        self._needs_full_clear: bool = True

    def load_page(self) -> None:
        """Fetch one page of tasks via list_tasks."""
        kwargs: dict = {
            "farmId": self.farm_id,
            "queueId": self.queue_id,
            "jobId": self.job_id,
            "stepId": self.step_id,
            "maxResults": self.page_size,
        }
        token = self.prev_tokens[self.page] if self.page < len(self.prev_tokens) else None
        if token:
            kwargs["nextToken"] = token
        response = self.deadline.list_tasks(**kwargs)
        self.tasks = response.get("tasks", [])
        self.next_token = response.get("nextToken")

    @staticmethod
    def _format_task_params(task: dict) -> str:
        """Format task parameters as a summary string like 'Frame=1'."""
        params = task.get("parameters", {})
        if not params:
            return task.get("taskId", "")[-8:]
        parts: list[str] = []
        for key, value_dict in params.items():
            # value_dict is like {"int": "1"} or {"string": "foo"}
            val = next(iter(value_dict.values()), "")
            parts.append(f"{key}={val}")
        return ", ".join(parts)

    def render(self) -> None:
        """Render task list with status, target badge, params, ID."""
        clear_screen(full=self._needs_full_clear)
        self._needs_full_clear = False
        render_header(
            f"{self.job_name} › {self.step_name}",
            "",
        )
        console.print("[dim]📍 Tasks[/dim]\n")

        if not self.tasks:
            console.print("[dim italic]  No tasks found[/dim italic]")
        else:
            table = Table(show_header=False, box=None, padding=(0, 1), width=console.width - 1)
            table.add_column("", width=3)
            table.add_column("Status", width=22, no_wrap=True)
            table.add_column("Parameters", no_wrap=True, overflow="ellipsis")
            table.add_column("ID", width=14, justify="right", no_wrap=True)

            for i, task in enumerate(self.tasks):
                self._add_task_row(table, i, task)
            console.print(table)

        if self.message:
            console.print(f"\n[yellow]{self.message}[/yellow]")
            self.message = ""

        console.print()
        render_help_bar(
            [
                ("↑↓", "nav"),
                ("←/Esc", "back"),
                ("l", "sessions"),
                ("a", "attachments"),
                ("c", "copy id"),
                ("n/p", "page"),
                ("r", "refresh"),
                ("f", "feedback"),
                ("q", "quit"),
            ]
        )

    def _add_task_row(self, table: Table, index: int, task: dict) -> None:
        """Add a single task row to the table."""
        status = task.get("runStatus", "UNKNOWN")
        target = task.get("targetRunStatus", "")
        color, icon = get_status_style(status)
        param_summary = self._format_task_params(task)
        task_id = task.get("taskId", "")
        short_id = format_short_id(task_id)

        status_text = f"{icon} {status}"
        if target and target != status:
            target_color, _ = get_status_style(target)
            status_text += f" [{target_color}]→{target}[/{target_color}]"

        if index == self.cursor:
            table.add_row(
                "[bold cyan]▶[/bold cyan]",
                f"[bold {color}]{status_text}[/bold {color}]",
                f"[bold reverse] {param_summary} [/bold reverse]",
                f"[bold cyan]{short_id}[/bold cyan]",
            )
        else:
            table.add_row(
                " ",
                f"[{color}]{status_text}[/{color}]",
                param_summary,
                f"[dim]{short_id}[/dim]",
            )

    def run(self) -> Optional[tuple[str, str]]:
        """Main loop. Returns ('sessions', task_id), ('attachments', task_id), ('back', ''), or None."""
        self._needs_full_clear = True
        self.prev_tokens = [None]
        self.load_page()

        while True:
            self.render()
            key = read_key()

            if key == "up":
                if self.cursor > 0:
                    self.cursor -= 1
                elif self.page > 0:
                    self.page -= 1
                    self._needs_full_clear = True
                    self.load_page()
                    self.cursor = max(0, len(self.tasks) - 1)
            elif key == "down":
                if self.tasks and self.cursor < len(self.tasks) - 1:
                    self.cursor += 1
                elif self.tasks and self.next_token:
                    self.page += 1
                    self._needs_full_clear = True
                    if self.page >= len(self.prev_tokens):
                        self.prev_tokens.append(self.next_token)
                    self.cursor = 0
                    self.load_page()
            elif key in ("left", "esc"):
                return ("back", "")
            elif key == "l" and self.tasks:
                task_id = self.tasks[self.cursor].get("taskId", "")
                return ("sessions", task_id)
            elif key == "a" and self.tasks:
                task_id = self.tasks[self.cursor].get("taskId", "")
                return ("attachments", task_id)
            elif key == "c" and self.tasks:
                task_id = self.tasks[self.cursor].get("taskId", "")
                if copy_to_clipboard(task_id):
                    self.message = f"📋 Copied {task_id}"
                else:
                    self.message = f"📋 {task_id}"
            elif key == "n" and self.next_token:
                self.page += 1
                self._needs_full_clear = True
                if self.page >= len(self.prev_tokens):
                    self.prev_tokens.append(self.next_token)
                self.cursor = 0
                self.load_page()
            elif key == "p" and self.page > 0:
                self.page -= 1
                self._needs_full_clear = True
                self.cursor = 0
                self.load_page()
            elif key == "r":
                self._needs_full_clear = True
                self.prev_tokens = [None]
                self.page = 0
                self.cursor = 0
                self.load_page()
            elif key == "f":
                self.message = open_feedback_url()
            elif key == "q":
                clear_screen()
                return None
