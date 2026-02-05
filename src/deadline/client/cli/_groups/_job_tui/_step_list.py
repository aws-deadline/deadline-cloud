# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Step list TUI screen."""

from __future__ import annotations

from typing import Any, Optional

from rich.table import Table

from ._common import (
    clear_screen,
    console,
    copy_to_clipboard,
    format_short_id,
    get_lifecycle_badge,
    get_status_style,
    get_terminal_page_size,
    open_feedback_url,
    read_key,
    render_header,
    render_help_bar,
)


class StepListTUI:
    """Paginated step list browser for a job."""

    def __init__(
        self,
        farm_id: str,
        queue_id: str,
        job_id: str,
        job_name: str,
        job_status: str,
        deadline_client: Any,
    ) -> None:
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.job_id = job_id
        self.job_name = job_name
        self.job_status = job_status
        self.deadline = deadline_client
        self.page_size = get_terminal_page_size()
        self.steps: list[dict] = []
        self.cursor: int = 0
        self.next_token: Optional[str] = None
        self.prev_tokens: list[Optional[str]] = []
        self.page: int = 0
        self.message: str = ""
        self._needs_full_clear: bool = True

    def load_page(self) -> None:
        """Fetch one page of steps via list_steps."""
        kwargs: dict = {
            "farmId": self.farm_id,
            "queueId": self.queue_id,
            "jobId": self.job_id,
            "maxResults": self.page_size,
        }
        token = self.prev_tokens[self.page] if self.page < len(self.prev_tokens) else None
        if token:
            kwargs["nextToken"] = token
        response = self.deadline.list_steps(**kwargs)
        self.steps = response.get("steps", [])
        self.next_token = response.get("nextToken")

    def render(self) -> None:
        """Render step list with status, target badge, lifecycle badge, name, task count, ID."""
        clear_screen(full=self._needs_full_clear)
        self._needs_full_clear = False
        color, icon = get_status_style(self.job_status)
        render_header(self.job_name, f"[{color}]{icon} {self.job_status}[/{color}]")
        console.print("[dim]📍 Steps[/dim]\n")

        if not self.steps:
            console.print("[dim italic]  No steps found[/dim italic]")
        else:
            table = Table(show_header=False, box=None, padding=(0, 1), width=console.width - 1)
            table.add_column("", width=3)
            table.add_column("Status", width=26, no_wrap=True)
            table.add_column("Name", no_wrap=True, overflow="ellipsis")
            table.add_column("Tasks", width=12, justify="right", no_wrap=True)
            table.add_column("ID", width=14, justify="right", no_wrap=True)

            for i, step in enumerate(self.steps):
                self._add_step_row(table, i, step)
            console.print(table)

        if self.message:
            console.print(f"\n[yellow]{self.message}[/yellow]")
            self.message = ""

        console.print()
        render_help_bar(
            [
                ("↑↓", "nav"),
                ("→/Enter", "tasks"),
                ("←/Esc", "back"),
                ("c", "copy id"),
                ("n/p", "page"),
                ("r", "refresh"),
                ("f", "feedback"),
                ("q", "quit"),
            ]
        )

    def _add_step_row(self, table: Table, index: int, step: dict) -> None:
        """Add a single step row to the table."""
        status = step.get("taskRunStatus", "UNKNOWN")
        target = step.get("targetTaskRunStatus", "")
        lifecycle = step.get("lifecycleStatus", "CREATE_COMPLETE")
        color, icon = get_status_style(status)
        name = step.get("name", "Unnamed")
        step_id = step.get("stepId", "")
        short_id = format_short_id(step_id)

        # Task count from taskRunStatusCounts
        counts = step.get("taskRunStatusCounts", {})
        total_tasks = sum(counts.values())
        task_label = f"{total_tasks} task{'s' if total_tasks != 1 else ''}"

        # Build status text
        status_text = f"{icon} {status}"
        if target and target != status:
            target_color, _ = get_status_style(target)
            status_text += f" [{target_color}]→{target}[/{target_color}]"

        # Lifecycle badge
        badge = get_lifecycle_badge(lifecycle)
        if badge:
            badge_text, badge_color = badge
            name = f"{name} [{badge_color}]{badge_text}[/{badge_color}]"

        if index == self.cursor:
            table.add_row(
                "[bold cyan]▶[/bold cyan]",
                f"[bold {color}]{status_text}[/bold {color}]",
                f"[bold reverse] {name} [/bold reverse]",
                f"[bold cyan]{task_label}[/bold cyan]",
                f"[bold cyan]{short_id}[/bold cyan]",
            )
        else:
            table.add_row(
                " ",
                f"[{color}]{status_text}[/{color}]",
                name,
                f"[dim]{task_label}[/dim]",
                f"[dim]{short_id}[/dim]",
            )

    def run(self) -> Optional[tuple[str, ...]]:
        """Main loop. Returns ('select', step_id, step_name), ('back', ''), or None."""
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
                    self.cursor = max(0, len(self.steps) - 1)
            elif key == "down":
                if self.steps and self.cursor < len(self.steps) - 1:
                    self.cursor += 1
                elif self.steps and self.next_token:
                    self.page += 1
                    self._needs_full_clear = True
                    if self.page >= len(self.prev_tokens):
                        self.prev_tokens.append(self.next_token)
                    self.cursor = 0
                    self.load_page()
            elif key in ("right", "enter") and self.steps:
                step_id = self.steps[self.cursor].get("stepId", "")
                step_name = self.steps[self.cursor].get("name", "Unnamed")
                return ("select", step_id, step_name)
            elif key in ("left", "esc"):
                return ("back", "")
            elif key == "c" and self.steps:
                step_id = self.steps[self.cursor].get("stepId", "")
                if copy_to_clipboard(step_id):
                    self.message = f"📋 Copied {step_id}"
                else:
                    self.message = f"📋 {step_id}"
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
