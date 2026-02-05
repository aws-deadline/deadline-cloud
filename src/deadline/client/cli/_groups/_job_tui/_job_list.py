# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Job list TUI screen."""

from __future__ import annotations

from typing import Any, Optional

from rich.table import Table

from ._common import (
    clear_screen,
    console,
    copy_to_clipboard,
    format_short_id,
    format_time_ago,
    get_status_style,
    get_terminal_page_size,
    open_feedback_url,
    read_key,
    render_header,
    render_help_bar,
)


class JobListTUI:
    """Paginated job list browser."""

    def __init__(self, farm_id: str, queue_id: str, deadline_client: Any) -> None:
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.deadline = deadline_client
        self.page_size = get_terminal_page_size()
        self.jobs: list[dict] = []
        self.cursor: int = 0
        self.page: int = 0
        self.total_jobs: int = 0
        self.message: str = ""
        self._needs_full_clear: bool = True

    def load_page(self) -> None:
        """Fetch one page of jobs via search_jobs."""
        response = self.deadline.search_jobs(
            farmId=self.farm_id,
            queueIds=[self.queue_id],
            itemOffset=self.page * self.page_size,
            pageSize=self.page_size,
            sortExpressions=[{"fieldSort": {"name": "CREATED_AT", "sortOrder": "DESCENDING"}}],
        )
        self.jobs = response.get("jobs", [])
        self.total_jobs = response.get("totalResults", 0)

    def render(self) -> None:
        """Render job list with status, target badge, name, time, short ID."""
        clear_screen(full=self._needs_full_clear)
        self._needs_full_clear = False
        render_header("Jobs", f"Queue: {format_short_id(self.queue_id)}")
        console.print()

        if not self.jobs:
            console.print("[dim italic]  No jobs found[/dim italic]")
        else:
            table = Table(show_header=False, box=None, padding=(0, 1), width=console.width - 1)
            table.add_column("", width=3)
            table.add_column("Status", width=22, no_wrap=True)
            table.add_column("Name", no_wrap=True, overflow="ellipsis")
            table.add_column("Time", width=10, justify="right", no_wrap=True)
            table.add_column("ID", width=14, justify="right", no_wrap=True)

            for i, job in enumerate(self.jobs):
                self._add_job_row(table, i, job)
            console.print(table)

        # Pagination info
        total_pages = max(1, (self.total_jobs + self.page_size - 1) // self.page_size)
        start = self.page * self.page_size + 1
        end = min(start + len(self.jobs) - 1, self.total_jobs)
        console.print(
            f"\n[dim]Page {self.page + 1}/{total_pages} ({start}-{end} of {self.total_jobs})[/dim]"
        )

        if self.message:
            console.print(f"\n[yellow]{self.message}[/yellow]")
            self.message = ""

        console.print()
        render_help_bar(
            [
                ("↑↓", "nav"),
                ("→/Enter", "steps"),
                ("a", "attachments"),
                ("c", "copy id"),
                ("n/p", "page"),
                ("r", "refresh"),
                ("f", "feedback"),
                ("q", "quit"),
            ]
        )

    def _add_job_row(self, table: Table, index: int, job: dict) -> None:
        """Add a single job row to the table."""
        status = job.get("taskRunStatus", "UNKNOWN")
        target = job.get("targetTaskRunStatus", "")
        color, icon = get_status_style(status)
        name = job.get("name", job.get("displayName", "Unnamed"))
        created = job.get("createdAt")
        time_str = format_time_ago(created) if created else ""
        job_id = job.get("jobId", "")
        short_id = format_short_id(job_id)

        # Build status text with optional target badge
        status_text = f"{icon} {status}"
        if target and target != status:
            target_color, _ = get_status_style(target)
            status_text += f" [{target_color}]→{target}[/{target_color}]"

        if index == self.cursor:
            table.add_row(
                "[bold cyan]▶[/bold cyan]",
                f"[bold {color}]{status_text}[/bold {color}]",
                f"[bold reverse] {name} [/bold reverse]",
                f"[bold cyan]{time_str}[/bold cyan]",
                f"[bold cyan]{short_id}[/bold cyan]",
            )
        else:
            table.add_row(
                " ",
                f"[{color}]{status_text}[/{color}]",
                name,
                f"[dim]{time_str}[/dim]",
                f"[dim]{short_id}[/dim]",
            )

    def run(self) -> Optional[tuple[str, str]]:
        """Main loop. Returns ('select', job_id), ('attachments', job_id), or None."""
        self._needs_full_clear = True
        self.load_page()

        while True:
            self.render()
            key = read_key()

            if key == "up":
                if self.cursor > 0:
                    self.cursor -= 1
                elif self.page > 0:
                    # Wrap to previous page, cursor at bottom
                    self.page -= 1
                    self._needs_full_clear = True
                    self.load_page()
                    self.cursor = max(0, len(self.jobs) - 1)
            elif key == "down":
                if self.jobs and self.cursor < len(self.jobs) - 1:
                    self.cursor += 1
                elif self.jobs:
                    # Wrap to next page, cursor at top
                    total_pages = max(1, (self.total_jobs + self.page_size - 1) // self.page_size)
                    if self.page < total_pages - 1:
                        self.page += 1
                        self._needs_full_clear = True
                        self.cursor = 0
                        self.load_page()
            elif key in ("right", "enter") and self.jobs:
                job_id = self.jobs[self.cursor].get("jobId", "")
                return ("select", job_id)
            elif key == "a" and self.jobs:
                job_id = self.jobs[self.cursor].get("jobId", "")
                return ("attachments", job_id)
            elif key == "c" and self.jobs:
                job_id = self.jobs[self.cursor].get("jobId", "")
                if copy_to_clipboard(job_id):
                    self.message = f"📋 Copied {job_id}"
                else:
                    self.message = f"📋 {job_id}"
            elif key == "n":
                total_pages = max(1, (self.total_jobs + self.page_size - 1) // self.page_size)
                if self.page < total_pages - 1:
                    self.page += 1
                    self._needs_full_clear = True
                    self.cursor = 0
                    self.load_page()
            elif key == "p":
                if self.page > 0:
                    self.page -= 1
                    self._needs_full_clear = True
                    self.cursor = 0
                    self.load_page()
            elif key == "r":
                self._needs_full_clear = True
                self.load_page()
                self.cursor = 0
            elif key == "f":
                self.message = open_feedback_url()
            elif key == "q":
                clear_screen()
                return None
