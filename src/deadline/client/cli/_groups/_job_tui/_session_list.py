# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Session list TUI screen."""

from __future__ import annotations

from typing import Any

from rich.table import Table

from ._common import (
    clear_screen,
    console,
    copy_to_clipboard,
    format_short_id,
    format_time_ago,
    get_status_style,
    open_feedback_url,
    read_key,
    render_header,
    render_help_bar,
)


class SessionListTUI:
    """Session list for a specific task."""

    def __init__(
        self,
        farm_id: str,
        queue_id: str,
        job_id: str,
        step_id: str,
        task_id: str,
        task_label: str,
        deadline_client: Any,
    ) -> None:
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.job_id = job_id
        self.step_id = step_id
        self.task_id = task_id
        self.task_label = task_label
        self.deadline = deadline_client
        self.sessions: list[dict] = []
        self.cursor: int = 0
        self.message: str = ""
        self._needs_full_clear: bool = True

    def load_sessions(self) -> None:
        """Load sessions for this job, then filter to those that ran this task's step+task."""
        # List all sessions for the job
        all_sessions: list[dict] = []
        kwargs: dict = {
            "farmId": self.farm_id,
            "queueId": self.queue_id,
            "jobId": self.job_id,
        }
        while True:
            response = self.deadline.list_sessions(**kwargs)
            all_sessions.extend(response.get("sessions", []))
            next_token = response.get("nextToken")
            if not next_token:
                break
            kwargs["nextToken"] = next_token

        # For each session, check if it has actions for our step+task
        matching_sessions: list[dict] = []
        for session in all_sessions:
            session_id = session.get("sessionId", "")
            try:
                actions_kwargs: dict = {
                    "farmId": self.farm_id,
                    "queueId": self.queue_id,
                    "jobId": self.job_id,
                    "sessionId": session_id,
                }
                actions_response = self.deadline.list_session_actions(**actions_kwargs)
                for action in actions_response.get("sessionActions", []):
                    definition = action.get("definition", {})
                    task_run = definition.get("taskRun", {})
                    if (
                        task_run.get("stepId") == self.step_id
                        and task_run.get("taskId") == self.task_id
                    ):
                        matching_sessions.append(session)
                        break
            except Exception:
                # If we can't list actions for a session, skip it
                continue

        self.sessions = matching_sessions

    def render(self) -> None:
        """Render session list."""
        clear_screen(full=self._needs_full_clear)
        self._needs_full_clear = False
        render_header(f"Sessions for {self.task_label}", "")
        console.print()

        if not self.sessions:
            console.print("[dim italic]  No sessions found for this task[/dim italic]")
        else:
            table = Table(show_header=False, box=None, padding=(0, 1), width=console.width - 1)
            table.add_column("", width=3)
            table.add_column("Status", width=12, no_wrap=True)
            table.add_column("Session", width=20, no_wrap=True)
            table.add_column("Worker", width=20, no_wrap=True)
            table.add_column("Time", width=10, justify="right", no_wrap=True)

            for i, session in enumerate(self.sessions):
                self._add_session_row(table, i, session)
            console.print(table)

        if self.message:
            console.print(f"\n[yellow]{self.message}[/yellow]")
            self.message = ""

        console.print()
        render_help_bar(
            [
                ("↑↓", "nav"),
                ("c", "copy id"),
                ("f", "feedback"),
                ("Esc", "back"),
                ("q", "quit"),
            ]
        )

    def _add_session_row(self, table: Table, index: int, session: dict) -> None:
        """Add a single session row."""
        status = session.get("lifecycleStatus", "UNKNOWN")
        color, icon = get_status_style(status)
        session_id = session.get("sessionId", "")
        short_id = format_short_id(session_id)
        worker_id = session.get("workerId", "")
        short_worker = format_short_id(worker_id) if worker_id else ""
        started = session.get("startedAt")
        time_str = format_time_ago(started) if started else ""

        if index == self.cursor:
            table.add_row(
                "[bold cyan]▶[/bold cyan]",
                f"[bold {color}]{icon} {status}[/bold {color}]",
                f"[bold cyan]{short_id}[/bold cyan]",
                f"[bold cyan]{short_worker}[/bold cyan]",
                f"[bold cyan]{time_str}[/bold cyan]",
            )
        else:
            table.add_row(
                " ",
                f"[{color}]{icon} {status}[/{color}]",
                f"[dim]{short_id}[/dim]",
                f"[dim]{short_worker}[/dim]",
                f"[dim]{time_str}[/dim]",
            )

    def run(self) -> None:
        """Browse sessions. Esc returns to caller."""
        self.load_sessions()

        while True:
            self.render()
            key = read_key()

            if key == "up":
                self.cursor = max(0, self.cursor - 1)
            elif key == "down":
                self.cursor = min(len(self.sessions) - 1, self.cursor + 1) if self.sessions else 0
            elif key == "c" and self.sessions:
                session_id = self.sessions[self.cursor].get("sessionId", "")
                if copy_to_clipboard(session_id):
                    self.message = f"📋 Copied {session_id}"
                else:
                    self.message = f"📋 {session_id}"
            elif key in ("esc", "left"):
                return
            elif key == "f":
                self.message = open_feedback_url()
            elif key == "q":
                clear_screen()
                return
