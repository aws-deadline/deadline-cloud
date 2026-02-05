# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Attachment browser TUI screen — wraps existing browse_group manifest logic."""

from __future__ import annotations

import os
from typing import Any, Optional

from rich.spinner import Spinner
from rich.live import Live

from ._common import (
    clear_screen,
    console,
    get_status_style,
    open_feedback_url,
    read_key,
    render_header,
    render_help_bar,
)

# Import tree/manifest types and functions from _browse_tui
from .._browse_tui import (
    NodeType,
    TreeNode,
    download_folder,
    download_single_file,
    is_image,
    load_input_manifests,
    load_output_manifests,
    open_image_viewer,
    preview_file_content,
    render_breadcrumb,
    render_file_list,
    show_file_info,
    show_manifest_list,
)

from deadline.client.api._session import get_default_client_config
from deadline.job_attachments.models import (
    S3_MANIFEST_FOLDER_NAME,
    JobAttachmentS3Settings,
)


class AttachmentBrowserTUI:
    """File tree browser for job/task attachments. Esc returns to caller."""

    def __init__(
        self,
        farm_id: str,
        queue_id: str,
        job_id: str,
        job_name: str,
        job_status: str,
        boto3_session: Any,
        queue_role_session: Any,
        s3_settings: JobAttachmentS3Settings,
        step_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> None:
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.job_id = job_id
        self.job_name = job_name
        self.job_status = job_status
        self.boto3_session = boto3_session
        self.queue_role_session = queue_role_session
        self.s3_settings = s3_settings
        self.step_id = step_id
        self.task_id = task_id
        self.root = TreeNode(name="Job", node_type=NodeType.ROOT)
        self.current_node = self.root
        self.cursor: int = 0
        self.message: str = ""
        self._needs_full_clear: bool = True

    def load_manifests(self) -> None:
        """Load input and output manifests into tree structure."""
        deadline = self.boto3_session.client("deadline", config=get_default_client_config())
        job = deadline.get_job(farmId=self.farm_id, queueId=self.queue_id, jobId=self.job_id)
        input_node = TreeNode(name="input", node_type=NodeType.CATEGORY, parent=self.root)
        output_node = TreeNode(name="output", node_type=NodeType.CATEGORY, parent=self.root)
        self.root.children = [input_node, output_node]

        s3_prefix = f"{self.s3_settings.rootPrefix}/{S3_MANIFEST_FOLDER_NAME}"

        # Input manifests are always job-level
        for tree in load_input_manifests(
            job, s3_prefix, self.s3_settings.s3BucketName, self.queue_role_session
        ):
            tree.parent = input_node
            input_node.children.append(tree)

        # Output manifests: scoped to step/task if provided
        output_kwargs: dict = {
            "s3_settings": self.s3_settings,
            "farm_id": self.farm_id,
            "queue_id": self.queue_id,
            "job_id": self.job_id,
            "session": self.queue_role_session,
        }
        for tree in load_output_manifests(**output_kwargs):
            tree.parent = output_node
            output_node.children.append(tree)

    def render(self) -> None:
        """Render the file tree browser."""
        clear_screen(full=self._needs_full_clear)
        self._needs_full_clear = False
        color, icon = get_status_style(self.job_status)
        render_header(self.job_name, f"[{color}]{icon} {self.job_status}[/{color}]")
        render_breadcrumb(self.current_node)
        render_file_list(self.current_node.children, self.cursor)

        if self.message:
            console.print(f"\n[yellow]{self.message}[/yellow]")
            self.message = ""

        console.print()
        render_help_bar(
            [
                ("←→↑↓", "nav"),
                ("Enter", "open"),
                ("d", "download"),
                ("i", "info"),
                ("v", "view"),
                ("m", "manifests"),
                ("f", "feedback"),
                ("Esc", "back"),
                ("q", "quit"),
            ]
        )

    def _handle_download(self, node: TreeNode) -> None:
        """Prompt for destination and download."""
        dest = console.input("[bold]Download to:[/bold] ") or os.getcwd()
        os.makedirs(dest, exist_ok=True)
        if node.node_type == NodeType.FILE:
            path = download_single_file(self.queue_role_session, self.s3_settings, node, dest)
            self.message = f"✅ Downloaded to {path}"
        else:
            count = download_folder(self.queue_role_session, self.s3_settings, node, dest)
            self.message = f"✅ Downloaded {count} files to {dest}"

    def run(self) -> None:
        """File tree browser. Esc returns to caller."""
        with Live(Spinner("dots", text="Loading manifests..."), console=console, transient=True):
            self.load_manifests()

        while True:
            self.render()
            items = self.current_node.children
            key = read_key()

            if key == "up":
                self.cursor = max(0, self.cursor - 1)
            elif key == "down":
                self.cursor = min(len(items) - 1, self.cursor + 1) if items else 0
            elif key == "left":
                if self.current_node.parent:
                    self.current_node = self.current_node.parent
                    self.cursor = 0
                    self._needs_full_clear = True
                else:
                    return  # At root, go back
            elif key == "esc":
                return
            elif key in ("right", "enter") and items:
                node = items[self.cursor]
                if node.node_type == NodeType.FILE:
                    show_file_info(node)
                else:
                    self.current_node = node
                    self.cursor = 0
                    self._needs_full_clear = True
            elif key == "d" and items:
                node = items[self.cursor]
                if node.node_type == NodeType.FILE:
                    preview_file_content(self.queue_role_session, self.s3_settings, node)
                else:
                    self._handle_download(node)
            elif key == "i" and items and items[self.cursor].node_type == NodeType.FILE:
                show_file_info(items[self.cursor])
            elif key == "v" and items:
                node = items[self.cursor]
                if node.node_type == NodeType.FILE and is_image(node.name):
                    open_image_viewer(self.queue_role_session, self.s3_settings, node)
                    self.message = f"🖼️  Opened {node.name}"
            elif key == "m":
                show_manifest_list(self.root)
            elif key == "f":
                self.message = open_feedback_url()
            elif key == "q":
                clear_screen()
                return
