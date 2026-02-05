# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for AttachmentBrowserTUI."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from deadline.client.cli._groups._job_tui._attachment_browser import AttachmentBrowserTUI
from deadline.client.cli._groups._browse_tui import NodeType, TreeNode


@pytest.fixture
def mock_s3_settings():
    settings = MagicMock()
    settings.rootPrefix = "root/prefix"
    settings.s3BucketName = "test-bucket"
    return settings


@pytest.fixture
def mock_sessions():
    boto3_session = MagicMock()
    queue_role_session = MagicMock()
    # Mock the deadline client returned by boto3_session.client()
    mock_deadline = MagicMock()
    mock_deadline.get_job.return_value = {
        "name": "Test Job",
        "attachments": {"manifests": []},
    }
    boto3_session.client.return_value = mock_deadline
    return boto3_session, queue_role_session


class TestAttachmentBrowserTUI:
    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_output_manifests",
        return_value=[],
    )
    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_input_manifests",
        return_value=[],
    )
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.read_key")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.console")
    def test_run_esc_returns(
        self,
        mock_console,
        mock_read_key,
        mock_load_input,
        mock_load_output,
        mock_sessions,
        mock_s3_settings,
    ):
        mock_read_key.return_value = "esc"
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        tui.run()  # Should return without error

    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_output_manifests",
        return_value=[],
    )
    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_input_manifests",
        return_value=[],
    )
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.read_key")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.console")
    def test_run_quit(
        self,
        mock_console,
        mock_read_key,
        mock_load_input,
        mock_load_output,
        mock_sessions,
        mock_s3_settings,
    ):
        mock_read_key.return_value = "q"
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        tui.run()  # Should return without error

    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_output_manifests",
        return_value=[],
    )
    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_input_manifests",
        return_value=[],
    )
    def test_load_manifests(
        self, mock_load_input, mock_load_output, mock_sessions, mock_s3_settings
    ):
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        tui.load_manifests()
        assert len(tui.root.children) == 2  # input + output categories
        assert tui.root.children[0].name == "input"
        assert tui.root.children[1].name == "output"


def _build_test_tree() -> TreeNode:
    """Build a small tree: root -> [input(category) -> [folder -> [file]], output(category)]."""
    root = TreeNode(name="Job", node_type=NodeType.ROOT)
    input_node = TreeNode(name="input", node_type=NodeType.CATEGORY, parent=root)
    output_node = TreeNode(name="output", node_type=NodeType.CATEGORY, parent=root)
    root.children = [input_node, output_node]

    folder = TreeNode(name="renders", node_type=NodeType.FOLDER, parent=input_node)
    input_node.children = [folder]

    file_node = TreeNode(name="frame001.exr", node_type=NodeType.FILE, size=1024, parent=folder)
    folder.children = [file_node]

    return root


class TestAttachmentBrowserScreenClearing:
    """Tests for _needs_full_clear flag behavior during folder navigation."""

    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_output_manifests",
        return_value=[],
    )
    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_input_manifests",
        return_value=[],
    )
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.read_key")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.console")
    def test_first_render_uses_hard_clear(
        self,
        mock_console,
        mock_read_key,
        mock_clear,
        mock_load_input,
        mock_load_output,
        mock_sessions,
        mock_s3_settings,
    ):
        """First render after run() should call clear_screen(full=True)."""
        mock_read_key.return_value = "esc"
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        tui.run()
        assert mock_clear.call_args_list[0] == call(full=True)

    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_output_manifests",
        return_value=[],
    )
    @patch(
        "deadline.client.cli._groups._job_tui._attachment_browser.load_input_manifests",
        return_value=[],
    )
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.read_key")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.console")
    def test_scrolling_uses_soft_clear(
        self,
        mock_console,
        mock_read_key,
        mock_clear,
        mock_load_input,
        mock_load_output,
        mock_sessions,
        mock_s3_settings,
    ):
        """Up/down scrolling within the same folder should use soft clear."""
        mock_read_key.side_effect = ["down", "up", "esc"]
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        tui.run()
        # First render: hard clear, subsequent renders: soft clear
        assert mock_clear.call_args_list[0] == call(full=True)
        assert mock_clear.call_args_list[1] == call(full=False)
        assert mock_clear.call_args_list[2] == call(full=False)

    @patch("deadline.client.cli._groups._job_tui._attachment_browser.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.read_key")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.console")
    def test_entering_folder_triggers_hard_clear(
        self,
        mock_console,
        mock_read_key,
        mock_clear,
        mock_sessions,
        mock_s3_settings,
    ):
        """Navigating into a subfolder (right/enter) should trigger hard clear."""
        # Navigate: right (enter input category), then esc
        mock_read_key.side_effect = ["right", "esc"]
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        # Pre-populate tree and skip load_manifests
        tui.root = _build_test_tree()
        tui.current_node = tui.root
        tui.load_manifests = lambda: None  # type: ignore[assignment]
        tui.run()
        # call 0: initial render (full=True)
        # call 1: render after entering folder (full=True again)
        assert mock_clear.call_args_list[0] == call(full=True)
        assert mock_clear.call_args_list[1] == call(full=True)

    @patch("deadline.client.cli._groups._job_tui._attachment_browser.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.read_key")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.console")
    def test_leaving_folder_triggers_hard_clear(
        self,
        mock_console,
        mock_read_key,
        mock_clear,
        mock_sessions,
        mock_s3_settings,
    ):
        """Navigating back to parent (left) should trigger hard clear."""
        # Navigate: right (enter input), left (back to root), esc
        mock_read_key.side_effect = ["right", "left", "esc"]
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        tui.root = _build_test_tree()
        tui.current_node = tui.root
        tui.load_manifests = lambda: None  # type: ignore[assignment]
        tui.run()
        # call 0: initial render (full=True)
        # call 1: after entering folder (full=True)
        # call 2: after leaving folder (full=True)
        assert mock_clear.call_args_list[0] == call(full=True)
        assert mock_clear.call_args_list[1] == call(full=True)
        assert mock_clear.call_args_list[2] == call(full=True)

    def test_needs_full_clear_initial_value(self, mock_sessions, mock_s3_settings):
        """_needs_full_clear should be True after construction."""
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        assert tui._needs_full_clear is True

    @patch("deadline.client.cli._groups._job_tui._attachment_browser.clear_screen")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.read_key")
    @patch("deadline.client.cli._groups._job_tui._attachment_browser.console")
    def test_deep_navigation_round_trip(
        self,
        mock_console,
        mock_read_key,
        mock_clear,
        mock_sessions,
        mock_s3_settings,
    ):
        """Navigate root→input→renders→(back)→(back)→esc: each folder change is hard clear."""
        # right=enter input, right=enter renders, left=back to input, left=back to root, esc
        mock_read_key.side_effect = ["right", "right", "left", "left", "esc"]
        boto3_session, queue_role_session = mock_sessions
        tui = AttachmentBrowserTUI(
            "farm-1",
            "queue-1",
            "job-1",
            "Test Job",
            "SUCCEEDED",
            boto3_session,
            queue_role_session,
            mock_s3_settings,
        )
        tui.root = _build_test_tree()
        tui.current_node = tui.root
        tui.load_manifests = lambda: None  # type: ignore[assignment]
        tui.run()
        # All 5 renders should use hard clear (each is a folder transition)
        for i in range(5):
            assert mock_clear.call_args_list[i] == call(full=True), (
                f"Render {i} should be hard clear"
            )
