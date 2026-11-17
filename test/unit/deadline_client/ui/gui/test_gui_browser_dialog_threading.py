# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Threading tests for the job bundle browser dialog (pytest-qt).

AGENTS.md requires that AWS APIs are never called from the main Qt thread.
resolve_selection() previously made a synchronous head_object call (via
get_bundle_size) on the main thread before starting the download worker; these
tests assert that both the size lookup and the download run off the main thread.
"""

import threading
from unittest.mock import MagicMock

from deadline.client.ui.dialogs.job_bundle_browser_dialog import JobBundleBrowserDialog


class TestResolveSelectionThreading:
    def test_head_object_and_download_run_off_main_thread(self, qtbot, tmp_path):
        main_ident = threading.get_ident()
        calls: dict = {}
        size_threads: list = []

        repo = MagicMock()

        def _get_size(path):
            size_threads.append(threading.get_ident())
            calls["size_thread"] = threading.get_ident()
            return 4096

        def _download(path, progress_callback=None):
            calls["download_thread"] = threading.get_ident()
            if progress_callback:
                progress_callback(4096)
            return "/tmp/resolved-bundle"

        repo.get_bundle_size.side_effect = _get_size
        repo.download_full_bundle.side_effect = _download

        # Construct with only a Local source (empty temp dir) to avoid exercising
        # the S3 listing path during construction, then wire up an S3 selection.
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._s3_repo = repo
        dialog._selected_is_s3 = True
        dialog._selected_path = "s3://bucket/prefix/bundle.ojd"

        result = dialog.resolve_selection()

        assert result == "/tmp/resolved-bundle"
        # get_bundle_size performs a synchronous head_object — it must NEVER run
        # on the Qt main thread (assert across every call, not just the last).
        assert size_threads, "get_bundle_size was never called"
        assert main_ident not in size_threads, (
            "get_bundle_size (head_object) must not run on the main Qt thread"
        )
        # The download must also be off the main thread (was already the case).
        assert calls.get("download_thread") is not None
        assert calls["download_thread"] != main_ident
