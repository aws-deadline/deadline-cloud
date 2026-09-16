# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI tests for the job bundle browser dialog styling (pytest-qt)."""

import time
from unittest.mock import MagicMock, patch

from qtpy.QtCore import Qt, QTimer
from qtpy.QtGui import QColor, QStandardItem
from qtpy.QtWidgets import QApplication, QDialog, QLabel, QProgressBar

from deadline.client.job_bundle._repository import BrowseEntry, BundleInfo, S3BundleRepository
from deadline.client.ui.dialogs.job_bundle_browser_dialog import (
    JobBundleBrowserDialog,
    ROLE_LOADED,
    ROLE_PATH,
    _DownloadCancelled,
)


class TestHiddenBundleStyling:
    def test_hide_dims_and_unhide_clears_foreground_override(self, qtbot, tmp_path):
        """Unhiding must clear the foreground override, not hard-code a color.

        Previously unhide set the foreground to QColor(0, 0, 0) (black), which is
        nearly invisible under a dark theme. It should instead clear the override
        so the item falls back to the palette's text color.
        """
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        item = QStandardItem("my-bundle")

        # Hiding dims the item with a theme-neutral gray.
        dialog._apply_hidden_style(item, hidden=True)
        assert item.foreground().color() == QColor(150, 150, 150)
        assert item.data(Qt.ItemDataRole.ForegroundRole) is not None

        # Unhiding clears the override entirely so the palette text color applies.
        dialog._apply_hidden_style(item, hidden=False)
        assert item.data(Qt.ItemDataRole.ForegroundRole) is None
        # Explicitly: it must NOT be reset to hard-coded black.
        assert item.data(Qt.ItemDataRole.ForegroundRole) != QColor(0, 0, 0)


class TestDownloadCancellation:
    def test_cancel_aborts_download_cooperatively_and_clears_cache(self, qtbot, tmp_path):
        """Cancelling a download must abort cooperatively, not via terminate().

        The download callback checks a cancel flag and raises to unwind the
        transfer cleanly (leaving the boto3 client/socket consistent), after
        which the partial cache is cleared. This test cancels mid-download and
        asserts the transfer stopped early and the cache was cleaned up.
        """
        repo = MagicMock()
        repo.get_bundle_size.return_value = 1024 * 1000
        observed = {"count": 0, "cancelled_via_exception": False}

        def _download(path, progress_callback=None):
            # Simulate a chunked transfer. Cooperative cancel raises
            # _DownloadCancelled *into* this call (so it unwinds in Python);
            # QThread.terminate() would instead kill the thread abruptly and this
            # except block would never run.
            try:
                for _ in range(1000):
                    progress_callback(1024)  # may raise _DownloadCancelled
                    observed["count"] += 1
                    time.sleep(0.005)
                return "/tmp/full-download"
            except _DownloadCancelled:
                observed["cancelled_via_exception"] = True
                raise

        repo.download_full_bundle.side_effect = _download

        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._s3_repo = repo
        dialog._selected_is_s3 = True
        dialog._selected_path = "s3://bucket/prefix/bundle.ojd"

        # Cancel the modal download dialog shortly after it appears.
        def _cancel():
            for widget in QApplication.topLevelWidgets():
                if isinstance(widget, QDialog) and widget.windowTitle() == "Downloading Bundle":
                    widget.reject()
                    return
            QTimer.singleShot(20, _cancel)

        QTimer.singleShot(50, _cancel)

        result = dialog.resolve_selection()

        assert result is None, "cancelled download should resolve to None"
        # The transfer was aborted by raising into the callback (cooperative),
        # not by killing the thread.
        assert observed["cancelled_via_exception"] is True
        # Partial cache must be cleaned up after the worker unwinds.
        repo.clear_cache_for.assert_called_once_with("s3://bucket/prefix/bundle.ojd")
        # The transfer aborted before completing all chunks.
        assert 0 < observed["count"] < 1000


class TestDownloadSizeReporting:
    def test_large_bundle_size_reports_human_readable_total(self, qtbot, tmp_path):
        """A >2 GiB archive must report its true total, not overflow to a bogus "1 KB".

        ``size_ready`` carries the raw byte count. A plain ``Signal(int)`` maps to
        a 32-bit C++ int, so a size above INT_MAX (2,147,483,647) overflowed and
        the dialog showed "1 KB" as the total. The signal is now ``qlonglong`` and
        the label is formatted with ``human_readable_file_size``.
        """
        repo = MagicMock()
        # ~2 GiB, larger than INT_MAX — this is the value that previously overflowed.
        big_size = 2148139290
        repo.get_bundle_size.return_value = big_size

        def _download(path, progress_callback=None):
            # Emit a handful of 100 MiB chunks so the dialog stays open long
            # enough for the inspector timer to read the label/range.
            for _ in range(30):
                progress_callback(100 * 1024 * 1024)
                time.sleep(0.01)
            return "/tmp/full-download"

        repo.download_full_bundle.side_effect = _download

        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._s3_repo = repo
        dialog._selected_is_s3 = True
        dialog._selected_path = "s3://bucket/prefix/big.ojd"

        captured_max = [0]
        captured_label = [""]

        def _inspect():
            for widget in QApplication.topLevelWidgets():
                if isinstance(widget, QDialog) and widget.windowTitle() == "Downloading Bundle":
                    bar = widget.findChild(QProgressBar)
                    label = widget.findChild(QLabel)
                    # Wait until both the size (range) and at least one progress
                    # update (value + label) have been processed.
                    if (
                        bar is not None
                        and bar.maximum() > 1
                        and bar.value() > 0
                        and label is not None
                        and "GB" in label.text()
                    ):
                        captured_max[0] = bar.maximum()
                        captured_label[0] = label.text()
                        widget.reject()
                        return
            QTimer.singleShot(10, _inspect)

        QTimer.singleShot(20, _inspect)

        dialog.resolve_selection()

        # The full 2 GiB size survived the signal round-trip without overflowing:
        # the bar's maximum is the size in KiB (~2 million, safely within int32).
        assert captured_max[0] == big_size // 1024
        # The label reports a human-readable GB total, never a bogus "1 KB".
        assert "GB" in captured_label[0]
        assert "1 KB" not in captured_label[0]


class TestPreviewInjectionHardening:
    """Bundle-derived preview values (name/description/steps) come from a template
    or S3 metadata that a queue-writer can control. The preview renders them
    inertly: the name, description, and steps labels are all forced to plain text
    so no value is ever interpreted as markup. (Qt QLabels have no script engine,
    so this is about HTML/CSS injection, not code execution.)"""

    def _dialog_with_info(self, qtbot, tmp_path, info):
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._selected_is_archive = False
        repo = MagicMock()
        repo.get_bundle_info.return_value = info
        dialog._current_repo = repo
        dialog._load_preview("some/path")
        return dialog

    def test_malicious_name_is_plain_text(self, qtbot, tmp_path):
        info = BundleInfo(
            path="s3://bucket/evil.ojd",
            name='<img src=x onerror="evil()"><b>pwn</b>',
        )
        dialog = self._dialog_with_info(qtbot, tmp_path, info)

        # Plain text: the name is shown literally and never parsed as markup,
        # so the payload is inert (no rich-text/HTML rendering path at all).
        assert dialog._preview_name.textFormat() == Qt.PlainText  # type: ignore[attr-defined]
        assert dialog._preview_name.text() == info.name

    def test_description_label_is_plain_text(self, qtbot, tmp_path):
        info = BundleInfo(
            path="s3://bucket/evil.ojd",
            name="Bundle",
            description="<script>steal()</script><style>*{}</style>",
        )
        dialog = self._dialog_with_info(qtbot, tmp_path, info)

        # Forced plain text so markup is shown literally and never interpreted.
        assert dialog._preview_desc.textFormat() == Qt.PlainText  # type: ignore[attr-defined]
        assert "<script>" in dialog._preview_desc.text()

    def test_steps_label_is_plain_text(self, qtbot, tmp_path):
        info = BundleInfo(
            path="s3://bucket/evil.ojd",
            name="Bundle",
            step_names=["<img src=x onerror=evil()>", "normal-step"],
        )
        dialog = self._dialog_with_info(qtbot, tmp_path, info)

        assert dialog._preview_steps.textFormat() == Qt.PlainText  # type: ignore[attr-defined]
        assert "<img src=x onerror=evil()>" in dialog._preview_steps.text()


BROWSER_MODULE = "deadline.client.ui.dialogs.job_bundle_browser_dialog"


class TestDownloadBundle:
    """The 'Download bundle' button resolves the selection to a local directory
    (downloading/extracting Queue bundles, opening local/history in place) and
    reveals it in the OS file explorer, without closing the dialog."""

    def _dialog(self, qtbot, tmp_path):
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        return dialog

    def test_download_button_shown_on_preview_hidden_on_error(self, qtbot, tmp_path):
        """The button lives in the preview panel: visible when a real bundle is
        previewed, hidden in the error state (and while the empty state shows)."""
        dialog = self._dialog(qtbot, tmp_path)
        dialog._selected_is_archive = False
        repo = MagicMock()
        repo.get_bundle_info.return_value = BundleInfo(path="/b", name="Bundle")
        dialog._current_repo = repo

        dialog._load_preview("/b")
        # isHidden() reflects the explicit visibility flag regardless of whether
        # the (never-shown) dialog's ancestors are visible in an offscreen test.
        assert dialog._download_button.isHidden() is False

        dialog._show_error_preview("\u26a0 broken")
        assert dialog._download_button.isHidden() is True

    def _queue_dialog(self, qtbot, tmp_path):
        repo = MagicMock(spec=S3BundleRepository)
        repo.root_path.return_value = "s3://bucket/prefix/job-bundles/"
        repo.list_entries.return_value = []
        repo.get_hidden_set.return_value = set()
        dialog = JobBundleBrowserDialog(queue_source=repo, local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        return dialog, repo

    def test_queue_button_says_download_with_size(self, qtbot, tmp_path):
        """Queue bundles are fetched over the network: the button says 'Download
        bundle' and shows the size so the user knows how much will transfer."""
        dialog, repo = self._queue_dialog(qtbot, tmp_path)
        assert dialog._radio_s3.isChecked()
        repo.get_bundle_info.return_value = BundleInfo(
            path="s3://b/p/x.ojd", name="Bundle", size_bytes=12_000_000
        )
        dialog._current_repo = repo

        dialog._load_preview("s3://b/p/x.ojd")
        # Queue previews resolve on a worker thread; wait for the render.
        qtbot.waitUntil(lambda: not dialog._preview_workers, timeout=5000)

        text = dialog._download_button.text()
        assert text.startswith("Download bundle")
        assert "MB" in text  # e.g. "Download bundle (12.0 MB)"

    def test_queue_button_download_without_size(self, qtbot, tmp_path):
        """A Queue bundle with unknown size still says 'Download bundle' (no size)."""
        dialog, repo = self._queue_dialog(qtbot, tmp_path)
        repo.get_bundle_info.return_value = BundleInfo(path="s3://b/p/x.ojd", name="Bundle")
        dialog._current_repo = repo

        dialog._load_preview("s3://b/p/x.ojd")
        qtbot.waitUntil(lambda: not dialog._preview_workers, timeout=5000)

        assert dialog._download_button.text() == "Download bundle"

    def test_local_button_says_open(self, qtbot, tmp_path):
        """Local/History bundles reveal the folder, so the button says 'Show bundle folder'."""
        dialog = self._dialog(qtbot, tmp_path)  # local source (Queue unavailable)
        assert not dialog._radio_s3.isChecked()
        dialog._selected_is_archive = False
        repo = MagicMock()
        repo.get_bundle_info.return_value = BundleInfo(path="/b", name="Bundle")
        dialog._current_repo = repo

        dialog._load_preview("/b")

        assert dialog._download_button.text() == "Show bundle folder"

    def test_download_opens_resolved_local_path(self, qtbot, tmp_path):
        dialog = self._dialog(qtbot, tmp_path)
        dialog._selected_path = "/some/bundle"
        # resolve_selection performs the download/extract/passthrough and returns
        # a local directory; isolate _on_download from that machinery here.
        with (
            patch.object(dialog, "resolve_selection", return_value="/local/extracted") as resolve,
            patch.object(dialog, "_open_in_file_explorer") as opener,
        ):
            dialog._on_download()
        resolve.assert_called_once()
        opener.assert_called_once_with("/local/extracted")

    def test_download_noop_when_nothing_selected(self, qtbot, tmp_path):
        dialog = self._dialog(qtbot, tmp_path)
        dialog._selected_path = None
        with (
            patch.object(dialog, "resolve_selection") as resolve,
            patch.object(dialog, "_open_in_file_explorer") as opener,
        ):
            dialog._on_download()
        resolve.assert_not_called()
        opener.assert_not_called()

    def test_download_noop_when_resolve_cancelled(self, qtbot, tmp_path):
        """A cancelled download (or already-surfaced error) resolves to None and
        opens nothing."""
        dialog = self._dialog(qtbot, tmp_path)
        dialog._selected_path = "/some/bundle"
        with (
            patch.object(dialog, "resolve_selection", return_value=None),
            patch.object(dialog, "_open_in_file_explorer") as opener,
        ):
            dialog._on_download()
        opener.assert_not_called()

    def test_download_surfaces_error_on_resolve_exception(self, qtbot, tmp_path):
        dialog = self._dialog(qtbot, tmp_path)
        dialog._selected_path = "/some/bundle"
        with (
            patch.object(dialog, "resolve_selection", side_effect=ValueError("bad zip")),
            patch.object(dialog, "_open_in_file_explorer") as opener,
            patch.object(dialog, "_show_error_preview") as show_error,
        ):
            dialog._on_download()
        opener.assert_not_called()
        show_error.assert_called_once()
        assert "bad zip" in show_error.call_args.args[0]

    def test_open_in_file_explorer_macos(self, qtbot, tmp_path, monkeypatch):
        monkeypatch.setattr(f"{BROWSER_MODULE}.sys.platform", "darwin")
        with patch(f"{BROWSER_MODULE}.subprocess.run") as run:
            JobBundleBrowserDialog._open_in_file_explorer("/x/y")
        run.assert_called_once_with(["open", "/x/y"], check=False)

    def test_open_in_file_explorer_linux(self, qtbot, tmp_path, monkeypatch):
        monkeypatch.setattr(f"{BROWSER_MODULE}.sys.platform", "linux")
        with patch(f"{BROWSER_MODULE}.subprocess.run") as run:
            JobBundleBrowserDialog._open_in_file_explorer("/x/y")
        run.assert_called_once_with(["xdg-open", "/x/y"], check=False)

    def test_open_in_file_explorer_windows(self, qtbot, tmp_path, monkeypatch):
        monkeypatch.setattr(f"{BROWSER_MODULE}.sys.platform", "win32")
        # os.startfile only exists on Windows; inject a stub so the test runs anywhere.
        startfile = MagicMock()
        monkeypatch.setattr(f"{BROWSER_MODULE}.os.startfile", startfile, raising=False)
        JobBundleBrowserDialog._open_in_file_explorer("C:\\x\\y")
        startfile.assert_called_once_with("C:\\x\\y")


class TestQueueLoadErrors:
    """When credentials are missing/expired, the Queue source must surface the
    error (inline banner) rather than leaving the tree stuck on 'Loading...'."""

    def test_set_queue_source_failure_shows_banner(self, qtbot, tmp_path):
        """A failed background init (from_config/list) shows the reason in the
        banner and disables the Queue radio."""
        dialog = JobBundleBrowserDialog(
            queue_source=None,
            queue_loading=True,
            local_source=str(tmp_path),
        )
        qtbot.addWidget(dialog)

        dialog.set_queue_source(None, "ExpiredTokenException: The security token expired")

        assert dialog._queue_warning.isHidden() is False
        assert "ExpiredTokenException" in dialog._queue_warning.text()
        assert dialog._radio_s3.isEnabled() is False
        # It must not stay on the (loading) Queue source: it falls back to Local,
        # so the tree is no longer stuck showing "Loading...".
        assert dialog._radio_local.isChecked() is True
        # The error text must be selectable so it can be copied (e.g. into a ticket).
        assert bool(dialog._queue_warning.textInteractionFlags() & Qt.TextSelectableByMouse)  # type: ignore[attr-defined]

    def test_refresh_error_surfaces_banner_and_clears_loading(self, qtbot, tmp_path):
        """A failed queue listing refresh surfaces the error and replaces the
        'Loading...' placeholder instead of hanging."""
        dialog = JobBundleBrowserDialog(
            queue_source=None,
            queue_loading=True,
            local_source=str(tmp_path),
        )
        qtbot.addWidget(dialog)
        # Queue is the default source while loading.
        assert dialog._radio_s3.isChecked()

        dialog._on_s3_refresh_error("ExpiredTokenException: The security token expired")

        assert dialog._queue_warning.isHidden() is False
        assert "ExpiredTokenException" in dialog._queue_warning.text()
        assert dialog._tree_empty_label.text() == "Could not load queue bundles"

    def test_refresh_worker_error_is_caught_and_surfaced(self, qtbot, tmp_path):
        """End-to-end: switching to a Queue whose listing raises (expired creds)
        drives the background worker's error path through to the banner, rather
        than silently killing the thread and hanging on 'Loading...'."""
        repo = MagicMock(spec=S3BundleRepository)
        repo.root_path.return_value = "s3://bucket/prefix/job-bundles/"
        repo.list_entries.side_effect = Exception("ExpiredToken: credentials expired")
        repo.get_hidden_set.return_value = set()

        # Construct on Local so the synchronous initial populate doesn't run the
        # queue listing; then switch to Queue to exercise _refresh_s3_async.
        dialog = JobBundleBrowserDialog(
            queue_source=repo,
            local_source=str(tmp_path),
        )
        qtbot.addWidget(dialog)
        dialog._radio_local.setChecked(True)
        dialog._queue_warning.setVisible(False)

        dialog._radio_s3.setChecked(True)  # triggers _refresh_s3_async

        qtbot.waitUntil(lambda: dialog._queue_warning.isHidden() is False, timeout=5000)
        assert "ExpiredToken" in dialog._queue_warning.text()


class TestFilterExpansion:
    """Filtering expands the tree to reveal matches. Clearing the filter collapses
    it back to the default root view — unless a bundle is selected, in which case
    the expansion is kept so the selection stays visible in its folder context."""

    def test_clearing_filter_collapses_when_no_selection(self, qtbot, tmp_path):
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._selected_path = None

        with (
            patch.object(dialog._tree, "expandAll") as expand,
            patch.object(dialog._tree, "collapseAll") as collapse,
        ):
            dialog._on_filter_changed("abc")  # typing expands
            dialog._on_filter_changed("")  # clearing collapses (nothing selected)
            # collapseAll is deferred (QTimer.singleShot) to avoid a mid-refilter
            # crash, so let the event loop run before asserting.
            qtbot.waitUntil(lambda: collapse.called, timeout=1000)

        expand.assert_called_once()
        collapse.assert_called_once()

    def test_clearing_filter_keeps_expansion_when_bundle_selected(self, qtbot, tmp_path):
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._selected_path = "/some/bundle"  # a bundle was chosen

        with patch.object(dialog._tree, "collapseAll") as collapse:
            dialog._on_filter_changed("abc")
            dialog._on_filter_changed("")  # cleared, but selection keeps it expanded
            qtbot.wait(50)  # give any (unexpected) deferred collapse a chance to run

        collapse.assert_not_called()


class TestPreloadFirstLevel:
    """The immediate children of each top-level folder are preloaded so the filter
    can match one level deep without expanding — but only one level (deeper folders
    stay lazy-loaded)."""

    @staticmethod
    def _root_child_by_path(dialog, path):
        root = dialog._model.invisibleRootItem()
        for row in range(root.rowCount()):
            item = root.child(row)
            if item is not None and item.data(ROLE_PATH) == path:
                return item
        return None

    def test_first_level_children_are_preloaded(self, qtbot, tmp_path):
        # tmp/sub/my-bundle/template.yaml  →  "sub" is a top-level folder whose
        # child bundle should be preloaded.
        sub = tmp_path / "sub"
        bundle = sub / "my-bundle"
        bundle.mkdir(parents=True)
        (bundle / "template.yaml").write_text("name: Nested\nsteps: []\n")

        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)

        sub_item = self._root_child_by_path(dialog, str(sub))
        assert sub_item is not None
        # Preloaded: marked loaded and populated with the real child (not a placeholder).
        assert sub_item.data(ROLE_LOADED) is True
        child_paths = [sub_item.child(r).data(ROLE_PATH) for r in range(sub_item.rowCount())]
        assert str(bundle) in child_paths

    def test_second_level_is_not_preloaded(self, qtbot, tmp_path):
        # tmp/a/b/c-bundle/template.yaml — only "a" (level 1) is preloaded; "b"
        # (level 2) must remain lazy (unloaded) so preloading stays bounded.
        a = tmp_path / "a"
        b = a / "b"
        cbundle = b / "c-bundle"
        cbundle.mkdir(parents=True)
        (cbundle / "template.yaml").write_text("name: Deep\nsteps: []\n")

        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)

        a_item = self._root_child_by_path(dialog, str(a))
        assert a_item is not None
        assert a_item.data(ROLE_LOADED) is True  # level 1 loaded
        # "b" is present (it's a's child) but its own children are NOT loaded yet.
        b_item = next(
            (
                a_item.child(r)
                for r in range(a_item.rowCount())
                if a_item.child(r).data(ROLE_PATH) == str(b)
            ),
            None,
        )
        assert b_item is not None
        assert not b_item.data(ROLE_LOADED)  # level 2 stays lazy


class TestQueueWarningEscaping:
    """`_show_queue_warning` renders into a RichText banner; the message is a
    botocore error string that can contain markup and must be escaped."""

    def test_show_queue_warning_escapes_markup(self, qtbot, tmp_path):
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)

        dialog._show_queue_warning("<b>boom</b> & <img src=x>")

        text = dialog._queue_warning.text()
        assert "&lt;b&gt;boom&lt;/b&gt;" in text
        assert "&amp;" in text
        assert "<img src=x>" not in text


class TestRelativeKeyHiding:
    """The browser must key hidden state by the prefix-relative path so hiding one
    bundle doesn't dim/filter a same-named bundle in a different subfolder."""

    def _s3_repo(self):
        with patch("boto3.Session"):
            repo = S3BundleRepository("bucket", "DeadlineCloud", session=MagicMock())
        repo._s3 = MagicMock()
        return repo

    def test_entry_hidden_is_scoped_to_the_subfolder(self, qtbot, tmp_path):
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._current_repo = self._s3_repo()
        # Only maya/render is hidden.
        dialog._hidden_set = {"maya/render"}

        base = "s3://bucket/DeadlineCloud/job-bundles/"
        maya = BrowseEntry(
            name="render", path=base + "maya/render.ojd", is_bundle=True, is_archive=True
        )
        nuke = BrowseEntry(
            name="render", path=base + "nuke/render.ojd", is_bundle=True, is_archive=True
        )

        assert dialog._entry_hidden(maya) is True
        # Same leaf name, different folder — must NOT be collaterally hidden.
        assert dialog._entry_hidden(nuke) is False

    def test_root_level_entry_hidden_by_name(self, qtbot, tmp_path):
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._current_repo = self._s3_repo()
        dialog._hidden_set = {"blender"}

        base = "s3://bucket/DeadlineCloud/job-bundles/"
        entry = BrowseEntry(
            name="blender", path=base + "blender.ojd", is_bundle=True, is_archive=True
        )
        assert dialog._entry_hidden(entry) is True

    def test_dot_prefixed_always_hidden(self, qtbot, tmp_path):
        dialog = JobBundleBrowserDialog(local_source=str(tmp_path))
        qtbot.addWidget(dialog)
        dialog._current_repo = self._s3_repo()
        dialog._hidden_set = set()

        base = "s3://bucket/DeadlineCloud/job-bundles/"
        dotfile = BrowseEntry(
            name=".secret", path=base + ".secret.ojd", is_bundle=True, is_archive=True
        )
        assert dialog._entry_hidden(dotfile) is True
