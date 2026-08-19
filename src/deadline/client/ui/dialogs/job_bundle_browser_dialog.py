# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Custom job bundle browser dialog that replaces the native folder picker.
Shows a navigable tree of directories/bundles with a preview panel.
"""

from __future__ import annotations

import html
import os
import re
import subprocess
import sys
from logging import getLogger
from typing import Optional

from deadline.job_attachments.api import human_readable_file_size

from qtpy.QtCore import Qt, QModelIndex, QSize, QSortFilterProxyModel, QThread, QTimer, Signal  # type: ignore
from qtpy.QtGui import QColor, QPalette, QStandardItemModel, QStandardItem  # type: ignore
from qtpy.QtWidgets import (  # type: ignore
    QApplication,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QGraphicsOpacityEffect,
    QScrollArea,
    QSplitter,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QTreeView,
    QVBoxLayout,
    QWidget,
)

from .._utils import tr, warning_banner_qss
from ..widgets.expandable_section import ExpandableSection as _ExpandableSection
from ...job_bundle._repository import (
    BrowseEntry as _BrowseEntry,
    BundleRepository as _BundleRepository,
    LocalBundleRepository as _LocalBundleRepository,
    S3BundleRepository as _S3BundleRepository,
)

logger = getLogger(__name__)

# Custom data roles
ROLE_PATH = Qt.UserRole + 1
ROLE_IS_BUNDLE = Qt.UserRole + 2
ROLE_LOADED = Qt.UserRole + 3
ROLE_IS_ARCHIVE = Qt.UserRole + 4
ROLE_IS_HIDDEN = Qt.UserRole + 5

# Semantic warning/accent color, matched to the queue-unavailable banner used
# elsewhere in this dialog so the panel reads as part of the same UI. Legible on
# both light and dark themes.
REQUIRED_COLOR = QColor("#b35900")


class _DownloadCancelled(Exception):
    """Raised inside a download progress callback to abort a transfer cooperatively.

    Preferred over ``QThread.terminate()``, which would kill the thread mid-request
    and could leave the boto3 client/socket in an inconsistent state.
    """


# Shared "quiet section label" style — small, bold, sentence-case, muted.


class _WrappingLabel(QLabel):
    """QLabel that word-wraps without expanding its parent's width.

    Standard QLabel with wordWrap reports a minimumSizeHint equal to the full
    single-line width, which pushes splitter panes and scroll areas wider.
    This subclass overrides that to allow shrinking.
    """

    def minimumSizeHint(self):
        return QSize(0, 0)


# Color is set per-widget from the palette so it adapts to the theme.
_SECTION_LABEL_QSS = "font-size: 13px; font-weight: bold;"

# Friendly, artist-facing labels for the OpenJD parameter type enums.
_FRIENDLY_PARAM_TYPES = {
    "STRING": "Text",
    "PATH": "Path",
    "INT": "Number",
    "FLOAT": "Number",
}


def _friendly_param_type(raw_type: str) -> str:
    """Map an OpenJD parameter type enum to an artist-facing label."""
    return _FRIENDLY_PARAM_TYPES.get(raw_type.upper(), raw_type.title() if raw_type else "?")


def _folders_first(entries: list) -> list:
    """Order entries folders-first, preserving the repo's name-sort within each group."""
    folders = [e for e in entries if not e.is_bundle]
    bundles = [e for e in entries if e.is_bundle]
    return folders + bundles


def _steps_list_text(step_names: list[str]) -> str:
    """Render step names as a plain bulleted list."""
    return "\n".join(f"  •  {name}" for name in step_names if name)


def _normalize_description(text: str) -> str:
    """Collapse hard line breaks within a paragraph so the label can word-wrap to
    the panel width, while preserving intentional blank-line paragraph breaks.

    Template descriptions are often authored as multi-line YAML blocks, which would
    otherwise display with awkward breaks mid-sentence.
    """
    paragraphs = re.split(r"\n\s*\n", text.strip())
    return "\n\n".join(" ".join(p.split()) for p in paragraphs if p.strip())


class _BundleFilterProxy(QSortFilterProxyModel):
    """Proxy that filters by text and optionally hides items marked as hidden."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._show_hidden = False

    def set_show_hidden(self, show: bool):
        self._show_hidden = show
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent) -> bool:  # type: ignore[override]
        index = self.sourceModel().index(source_row, 0, source_parent)
        if not self._show_hidden and index.data(ROLE_IS_HIDDEN):
            return False
        return super().filterAcceptsRow(source_row, source_parent)


class JobBundleBrowserDialog(QDialog):
    """
    A dialog for browsing and selecting job bundles from local filesystem or queue.

    Args:
        local_root: Default local directory to browse.
        s3_bucket_name: The queue's job attachment S3 bucket name (optional).
        s3_root_prefix: The queue's job attachment S3 root prefix (optional).
        parent: Parent widget.
    """

    bundle_selected = Signal(str)  # Emits the selected bundle path

    def __init__(
        self,
        *,
        queue_source: Optional[_S3BundleRepository] = None,
        queue_error: str = "",
        queue_loading: bool = False,
        local_source: str = "",
        history_source: str = "",
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent=parent)
        self.setWindowTitle(tr("Browse Job Bundles"))
        self.setMinimumSize(750, 550)
        self.resize(850, 620)

        self._s3_repo: Optional[_S3BundleRepository] = queue_source
        self._s3_error = queue_error
        self._s3_available = self._s3_repo is not None
        self._s3_loading = queue_loading

        self._local_repo = _LocalBundleRepository(root=local_source, include_archives=True)

        self._history_repo: Optional[_LocalBundleRepository] = None
        if history_source and os.path.isdir(history_source):
            self._history_repo = _LocalBundleRepository(root=history_source, include_archives=True)

        self._current_repo: _BundleRepository = self._local_repo
        self._selected_path: Optional[str] = None
        self._selected_is_s3 = False
        self._selected_is_archive = False
        self._cached_root_entries: list[_BrowseEntry] = []
        self._hidden_set: set[str] = set()
        self._last_preview_path: Optional[str] = None
        self._tree_states: dict[str, set[str]] = {}  # repo root -> expanded paths
        self._tree_selections: dict[str, str] = {}  # repo root -> selected path
        self._s3_refresh_worker: Optional[QThread] = None
        self._prefetch_worker: Optional[QThread] = None
        self._prefetch_cancelled = False
        # Background preview loads (Queue source). Tracked so they're joined on
        # teardown (a GC'd running QThread aborts the process) and staleness-
        # guarded via ``_preview_request_path`` so a superseded result is ignored.
        self._preview_workers: set = set()
        self._preview_request_path: Optional[str] = None
        # Background first-level preloads (Queue source): list top-level folders'
        # children off the UI thread, then apply on the main thread.
        self._preload_workers: set = set()
        self._ready = False

        self._build_ui()
        self._ready = True
        self._populate_root()

        # Set initial focus to the source radio group
        if self._radio_s3.isChecked():
            self._radio_s3.setFocus()
        elif self._radio_local.isChecked():
            self._radio_local.setFocus()

    @property
    def selected_path(self) -> Optional[str]:
        return self._selected_path

    @property
    def selected_is_s3(self) -> bool:
        return self._selected_is_s3

    @property
    def selected_is_archive(self) -> bool:
        return self._selected_is_archive

    @property
    def s3_repo(self) -> Optional[_S3BundleRepository]:
        return self._s3_repo

    def set_queue_source(self, repo, error: str, entries: list = None, hidden_set: set = None):
        """Called when background S3 initialization completes."""
        self._s3_loading = False
        if repo:
            self._s3_repo = repo
            self._s3_available = True
            self._s3_error = ""
        else:
            self._s3_available = False
            self._s3_error = error
            self._radio_s3.setEnabled(False)
            # Surface the reason (e.g. not logged in / expired credentials) in the
            # inline banner rather than silently disabling the Queue option.
            self._show_queue_warning(error)
            # Switch to Local if Queue was selected but failed
            if self._radio_s3.isChecked():
                self._radio_local.setChecked(True)
                return

        # If Queue is currently selected, populate now
        if self._radio_s3.isChecked():
            self._current_repo = self._s3_repo
            if entries is not None:
                # Use pre-fetched data to avoid blocking the main thread
                self._cached_root_entries = _folders_first(entries)
                self._hidden_set = hidden_set or set()
                self._populate_tree_from_cache()
            else:
                self._populate_root()

    def done(self, result: int) -> None:
        """Join background workers before the dialog is torn down.

        Both accept() and reject() (including the window-close/Esc paths, which
        QDialog routes through reject()) call ``done``. Dropping the last
        reference to a still-running ``QThread`` makes Qt call ``std::terminate``
        (SIGABRT), which hard-aborts the host application (e.g. Maya/Nuke). This
        also closes the window where ``resolve_selection``'s download races the
        preview prefetch on the repo's shared HEAD cache.
        """
        self._prefetch_cancelled = True
        for worker in (self._s3_refresh_worker, self._prefetch_worker):
            if worker is not None:
                worker.wait()
        for worker in list(self._preview_workers):
            worker.wait()
        for worker in list(self._preload_workers):
            worker.wait()
        super().done(result)

    def resolve_selection(self) -> Optional[str]:
        """Resolve the selected bundle to a local directory path.

        Handles S3 download/cache, archive extraction, and direct directory paths.
        Returns None if no selection.
        """
        if not self._selected_path:
            return None

        if self._selected_is_s3 and self._s3_repo:

            class _DownloadWorker(QThread):
                progress = Signal(int)
                # Emitted once the archive size is known (from a head_object made
                # on this worker thread — see below). Declared as ``qlonglong``
                # (64-bit) because archive sizes routinely exceed INT_MAX (2 GiB);
                # a plain ``Signal(int)`` maps to a 32-bit C++ int, so a >2 GiB
                # size overflows (shiboken raises OverflowError / clamps the value,
                # which is why the reported total came out as a garbage "1 KB").
                size_ready = Signal("qlonglong")  # type: ignore[arg-type]
                # NOTE: named ``done`` rather than ``finished`` to avoid shadowing
                # QThread's built-in ``finished`` signal.
                done = Signal(str)
                error = Signal(str)

                def __init__(self, repo, path):
                    super().__init__()
                    self._repo = repo
                    self._path = path
                    self._sent = 0
                    self._cancelled = False

                def cancel(self):
                    # Cooperative cancel: the next progress callback aborts the
                    # transfer, letting boto3 unwind cleanly.
                    self._cancelled = True

                def run(self):
                    try:
                        # Determine the archive size here rather than on the main
                        # (Qt) thread: get_bundle_size() makes a synchronous
                        # head_object network call, which would block the UI.
                        # The result is cached in the repo's _last_head and reused
                        # by the subsequent download, so this adds no extra call.
                        total = self._repo.get_bundle_size(self._path)
                        self.size_ready.emit(total)

                        def _cb(n):
                            if self._cancelled:
                                # Abort the transfer cooperatively instead of
                                # terminating the thread, which would risk an
                                # inconsistent boto3 client/socket.
                                raise _DownloadCancelled()
                            self._sent += n
                            self.progress.emit(self._sent // 1024)

                        result = self._repo.download_full_bundle(self._path, progress_callback=_cb)
                        self.done.emit(result)
                    except _DownloadCancelled:
                        # User cancelled — nothing to report; the main thread
                        # clears any partial cache after the worker unwinds.
                        pass
                    except Exception as e:
                        self.error.emit(str(e))

            progress = QDialog(self)
            progress.setWindowFlags(Qt.Dialog | Qt.CustomizeWindowHint | Qt.WindowTitleHint)
            progress.setWindowTitle("Downloading Bundle")
            progress.setWindowModality(Qt.ApplicationModal)
            progress.setMinimumWidth(350)
            _dlg_layout = QVBoxLayout(progress)
            _progress_label = QLabel("Downloading bundle...")
            _progress_label.setAlignment(Qt.AlignCenter)
            _progress_bar = QProgressBar()
            # Start indeterminate (busy) until the worker reports the size.
            _progress_bar.setRange(0, 0)
            _cancel_btn = QPushButton("Cancel")
            _cancel_btn.clicked.connect(progress.reject)
            _dlg_layout.addWidget(_progress_label)
            _dlg_layout.addWidget(_progress_bar)
            _dlg_layout.addWidget(_cancel_btn, alignment=Qt.AlignRight)

            worker = _DownloadWorker(self._s3_repo, self._selected_path)
            download_result = [None]
            download_error = []
            # The true archive size in bytes. Tracked on the Python side rather
            # than read back from the progress bar's maximum, because the bar
            # works in KiB (a 32-bit int, so multi-GiB byte counts would overflow
            # it) and reconstructing bytes from it loses precision.
            total_bytes = [0]

            def _on_size(total):
                total_bytes[0] = total
                try:
                    # Progress bar tracks KiB to stay within QProgressBar's
                    # 32-bit int range for large (multi-GiB) archives.
                    _progress_bar.setRange(0, max(1, total // 1024))
                except RuntimeError:
                    return

            def _on_progress(n):
                try:
                    _progress_bar.setValue(n)
                except RuntimeError:
                    return
                total = total_bytes[0]
                current = n * 1024
                if total > 0:
                    _progress_label.setText(
                        f"Downloading bundle... {human_readable_file_size(current)}"
                        f" / {human_readable_file_size(total)}"
                    )

            def _on_finished(path):
                download_result[0] = path
                progress.close()

            def _on_error(msg):
                download_error.append(msg)
                progress.close()

            worker.size_ready.connect(_on_size, Qt.QueuedConnection)
            worker.progress.connect(_on_progress, Qt.QueuedConnection)
            worker.done.connect(_on_finished, Qt.QueuedConnection)
            worker.error.connect(_on_error, Qt.QueuedConnection)
            # Cancelling the dialog (Cancel button or window close) flags the
            # worker so its download callback aborts cooperatively.
            progress.rejected.connect(worker.cancel)
            worker.start()
            progress.exec_()

            if download_error:
                # A real failure (AccessDenied, corrupt/non-zip .ojd, rejected
                # zip bomb, full disk). Surface it instead of silently returning
                # None, which the callers treat as "user changed their mind" and
                # would loop on forever.
                worker.wait()
                self._s3_repo.clear_cache_for(self._selected_path)
                self._show_error_preview(f"Failed to download bundle:\n{download_error[0]}")
                return None

            if not download_result[0]:
                # User cancelled — stop the worker cooperatively (the flag is
                # checked in the download callback) and wait for it to unwind so
                # the boto3 client/socket close cleanly, then remove partial cache.
                worker.cancel()
                worker.wait()
                self._s3_repo.clear_cache_for(self._selected_path)
                return None

            worker.wait()
            return download_result[0]
        elif self._selected_is_archive:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                return self._local_repo.extract_bundle(self._selected_path)
            except Exception as e:
                # extract_bundle raises ValueError for a corrupt/renamed .ojd or a
                # rejected zip bomb; surface it rather than letting it propagate
                # out of the Qt slot with the wait cursor still pushed.
                logger.warning("Failed to open bundle %s: %s", self._selected_path, e)
                self._show_error_preview(f"Failed to open bundle:\n{e}")
                return None
            finally:
                QApplication.restoreOverrideCursor()
        else:
            return self._selected_path

    # ── UI Construction ──────────────────────────────────────────

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Source toggle row — at the top so users select source before browsing
        source_row = QHBoxLayout()
        source_label = QLabel(tr("Source:"))
        source_row.addWidget(source_label)
        self._radio_s3 = QRadioButton(tr("Queue"))
        self._radio_s3.setEnabled(self._s3_available or self._s3_loading)
        self._radio_s3.setFocusPolicy(Qt.StrongFocus)
        self._radio_s3.toggled.connect(self._on_source_changed)
        source_row.addWidget(self._radio_s3)
        self._radio_local = QRadioButton(tr("Local"))
        self._radio_local.setFocusPolicy(Qt.StrongFocus)
        self._radio_local.toggled.connect(self._on_source_changed)
        source_row.addWidget(self._radio_local)
        self._radio_history = QRadioButton(tr("History"))
        self._radio_history.setFocusPolicy(Qt.StrongFocus)
        self._radio_history.setEnabled(self._history_repo is not None)
        self._radio_history.toggled.connect(self._on_source_changed)
        source_row.addWidget(self._radio_history)
        source_row.addStretch()
        layout.addLayout(source_row)

        # Inline warning when queue source is unavailable
        self._queue_warning = QLabel()
        self._queue_warning.setWordWrap(True)
        # Let users select/copy the error text (e.g. to paste an expired-token or
        # AccessDenied message into a ticket). QLabels aren't selectable by default.
        self._queue_warning.setTextInteractionFlags(
            Qt.TextSelectableByMouse | Qt.TextSelectableByKeyboard
        )
        self._queue_warning.setStyleSheet(warning_banner_qss(self))
        if not self._s3_available and self._s3_error:
            self._queue_warning.setText(
                f"\u26a0 <b>Queue browsing unavailable:</b> {html.escape(self._s3_error)}"
            )
            self._queue_warning.setTextFormat(Qt.RichText)
            self._queue_warning.setVisible(True)
        else:
            self._queue_warning.setVisible(False)
        layout.addWidget(self._queue_warning)

        # #7 — Path display sits just under the source selection (both describe
        # "where am I browsing"), rather than at the bottom by the buttons.
        path_row = QHBoxLayout()
        path_row.addWidget(QLabel(tr("Path:")))
        self._path_display = QLineEdit()
        self._path_display.setReadOnly(True)
        # Look like static text, not an editable field: drop the frame and fill so
        # it blends into the dialog. ClickFocus (rather than NoFocus) is kept so the
        # user can still click-drag to select and copy the path.
        self._path_display.setFrame(False)
        self._path_display.setFocusPolicy(Qt.ClickFocus)
        self._path_display.setStyleSheet("QLineEdit { background: transparent; border: none; }")
        path_row.addWidget(self._path_display)
        layout.addLayout(path_row)

        # Default to Queue if available or loading, otherwise Local
        if self._s3_available or self._s3_loading:
            self._radio_s3.setChecked(True)
            if self._s3_repo:
                self._current_repo = self._s3_repo
            else:
                self._current_repo = None  # Will be set by set_queue_source
        else:
            self._radio_local.setChecked(True)

        # Main splitter: tree on left, preview on right
        splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(splitter, stretch=1)

        # Left: tree view with filter
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)

        # Row 1: filter + "Show hidden" (both are list-view controls, grouped above
        # the tree). Show hidden is a filter toggle, so it lives with the filter.
        filter_row = QHBoxLayout()
        self._filter_edit = QLineEdit()
        self._filter_edit.setPlaceholderText(tr("Filter bundles..."))
        self._filter_edit.setClearButtonEnabled(True)
        self._filter_edit.textChanged.connect(self._on_filter_changed)
        filter_row.addWidget(self._filter_edit, stretch=1)

        self._show_hidden_cb = QCheckBox(tr("Show hidden"), parent=self)
        self._show_hidden_cb.setChecked(False)
        self._show_hidden_cb.toggled.connect(self._on_hidden_toggled)
        filter_row.addSpacing(8)
        filter_row.addWidget(self._show_hidden_cb)
        left_layout.addLayout(filter_row)

        self._model = QStandardItemModel()
        self._model.setHorizontalHeaderLabels([tr("Name")])

        self._proxy = _BundleFilterProxy()
        self._proxy.setSourceModel(self._model)
        self._proxy.setRecursiveFilteringEnabled(True)
        self._proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)

        self._tree = QTreeView()
        self._tree.setModel(self._proxy)
        self._tree.setHeaderHidden(True)
        self._tree.setEditTriggers(QTreeView.NoEditTriggers)
        self._tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self._tree.customContextMenuRequested.connect(self._on_context_menu)
        self._tree.expanded.connect(self._on_expanded)
        self._tree.clicked.connect(self._on_clicked)
        self._tree.doubleClicked.connect(self._on_double_clicked)
        self._tree.selectionModel().currentChanged.connect(self._on_selection_changed)
        # Overlay label for empty tree state (no bundles or no filter match)
        self._tree_empty_label = QLabel("No bundles found", self._tree.viewport())
        self._tree_empty_label.setAlignment(Qt.AlignCenter)
        self._tree_empty_label.setStyleSheet(
            "color: palette(text); font-size: 13px; background: transparent;"
        )
        self._tree_empty_label.setAttribute(Qt.WA_TransparentForMouseEvents)
        self._tree_empty_label.setVisible(False)
        self._tree.viewport().installEventFilter(self)
        self._tree.installEventFilter(self)
        left_layout.addWidget(self._tree)

        splitter.addWidget(left_widget)

        # Right: preview panel. A QStackedWidget switches between an empty-state
        # page (centered prompt) and the detail page (scrollable bundle info).
        self._muted_hex = self.palette().color(QPalette.PlaceholderText).name()
        muted_qss = f"color: {self._muted_hex};"

        self._preview_stack = QStackedWidget()

        # Empty-state page — icon + prompt + hint, centered both ways.
        empty_page = QWidget()
        empty_layout = QVBoxLayout(empty_page)
        empty_layout.setSpacing(4)

        # Large, low-opacity bundle glyph as a backdrop (matches the tree's 📦).
        empty_icon = QLabel("\U0001f4e6")
        empty_icon.setAlignment(Qt.AlignCenter)
        empty_icon.setStyleSheet("font-size: 44px;")
        empty_icon.setGraphicsEffect(self._make_opacity(0.35))

        empty_prompt = QLabel(tr("Select a job bundle"))
        empty_prompt.setAlignment(Qt.AlignCenter)
        empty_prompt.setStyleSheet("font-size: 15px; font-weight: bold;")

        self._empty_label = QLabel(tr("Choose one from the list to preview its details"))
        self._empty_label.setAlignment(Qt.AlignCenter)
        self._empty_label.setWordWrap(True)
        self._empty_label.setStyleSheet(f"font-size: 12px; {muted_qss}")

        empty_layout.addStretch(1)
        empty_layout.addWidget(empty_icon)
        empty_layout.addWidget(empty_prompt)
        empty_layout.addWidget(self._empty_label)
        empty_layout.addStretch(1)
        self._preview_stack.addWidget(empty_page)  # index 0

        # Detail page — scrollable bundle info.
        preview_widget = QWidget()
        preview_layout = QVBoxLayout(preview_widget)
        # One consistent vertical rhythm instead of scattered per-widget margins.
        preview_layout.setSpacing(6)

        # Title — top of a 3-step scale (title / body / section-label).
        self._preview_name = _WrappingLabel()
        self._preview_name.setWordWrap(True)
        # Plain text: the bundle-derived name must never be interpreted as markup.
        # Wrapping and all visual styling come from setWordWrap + the stylesheet
        # below (widget-level styling is independent of the text format); we only
        # forgo CSS break-word for a pathological unbroken name, which is bounded
        # by PREVIEW_MAX_NAME_LEN and shrinks via _WrappingLabel.
        self._preview_name.setTextFormat(Qt.PlainText)
        self._preview_name.setStyleSheet("font-weight: bold; font-size: 18px;")
        preview_layout.addWidget(self._preview_name)

        # Muted subline: bundle type + source (e.g. "📦 Folder · Local")
        self._preview_subline = QLabel()
        self._preview_subline.setStyleSheet(f"font-size: 11px; {muted_qss}")
        preview_layout.addWidget(self._preview_subline)

        # Extra breathing room between the title/subline block and the description.
        preview_layout.addSpacing(12)

        # Description — expandable, default expanded.
        self._desc_section = _ExpandableSection(expanded=True, disable_content_paddings=True)
        self._desc_section.set_header_style(f"{_SECTION_LABEL_QSS} {muted_qss}")
        self._preview_desc = _WrappingLabel()
        self._preview_desc.setWordWrap(True)
        # Plain text: bundle-derived descriptions/errors must never be interpreted
        # as rich text (defense against HTML/CSS injection from crafted metadata).
        self._preview_desc.setTextFormat(Qt.PlainText)
        # Selectable so users can copy a description or an error message shown here.
        self._preview_desc.setTextInteractionFlags(
            Qt.TextSelectableByMouse | Qt.TextSelectableByKeyboard
        )
        self._desc_section.set_content(self._preview_desc)
        preview_layout.addWidget(self._desc_section)

        preview_layout.addSpacing(8)
        # Parameters come before Steps and default to expanded — they're the most
        # commonly inspected detail before submitting.
        # Content paddings are disabled here; the table is indented via its own
        # stylesheet margin (see below) to avoid a double indent.
        self._params_section = _ExpandableSection(expanded=True, disable_content_paddings=True)
        self._params_section.set_header_style(f"{_SECTION_LABEL_QSS} {muted_qss}")
        self._preview_params = QTableWidget()
        self._preview_params.setColumnCount(3)
        self._preview_params.setHorizontalHeaderLabels([tr("Name"), tr("Type"), tr("Value")])
        header = self._preview_params.horizontalHeader()
        # Name/Type size to content but are capped (see _size_params_table below);
        # Value takes the rest and wraps long content rather than eliding/scrolling.
        header.setSectionResizeMode(0, QHeaderView.Interactive)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setHighlightSections(False)
        # Left-align the column headers to match the cell text below them.
        header.setDefaultAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self._preview_params.verticalHeader().setVisible(False)
        self._preview_params.setEditTriggers(QTableWidget.NoEditTriggers)
        self._preview_params.setSelectionMode(QTableWidget.NoSelection)
        # #2 — subtle alternating row tint so rows are easier to scan.
        self._preview_params.setAlternatingRowColors(True)
        # Wrap long Name/Value text onto multiple lines instead of eliding it.
        self._preview_params.setWordWrap(True)
        self._preview_params.setTextElideMode(Qt.ElideNone)
        self._preview_params.setShowGrid(False)
        self._preview_params.setFocusPolicy(Qt.NoFocus)
        # No frame on the table itself — the panel outline already contains it.
        self._preview_params.setFrameShape(QFrame.NoFrame)
        # #5 — header divider; header is transparent so it sits on the panel surface.
        hdr_line = self._muted_hex
        header.setStyleSheet(
            "QHeaderView::section {"
            " background: transparent;"
            f" border: none; border-bottom: 1px solid {hdr_line};"
            " padding: 4px 6px; font-weight: bold; }"
        )
        # Table is transparent (inherits the panel surface); alternating rows provide
        # the only fill, so they read as subtle stripes on the panel. The margin-left
        # indents the table under its section header (QSS margin is honored even
        # though programmatic setContentsMargins is not, once a stylesheet is set).
        self._preview_params.setStyleSheet(
            "QTableWidget { background: transparent; margin-left: 16px; }"
        )
        # Let the panel's own scroll area handle overflow; the table sizes to its rows.
        self._preview_params.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._preview_params.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._preview_params.setSizeAdjustPolicy(QTableWidget.AdjustToContents)
        self._params_section.set_content(self._preview_params)
        preview_layout.addWidget(self._params_section)

        preview_layout.addSpacing(8)
        self._steps_section = _ExpandableSection(expanded=True, disable_content_paddings=True)
        self._steps_section.set_header_style(f"{_SECTION_LABEL_QSS} {muted_qss}")
        self._preview_steps = QLabel()
        self._preview_steps.setWordWrap(True)
        # Plain text: step names come from bundle metadata and must be literal.
        self._preview_steps.setTextFormat(Qt.PlainText)
        self._steps_section.set_content(self._preview_steps)
        preview_layout.addWidget(self._steps_section)

        preview_layout.addStretch(1)

        self._clear_preview()

        # Inner padding so content doesn't crowd the panel edges.
        preview_layout.setContentsMargins(14, 14, 14, 14)

        preview_scroll = QScrollArea()
        preview_scroll.setWidget(preview_widget)
        preview_scroll.setWidgetResizable(True)
        preview_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        preview_scroll.setFrameShape(QFrame.NoFrame)
        # Make the detail page transparent so the panel's gradient shows through here
        # too (a QScrollArea + its content otherwise paint an opaque Base background).
        preview_scroll.setStyleSheet(
            "QScrollArea, QScrollArea > QWidget > QWidget { background: transparent; }"
        )
        preview_widget.setAttribute(Qt.WA_TranslucentBackground, False)
        preview_widget.setStyleSheet("background: transparent;")

        # Detail page = scrollable content + an action row pinned to the bottom.
        # "Download bundle" lives here (rather than the dialog's button box) so it
        # is anchored to the preview and only appears while a bundle is previewed.
        # It opens the bundle in the OS file explorer without closing the dialog:
        # Queue bundles are downloaded/extracted to the cache first (reusing the
        # Select flow); local/history bundles open in place.
        self._download_button = QPushButton(tr("Download bundle"))
        self._download_button.clicked.connect(self._on_download)
        self._download_button.setCursor(Qt.PointingHandCursor)
        # The preview panel is stylesheet-driven (transparent surface + gradient),
        # which flattens a default child button so it stops reading as clickable.
        # Give it an explicit, theme-aware style (fill, border, radius, hover /
        # pressed feedback) derived from the palette so it stands out on the panel.
        _pal = self.palette()
        _btn_bg = _pal.color(QPalette.Button)
        _btn_txt = _pal.color(QPalette.ButtonText)
        _wt = _pal.color(QPalette.WindowText)
        _border = f"rgba({_wt.red()}, {_wt.green()}, {_wt.blue()}, 120)"
        _is_dark = _btn_bg.lightness() < 128
        _hover_bg = _btn_bg.lighter(118) if _is_dark else _btn_bg.darker(104)
        _pressed_bg = _btn_bg.lighter(105) if _is_dark else _btn_bg.darker(112)
        self._download_button.setStyleSheet(
            "QPushButton {"
            f" background-color: {_btn_bg.name()};"
            f" color: {_btn_txt.name()};"
            f" border: 1px solid {_border};"
            " border-radius: 6px; padding: 5px 14px; font-weight: 600; }"
            f" QPushButton:hover {{ background-color: {_hover_bg.name()}; }}"
            f" QPushButton:pressed {{ background-color: {_pressed_bg.name()}; }}"
        )
        download_row = QHBoxLayout()
        download_row.setContentsMargins(14, 4, 14, 12)
        download_row.addStretch(1)
        download_row.addWidget(self._download_button)

        detail_page = QWidget()
        detail_layout = QVBoxLayout(detail_page)
        detail_layout.setContentsMargins(0, 0, 0, 0)
        detail_layout.setSpacing(0)
        detail_layout.addWidget(preview_scroll, 1)
        detail_layout.addLayout(download_row)
        detail_page.setStyleSheet("background: transparent;")
        self._preview_stack.addWidget(detail_page)  # index 1

        # #4 + #5 — the whole panel is a raised, rounded surface distinct from the
        # tree, with a subtle top-to-bottom gradient for a bit of depth. Both stops
        # are derived from QPalette.Base so it adapts to the theme. objectName
        # scoping keeps the styling off child widgets.
        base = self.palette().color(QPalette.Base)
        is_dark = base.lightness() < 128
        # Lighten the top / darken the bottom. Kept subtle: enough to add depth but
        # gentle enough not to compete with the dense detail content for readability.
        # (Near-black Base needs a larger % shift than a light Base to be visible.)
        top = base.lighter(128) if is_dark else base.lighter(104)
        bottom = base.darker(110) if is_dark else base.darker(106)
        tc = self.palette().color(QPalette.WindowText)
        border = f"rgba({tc.red()}, {tc.green()}, {tc.blue()}, 70)"
        self._preview_stack.setObjectName("previewPanel")
        self._preview_stack.setStyleSheet(
            "#previewPanel {"
            " background: qlineargradient(x1:0, y1:0, x2:0, y2:1,"
            f" stop:0 {top.name()}, stop:1 {bottom.name()});"
            f" border: 1px solid {border}; border-radius: 8px; }}"
        )
        splitter.addWidget(self._preview_stack)
        splitter.setSizes([350, 350])

        # Dialog buttons
        self._button_box = QDialogButtonBox(QDialogButtonBox.Cancel)
        self._select_button = QPushButton(tr("Select"))
        self._select_button.setDefault(True)
        self._select_button.setEnabled(False)
        self._button_box.addButton(self._select_button, QDialogButtonBox.AcceptRole)
        self._select_button.clicked.connect(self.accept)
        self._button_box.rejected.connect(self.reject)
        layout.addWidget(self._button_box)

        # Tab order: Source radios → Filter → Tree → Select
        self.setTabOrder(self._radio_s3, self._filter_edit)
        self.setTabOrder(self._filter_edit, self._tree)
        self.setTabOrder(self._tree, self._select_button)

    # ── Tree Population ──────────────────────────────────────────

    def _refresh_s3_async(self):
        """Refresh S3 listing in a background thread."""
        from concurrent.futures import ThreadPoolExecutor

        # Wait for any previous refresh to finish
        if hasattr(self, "_s3_refresh_worker") and self._s3_refresh_worker is not None:
            self._s3_refresh_worker.wait()

        assert self._s3_repo is not None
        repo = self._s3_repo

        class _Worker(QThread):
            # NOTE: named ``done`` rather than ``finished`` to avoid shadowing
            # QThread's built-in ``finished`` signal.
            done = Signal(list, set)
            error = Signal(str)

            def run(self):
                try:
                    with ThreadPoolExecutor(max_workers=2) as ex:
                        entries_f = ex.submit(repo.list_entries, repo.root_path())
                        hidden_f = ex.submit(repo.get_hidden_set)
                        self.done.emit(entries_f.result(), hidden_f.result())
                except Exception as e:
                    # Without this, an exception (e.g. expired/invalid credentials
                    # raised by the S3 list call) would kill the thread silently
                    # and leave the tree stuck on "Loading..." forever.
                    self.error.emit(str(e))

        self._s3_refresh_worker = _Worker()
        self._s3_refresh_worker.done.connect(self._on_s3_refresh_done)
        self._s3_refresh_worker.error.connect(self._on_s3_refresh_error)
        self._s3_refresh_worker.start()

    def _on_s3_refresh_done(self, entries, hidden_set):
        """Handle background S3 refresh completion."""
        if not self._radio_s3.isChecked():
            return  # User switched away
        self._cached_root_entries = _folders_first(entries)
        self._hidden_set = hidden_set
        self._populate_tree_from_cache()
        self._restore_tree_state()
        # Listing is now on screen (fast: one list + the local visibility file).
        # Warm the preview cache in the background so opening a bundle is instant.
        self._start_preview_prefetch()

    def _start_preview_prefetch(self):
        """Warm the repo's preview cache off the UI thread (best-effort)."""
        if self._s3_repo is None:
            return
        # Let any previous prefetch finish first.
        prev = self._prefetch_worker
        if prev is not None:
            prev.wait()

        repo = self._s3_repo
        self._prefetch_cancelled = False
        should_cancel = lambda: self._prefetch_cancelled  # noqa: E731

        class _PrefetchWorker(QThread):
            def run(self):
                try:
                    repo.prefetch_previews(should_cancel=should_cancel)
                except Exception:
                    # Prefetch is a pure optimization — never surface its failures;
                    # previews fall back to an on-demand HEAD.
                    logger.debug("Preview prefetch failed", exc_info=True)

        self._prefetch_worker = _PrefetchWorker()
        self._prefetch_worker.start()

    def _on_s3_refresh_error(self, message: str):
        """Handle a failed queue listing (e.g. expired credentials).

        Surfaces the underlying error in the inline banner and clears the
        "Loading..." placeholder, instead of leaving the browser stuck.
        """
        logger.warning("Failed to refresh queue bundles: %s", message)
        if not self._radio_s3.isChecked():
            return  # User switched away before the failure arrived
        self._cached_root_entries = []
        self._model.clear()
        self._model.setHorizontalHeaderLabels([tr("Name")])
        self._show_queue_warning(message)
        if hasattr(self, "_tree_empty_label"):
            self._tree_empty_label.setText("Could not load queue bundles")
            self._tree_empty_label.setVisible(True)

    def _populate_root(self):
        self._model.clear()
        self._model.setHorizontalHeaderLabels([tr("Name")])
        if self._current_repo is None:
            # Queue source still loading
            self._path_display.setText("")
            self._cached_root_entries = []
            if hasattr(self, "_tree_empty_label"):
                self._tree_empty_label.setText("Loading...")
                self._tree_empty_label.setVisible(True)
            return
        root_path = self._current_repo.root_path()
        self._path_display.setText(root_path)
        try:
            self._cached_root_entries = self._current_repo.list_entries(root_path)
        except Exception as e:
            logger.warning("Failed to list bundles: %s", e, exc_info=True)
            self._show_error_preview(f"Failed to list bundles:\n{e}")
            self._cached_root_entries = []

        # Fetch S3 hidden set for Queue source
        self._hidden_set = set()
        if isinstance(self._current_repo, _S3BundleRepository):
            try:
                self._hidden_set = self._current_repo.get_hidden_set()
            except Exception:
                logger.debug("Failed to fetch hidden bundle set", exc_info=True)

        # Folders first, then bundles — each group already name-sorted by the repo.
        self._cached_root_entries = _folders_first(self._cached_root_entries)

        root = self._model.invisibleRootItem()
        for entry in self._cached_root_entries:
            is_hidden = self._entry_hidden(entry)
            self._add_entry_item(root, entry, is_hidden=is_hidden)

        self._preload_first_level()
        self._update_tree_empty_state()

    def _populate_tree_from_cache(self):
        """Populate tree from pre-fetched _cached_root_entries and _hidden_set."""
        self._model.clear()
        self._model.setHorizontalHeaderLabels([tr("Name")])
        self._path_display.setText(self._current_repo.root_path())
        root = self._model.invisibleRootItem()
        for entry in self._cached_root_entries:
            is_hidden = self._entry_hidden(entry)
            self._add_entry_item(root, entry, is_hidden=is_hidden)
        self._preload_first_level()
        self._update_tree_empty_state()

    def _entry_hidden(self, entry) -> bool:
        """Whether an entry should be treated as hidden (dimmed/filtered).

        Dot-prefixed names are always hidden. Otherwise membership is by the
        queue's prefix-relative visibility key (so ``maya/render`` and
        ``nuke/render`` are distinct); the hidden set is only populated for the
        Queue source.
        """
        if entry.name.startswith("."):
            return True
        if not self._hidden_set:
            return False
        if isinstance(self._current_repo, _S3BundleRepository):
            return self._current_repo.visibility_key(entry.path) in self._hidden_set
        return entry.name in self._hidden_set

    def _apply_hidden_style(self, item: QStandardItem, hidden: bool) -> None:
        """Apply (or clear) the dimmed styling that marks a hidden bundle.

        When un-hiding, the foreground override is cleared (set to ``None``) so the
        item falls back to the palette's text color. Hard-coding a color such as
        black would render the name nearly invisible under a dark theme.
        """
        if hidden:
            item.setForeground(QColor(150, 150, 150))
        else:
            item.setData(None, Qt.ForegroundRole)

    def _add_entry_item(
        self, parent_item: QStandardItem, entry: _BrowseEntry, *, is_hidden: bool = False
    ):
        item = QStandardItem(self._entry_display(entry))
        item.setData(entry.path, ROLE_PATH)
        item.setData(entry.is_bundle, ROLE_IS_BUNDLE)
        item.setData(False, ROLE_LOADED)
        item.setData(entry.is_archive, ROLE_IS_ARCHIVE)
        item.setData(is_hidden, ROLE_IS_HIDDEN)
        self._apply_hidden_style(item, is_hidden)
        if not entry.is_bundle:
            # Add a placeholder child so the expand arrow shows
            placeholder = QStandardItem()
            placeholder.setData(is_hidden, ROLE_IS_HIDDEN)
            item.appendRow(placeholder)
        parent_item.appendRow(item)

    @staticmethod
    def _entry_display(entry: _BrowseEntry) -> str:
        icon = "\U0001f4e6" if entry.is_bundle else "\U0001f4c1"  # 📦 or 📁
        suffix = ".ojd" if entry.is_archive and not entry.path.startswith("s3://") else ""
        return f"{icon} {entry.name}{suffix}"

    # ── Event Handlers ───────────────────────────────────────────

    def _source_item(self, proxy_index: QModelIndex):
        """Map a proxy model index to the source model item."""
        source_index = self._proxy.mapToSource(proxy_index)
        return self._model.itemFromIndex(source_index)

    def _on_expanded(self, proxy_index: QModelIndex):
        self._load_children(self._source_item(proxy_index))

    def _load_children(self, item, entries=None) -> None:
        """Populate a folder item's real children, replacing its placeholder.

        Idempotent: a no-op for bundles or folders already loaded (so re-expanding,
        or expanding a folder whose children were preloaded, doesn't re-fetch).

        ``entries`` may be supplied by a background preloader so the (potentially
        network) listing happens off the UI thread; when ``None`` the listing is
        done inline (cheap for local/history; a single user-driven expand for S3).
        """
        if not item or item.data(ROLE_IS_BUNDLE) or item.data(ROLE_LOADED):
            return
        # Mark as loaded and replace placeholder with real children
        item.setData(True, ROLE_LOADED)
        item.removeRows(0, item.rowCount())
        path = item.data(ROLE_PATH)
        if entries is None:
            try:
                entries = self._current_repo.list_entries(path)
            except Exception as e:
                logger.warning("Failed to list bundles in %s: %s", path, e, exc_info=True)
                error_item = QStandardItem(f"\u26a0 Error: {e}")
                error_item.setEnabled(False)
                item.appendRow(error_item)
                return
        for entry in _folders_first(entries):
            is_hidden = self._entry_hidden(entry)
            # If parent is hidden, children inherit hidden state
            if item.data(ROLE_IS_HIDDEN):
                is_hidden = True
            self._add_entry_item(item, entry, is_hidden=is_hidden)

    def _preload_first_level(self) -> None:
        """Eagerly load the immediate children of each top-level folder.

        This lets the filter match one level below the root without the user first
        expanding folders. Deeper levels stay lazy-loaded on expand.

        For Local/History the listing is a cheap disk read, done inline. For the
        Queue source each listing is an S3 ``list_objects_v2``, so they are issued
        in parallel on a worker thread and applied on the UI thread — never
        blocking the event loop with N sequential round-trips (AGENTS.md: never
        call AWS on the main Qt thread).
        """
        root = self._model.invisibleRootItem()
        folder_items = [
            root.child(row)
            for row in range(root.rowCount())
            if root.child(row) is not None and not root.child(row).data(ROLE_IS_BUNDLE)
        ]
        if not folder_items:
            return

        if not self._radio_s3.isChecked():
            for child in folder_items:
                self._load_children(child)
            return

        repo = self._current_repo
        folder_paths = [it.data(ROLE_PATH) for it in folder_items]

        class _PreloadWorker(QThread):
            # NOTE: named ``done`` rather than ``finished`` to avoid shadowing
            # QThread's built-in ``finished`` signal.
            done = Signal(object)  # {folder_path: [BrowseEntry]}

            def run(self):
                from concurrent.futures import ThreadPoolExecutor

                results: dict = {}
                with ThreadPoolExecutor(max_workers=8) as ex:
                    futs = {p: ex.submit(repo.list_entries, p) for p in folder_paths}
                    for p, fut in futs.items():
                        try:
                            results[p] = fut.result()
                        except Exception:
                            logger.debug("Failed to preload children for %s", p, exc_info=True)
                self.done.emit(results)

        worker = _PreloadWorker()

        def _on_done(children):
            self._preload_workers.discard(worker)
            # Ignore if the user switched away from the Queue source or the repo
            # was swapped out while the listing was in flight.
            if not self._radio_s3.isChecked() or self._current_repo is not repo:
                return
            self._apply_preloaded_children(children)

        worker.done.connect(_on_done, Qt.QueuedConnection)
        self._preload_workers.add(worker)
        worker.start()

    def _apply_preloaded_children(self, children: dict) -> None:
        """Apply background-fetched first-level children to their folder items."""
        root = self._model.invisibleRootItem()
        for row in range(root.rowCount()):
            item = root.child(row)
            if item is None or item.data(ROLE_IS_BUNDLE):
                continue
            path = item.data(ROLE_PATH)
            if path in children:
                self._load_children(item, entries=children[path])

    def _on_clicked(self, proxy_index: QModelIndex):
        self._update_selection(proxy_index)

    def _on_double_clicked(self, proxy_index: QModelIndex):
        item = self._source_item(proxy_index)
        if item and item.data(ROLE_IS_BUNDLE):
            self._update_selection(proxy_index)
            self.accept()

    def _on_selection_changed(self, current: QModelIndex, previous: QModelIndex):
        self._update_selection(current)

    def _show_queue_warning(self, message: str) -> None:
        """Show the inline 'Queue browsing unavailable' banner with ``message``.

        Used both for background initialization failures and for a failed queue
        listing refresh (e.g. expired/invalid credentials), so the reason is
        surfaced instead of leaving the tree stuck on 'Loading...'.
        """
        self._s3_error = message
        self._queue_warning.setText(
            f"\u26a0 <b>Queue browsing unavailable:</b> {html.escape(message)}"
        )
        self._queue_warning.setTextFormat(Qt.RichText)
        self._queue_warning.setVisible(bool(message))

    def _on_download(self) -> None:
        """Open the selected bundle in the OS file explorer, keeping the dialog open.

        Queue (S3) bundles are downloaded — and archives extracted — to the local
        cache first, reusing the same flow as Select (so a large download shows
        the progress dialog and can be cancelled). Local and history bundles are
        opened in place. Errors are surfaced inline in the preview panel rather
        than as popups.
        """
        if not self._selected_path:
            return
        try:
            local_path = self.resolve_selection()
        except Exception as e:
            logger.warning("Failed to resolve bundle for download: %s", e, exc_info=True)
            self._show_error_preview(f"\u26a0 Could not open bundle: {e}")
            return
        if not local_path:
            # Cancelled, or a download error was already surfaced — nothing to open.
            return
        try:
            self._open_in_file_explorer(local_path)
        except Exception as e:
            logger.warning("Failed to open %s in file explorer: %s", local_path, e, exc_info=True)
            self._show_error_preview(f"\u26a0 Could not open bundle location: {e}")

    @staticmethod
    def _open_in_file_explorer(path: str) -> None:
        """Reveal a local path in the platform file explorer.

        Uses list-form invocation (no shell) so a path is never interpreted as a
        command. ``path`` is a local directory produced by ``resolve_selection``.
        """
        if sys.platform == "darwin":
            subprocess.run(["open", path], check=False)
        elif sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore[attr-defined]  # Windows-only
        else:
            subprocess.run(["xdg-open", path], check=False)

    def _on_filter_changed(self, text: str):
        self._proxy.setFilterFixedString(text)
        if text:
            self._tree.expandAll()
        elif not self._selected_path:
            # Collapse back to the default root view once the filter is cleared and
            # nothing is chosen (filtering had expanded everything to reveal
            # matches). Deferred to the next event-loop turn so it never runs while
            # the proxy is mid-refilter or inside a nested selection/clear — doing
            # tree structure ops in that window can touch a stale index and crash
            # (segfault). If a bundle IS selected, keep the expansion so it stays
            # visible in its folder context.
            QTimer.singleShot(0, self._tree.collapseAll)
        self._update_tree_empty_state()

    def _update_tree_empty_state(self):
        """Show/hide the 'No bundles found' overlay based on visible row count."""
        has_rows = self._proxy.rowCount() > 0
        self._tree_empty_label.setVisible(not has_rows)
        if not has_rows:
            self._tree_empty_label.setText("No bundles found")
            self._tree_empty_label.resize(self._tree.viewport().size())

    def eventFilter(self, obj, event):  # type: ignore[override]
        if obj is self._tree.viewport() and event.type() == event.Type.Resize:
            self._tree_empty_label.resize(event.size())
        elif obj is self._tree and event.type() == event.Type.FocusIn:
            if not self._tree.currentIndex().isValid() and self._proxy.rowCount() > 0:
                self._tree.setCurrentIndex(self._proxy.index(0, 0))
        return super().eventFilter(obj, event)

    def _update_selection(self, proxy_index: QModelIndex):
        item = self._source_item(proxy_index)
        if not item:
            self._clear_preview()
            self._select_button.setEnabled(False)
            self._selected_path = None
            return

        path = item.data(ROLE_PATH)
        is_bundle = item.data(ROLE_IS_BUNDLE)
        self._path_display.setText(path)

        if is_bundle:
            self._selected_path = path
            self._selected_is_s3 = self._radio_s3.isChecked()
            self._selected_is_archive = bool(item.data(ROLE_IS_ARCHIVE))
            self._select_button.setEnabled(True)
            if path != self._last_preview_path:
                self._load_preview(path, item)
        else:
            self._selected_path = None
            self._select_button.setEnabled(False)
            self._selected_is_archive = False
            self._clear_preview()
            # Auto-expand folders when clicked — clear filter first so children are visible
            if self._filter_edit.text():
                folder_path = item.data(ROLE_PATH)
                self._filter_edit.clear()
                # Defer select+expand to after Qt processes the filter change
                QTimer.singleShot(0, lambda p=folder_path: self._select_and_expand_path(p))
            else:
                if not self._tree.isExpanded(proxy_index):
                    self._tree.expand(proxy_index)

    def _select_and_expand_path(self, path: str):
        """Find an item by path in the proxy model, select it, and expand it."""
        proxy_index = self._find_proxy_index_by_path(path)
        if proxy_index and proxy_index.isValid():
            self._tree.setCurrentIndex(proxy_index)
            self._tree.expand(proxy_index)
            self._tree.scrollTo(proxy_index, QTreeView.PositionAtTop)

    def _find_proxy_index_by_path(self, path: str) -> Optional[QModelIndex]:
        """Walk the source model to find an item by ROLE_PATH, return its proxy index."""

        def _search(parent_item):
            for row in range(parent_item.rowCount()):
                child = parent_item.child(row)
                if child and child.data(ROLE_PATH) == path:
                    return self._proxy.mapFromSource(child.index())
                result = _search(child)
                if result:
                    return result
            return None

        return _search(self._model.invisibleRootItem())

    def _on_source_changed(self, checked: bool):
        if not self._ready:
            return
        # Save expanded paths for the current source before switching
        self._save_tree_state()

        if self._radio_local.isChecked():
            self._current_repo = self._local_repo
        elif self._radio_s3.isChecked() and self._s3_repo:
            self._current_repo = self._s3_repo
        elif self._radio_s3.isChecked() and self._s3_loading:
            self._current_repo = None  # Will be populated when set_queue_source arrives
        elif self._radio_history.isChecked() and self._history_repo:
            self._current_repo = self._history_repo
        self._selected_path = None
        self._select_button.setEnabled(False)
        self._clear_preview()

        # For S3, refresh in background to avoid blocking the UI
        if isinstance(self._current_repo, _S3BundleRepository):
            self._model.clear()
            self._model.setHorizontalHeaderLabels([tr("Name")])
            self._path_display.setText(self._current_repo.root_path())
            self._tree_empty_label.setText("Loading...")
            self._tree_empty_label.setVisible(True)
            self._refresh_s3_async()
        else:
            self._populate_root()

        # Restore expanded paths for the new source
        self._restore_tree_state()

    def _save_tree_state(self):
        """Save expanded folder paths and selection for the current source."""
        if self._current_repo is None:
            return
        key = self._current_repo.root_path()

        # Save selection
        if self._selected_path:
            self._tree_selections[key] = self._selected_path
        elif key in self._tree_selections:
            del self._tree_selections[key]

        # Save expanded folders
        expanded: set[str] = set()

        def _collect(parent_index):
            for row in range(self._proxy.rowCount(parent_index)):
                idx = self._proxy.index(row, 0, parent_index)
                if self._tree.isExpanded(idx):
                    source_idx = self._proxy.mapToSource(idx)
                    item = self._model.itemFromIndex(source_idx)
                    if item:
                        path = item.data(ROLE_PATH)
                        if path:
                            expanded.add(path)
                    _collect(idx)

        _collect(self._tree.rootIndex())
        key = self._current_repo.root_path()
        if expanded:
            self._tree_states[key] = expanded
        elif key in self._tree_states:
            del self._tree_states[key]

    def _restore_tree_state(self):
        """Restore previously expanded folders and selection for the current source."""
        if self._current_repo is None:
            return
        key = self._current_repo.root_path()
        expanded = self._tree_states.get(key)
        if expanded:

            def _expand(parent_index):
                for row in range(self._proxy.rowCount(parent_index)):
                    idx = self._proxy.index(row, 0, parent_index)
                    source_idx = self._proxy.mapToSource(idx)
                    item = self._model.itemFromIndex(source_idx)
                    if item and item.data(ROLE_PATH) in expanded:
                        self._tree.expand(idx)
                        _expand(idx)

            _expand(self._tree.rootIndex())

        # Restore selection
        saved_path = self._tree_selections.get(key)
        if saved_path:
            proxy_index = self._find_proxy_index_by_path(saved_path)
            if proxy_index and proxy_index.isValid():
                self._tree.setCurrentIndex(proxy_index)
                self._tree.scrollTo(proxy_index)

    def _on_hidden_toggled(self, checked: bool):
        if not self._ready:
            return
        self._proxy.set_show_hidden(checked)

    def _on_context_menu(self, position):
        """Show hide/unhide context menu for Queue source bundles."""
        if not isinstance(self._current_repo, _S3BundleRepository):
            return
        proxy_index = self._tree.indexAt(position)
        if not proxy_index.isValid():
            return
        source_index = self._proxy.mapToSource(proxy_index)
        item = self._model.itemFromIndex(source_index)
        if not item or not item.data(ROLE_IS_BUNDLE):
            return

        path = item.data(ROLE_PATH)
        # Key by the prefix-relative path, not the bare leaf name, so hiding
        # maya/render.ojd doesn't also hide nuke/render.ojd.
        key = self._current_repo.visibility_key(path)
        is_hidden = bool(item.data(ROLE_IS_HIDDEN))

        menu = QMenu(self)
        if is_hidden:
            action = menu.addAction(tr("Unhide bundle"))
        else:
            action = menu.addAction(tr("Hide bundle"))

        chosen = menu.exec_(self._tree.viewport().mapToGlobal(position))
        if chosen != action:
            return

        try:
            self._current_repo.set_bundle_visibility(key, hidden=not is_hidden)
            item.setData(not is_hidden, ROLE_IS_HIDDEN)
            self._apply_hidden_style(item, not is_hidden)
            if not is_hidden:
                self._hidden_set.add(key)
            else:
                self._hidden_set.discard(key)
            self._proxy.invalidateFilter()
        except Exception as e:
            logger.warning("Failed to update visibility: %s", e, exc_info=True)
            self._show_error_preview(
                f"\u26a0 Could not {'hide' if not is_hidden else 'unhide'} bundle: {e}"
            )

    # ── Preview ──────────────────────────────────────────────────

    def _load_preview(self, path: str, item: Optional[QStandardItem] = None):
        self._last_preview_path = path
        self._preview_request_path = path
        repo = self._current_repo

        # Local/History previews read the template from disk — cheap, so keep the
        # common case synchronous.
        if not self._radio_s3.isChecked():
            try:
                info = repo.get_bundle_info(path)
            except Exception as e:
                self._on_preview_error(path, str(e), item)
                return
            self._on_preview_ready(path, info, item)
            return

        # Queue previews may issue a HEAD or, for objects with no ojd-* metadata,
        # download and extract the whole archive. Never do that on the Qt main
        # thread (AGENTS.md); run it on a worker and render from the result.
        self._show_loading_preview()

        class _PreviewWorker(QThread):
            # NOTE: named ``done`` rather than ``finished`` to avoid shadowing
            # QThread's built-in ``finished`` signal.
            done = Signal(object)
            error = Signal(str)

            def run(self):
                try:
                    self.done.emit(repo.get_bundle_info(path))
                except Exception as e:
                    self.error.emit(str(e))

        worker = _PreviewWorker()

        def _on_done(info):
            self._on_preview_ready(path, info, item)
            self._preview_workers.discard(worker)

        def _on_error(msg):
            self._on_preview_error(path, msg, item)
            self._preview_workers.discard(worker)

        worker.done.connect(_on_done, Qt.QueuedConnection)
        worker.error.connect(_on_error, Qt.QueuedConnection)
        self._preview_workers.add(worker)
        worker.start()

    def _on_preview_ready(self, path: str, info, item: Optional[QStandardItem]):
        # Ignore results for a selection the user has already moved past.
        if self._preview_request_path != path:
            return
        if not info:
            self._show_error_preview(
                "Could not read bundle template.\nThe template may be missing or malformed."
            )
            if item:
                self._mark_item_error(item)
            self._select_button.setEnabled(False)
            return
        self._render_preview(info)

    def _on_preview_error(self, path: str, message: str, item: Optional[QStandardItem]):
        if self._preview_request_path != path:
            return
        logger.warning("Failed to load bundle info for %s: %s", path, message)
        self._show_error_preview(f"Failed to load bundle info:\n{message}")
        if item:
            self._mark_item_error(item)
        self._select_button.setEnabled(False)

    def _show_loading_preview(self):
        """Show a lightweight 'loading' state while a Queue preview resolves."""
        self._preview_stack.setCurrentIndex(1)  # show detail page
        self._download_button.setVisible(False)
        self._preview_name.setText("Loading preview\u2026")
        self._preview_name.setStyleSheet("font-weight: bold; font-size: 14px;")
        self._preview_name.setVisible(True)
        self._preview_subline.setVisible(False)
        self._desc_section.setVisible(False)
        self._preview_desc.setVisible(False)
        self._steps_section.setVisible(False)
        self._preview_steps.setVisible(False)
        self._params_section.setVisible(False)
        self._preview_params.setVisible(False)

    def _render_preview(self, info):
        self._preview_stack.setCurrentIndex(1)  # show detail page
        # A real bundle is previewed — offer to reveal/fetch it. Queue bundles are
        # fetched over the network, so label it "Download bundle" and show the size
        # (from the preview's head_object) so the user sees how much will transfer.
        # Local/History bundles just open the containing folder in the OS file
        # manager, so label it "Show bundle folder" (not "Open", which reads like
        # loading the bundle into the submitter).
        self._download_button.setVisible(True)
        if self._radio_s3.isChecked():
            if info.size_bytes:
                self._download_button.setText(
                    f"{tr('Download bundle')} ({human_readable_file_size(info.size_bytes)})"
                )
            else:
                self._download_button.setText(tr("Download bundle"))
        else:
            self._download_button.setText(tr("Show bundle folder"))
        # Plain text (see label setup): the name is shown literally, so crafted
        # metadata markup is inert without needing to escape it.
        self._preview_name.setText(info.name)
        self._preview_name.setStyleSheet("font-weight: bold; font-size: 18px;")
        self._preview_name.setVisible(True)

        # Subline: bundle type + source
        type_label = tr("Archive") if self._selected_is_archive else tr("Folder")
        type_icon = "\U0001f4e6"  # \ud83d\udce6
        if self._radio_s3.isChecked():
            source_label = tr("Queue")
        elif self._radio_history.isChecked():
            source_label = tr("History")
        else:
            source_label = tr("Local")
        self._preview_subline.setText(f"{type_icon} {type_label} \u00b7 {source_label}")
        self._preview_subline.setVisible(True)

        if info.description:
            self._desc_section.set_title(tr("Description"))
            self._desc_section.setVisible(True)
            self._preview_desc.setText(_normalize_description(info.description))
            self._preview_desc.setVisible(self._desc_section.is_expanded())
        else:
            self._desc_section.setVisible(False)

        if info.step_names:
            # Detect if steps were truncated in S3 metadata
            steps_truncated = info.step_names and info.step_names[-1].endswith("...")
            steps = info.step_names[:-1] if steps_truncated else info.step_names
            # Use total_steps from metadata count if available
            if info.total_steps and info.total_steps > len(steps):
                count_str = str(info.total_steps)
            elif steps_truncated:
                count_str = f"{len(steps)}+"
            else:
                count_str = str(len(steps))
            self._steps_section.set_title(f"{tr('Steps')} ({count_str})")
            self._steps_section.setVisible(True)
            steps_text = _steps_list_text(steps)
            if steps_truncated or (info.total_steps and info.total_steps > len(steps)):
                hidden_count = (info.total_steps - len(steps)) if info.total_steps else None
                if hidden_count:
                    steps_text += f"\n  \u2026 {hidden_count} more"
                else:
                    steps_text += "\n  \u2026 more"
            self._preview_steps.setText(steps_text)
            self._preview_steps.setVisible(self._steps_section.is_expanded())
        else:
            self._steps_section.setVisible(False)
            self._preview_steps.setVisible(False)

        if info.parameters:
            muted_color = self._preview_params.palette().color(QPalette.PlaceholderText)
            # Detect if parameters were truncated in metadata
            garbled = any(
                p.get("name", "").endswith("...") or p.get("type", "").endswith("...")
                for p in info.parameters
            )
            # Drop the last entry if it's garbled from truncation
            params = info.parameters[:-1] if garbled else info.parameters
            # Truncation: either garbled suffix detected, or count metadata says more exist
            truncated = garbled or (
                info.total_parameters is not None and info.total_parameters > len(params)
            )
            # Use total_parameters from metadata count if available
            if info.total_parameters and info.total_parameters > len(params):
                count_str = str(info.total_parameters)
            elif truncated:
                count_str = f"{len(params)}+"
            else:
                count_str = str(len(params))
            self._params_section.set_title(f"{tr('Parameters')} ({count_str})")
            self._params_section.setVisible(True)
            row_count = len(params) + (1 if truncated else 0)
            self._preview_params.setRowCount(row_count)
            for row, p in enumerate(params):
                value = p.get("_display_value", "")
                # "Required" = the artist must supply it because the bundle gives no
                # default/value. Only meaningful for full template info (not S3 metadata).
                is_required = not value and "default" not in p and "value" not in p

                # Name is plain; the "(required)" value cell carries the signal.
                self._preview_params.setItem(row, 0, QTableWidgetItem(p.get("name", "?")))

                self._preview_params.setItem(
                    row, 1, QTableWidgetItem(_friendly_param_type(p.get("type", "")))
                )

                if value:
                    value_item = QTableWidgetItem(str(value))
                elif p.get("_from_metadata"):
                    value_item = QTableWidgetItem("")
                elif is_required:
                    value_item = QTableWidgetItem(tr("(required)"))
                    value_item.setForeground(REQUIRED_COLOR)  # warm = needs input
                else:
                    value_item = QTableWidgetItem(tr("(no default)"))
                    value_item.setForeground(muted_color)
                self._preview_params.setItem(row, 2, value_item)
            if truncated:
                hidden_count = (
                    (info.total_parameters - len(params)) if info.total_parameters else None
                )
                if hidden_count:
                    msg = f"\u2026 {hidden_count} more"
                else:
                    msg = "\u2026 more"
                truncation_item = QTableWidgetItem(msg)
                truncation_item.setForeground(muted_color)
                self._preview_params.setItem(len(params), 0, truncation_item)
                self._preview_params.setSpan(len(params), 0, 1, 3)
            self._preview_params.setVisible(self._params_section.is_expanded())
            self._size_params_table_to_contents()
        else:
            self._params_section.setVisible(False)
            self._preview_params.setVisible(False)

    def _size_params_table_to_contents(self) -> None:
        """Fix the table's height to exactly its rows + header so it doesn't leave
        a large empty body, and cap the Name column so a long name can't squeeze
        the Value column. Value wraps within its remaining width."""
        # Cap the Name column at ~40% of the table width so it can't dominate.
        table_width = self._preview_params.viewport().width()
        if table_width > 0:
            name_w = self._preview_params.columnWidth(0)
            self._preview_params.setColumnWidth(0, min(name_w, int(table_width * 0.4)))
        # Recompute row heights now that wrapping/column widths are settled.
        self._preview_params.resizeRowsToContents()
        total = self._preview_params.horizontalHeader().height()
        for row in range(self._preview_params.rowCount()):
            total += self._preview_params.rowHeight(row)
        # +2 for the frame border
        self._preview_params.setFixedHeight(total + 2)

    @staticmethod
    def _make_opacity(value: float) -> QGraphicsOpacityEffect:
        """Opacity effect for a widget (QLabel CSS 'opacity' has no effect)."""
        eff = QGraphicsOpacityEffect()
        eff.setOpacity(value)
        return eff

    def _clear_preview(self):
        # Switch to the centered empty-state page. (The empty prompt is its own
        # widget, so it can't be polluted by the error state's red styling.)
        self._last_preview_path = None
        self._preview_stack.setCurrentIndex(0)

    def _mark_item_error(self, item: QStandardItem) -> None:
        """Replace the bundle/folder icon with a warning icon."""
        text = item.text()
        # Remove existing icon prefix
        for prefix in ("\U0001f4e6 ", "\U0001f4c1 ", "\u26a0 "):
            if text.startswith(prefix):
                text = text[len(prefix) :]
                break
        item.setText(f"\u26a0 {text}")

    def _show_error_preview(self, message: str):
        """Show an error message in the preview panel."""
        self._preview_stack.setCurrentIndex(1)  # show detail page
        # Nothing valid to inspect in an error state.
        self._download_button.setVisible(False)
        self._preview_name.setText("\u26a0 Error")
        self._preview_name.setStyleSheet("font-weight: bold; font-size: 14px; color: red;")
        self._preview_name.setVisible(True)
        self._preview_subline.setVisible(False)
        self._desc_section.setVisible(False)
        self._preview_desc.setText(message)
        self._preview_desc.setVisible(True)
        self._steps_section.setVisible(False)
        self._preview_steps.setVisible(False)
        self._params_section.setVisible(False)
        self._preview_params.setVisible(False)
