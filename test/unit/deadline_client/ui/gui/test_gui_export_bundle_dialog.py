# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for ExportBundleDialog."""

import pytest

try:
    from deadline.client.ui.dialogs.export_bundle_dialog import ExportBundleDialog
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.export_bundle_dialog")


class TestQueueWarningEscaping:
    """The queue-unavailable banner is RichText; a botocore error string can
    contain markup (``<``, ``&``, ARNs, server text) and must be escaped so it
    renders as text instead of being parsed as HTML (and silently swallowed, or
    rendering an ``<img>``/``<a>``)."""

    def test_queue_error_is_html_escaped(self, qtbot):
        dialog = ExportBundleDialog(
            default_name="b",
            queue_repo=None,
            queue_error="<img src=x> bucket & <prefix>",
        )
        qtbot.addWidget(dialog)

        text = dialog._queue_warning.text()
        assert "&lt;img src=x&gt;" in text
        assert "&amp;" in text
        # The raw tag must not survive into the label markup.
        assert "<img src=x>" not in text
