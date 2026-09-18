# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The GUI logout entry points must mark themselves as GUI usage.

Nothing in an `api.logout()` call distinguishes it from the CLI's, so `usage_mode` on the
resulting telemetry event comes from `from_gui` and a call site that forgets it is
silently mislabelled rather than broken. These call the handlers with a mock `self` so
each call site is covered without standing up a real dialog.
"""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("qtpy")


def test_config_dialog_logout_marks_gui_usage():
    from deadline.client.ui.dialogs.deadline_config_dialog import DeadlineConfigDialog

    dialog = MagicMock()
    with patch("deadline.client.ui.dialogs.deadline_config_dialog.api.logout") as logout_mock:
        DeadlineConfigDialog.on_logout(dialog)

    assert logout_mock.call_args.kwargs["from_gui"] is True


def test_submit_job_dialog_logout_marks_gui_usage():
    from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
        SubmitJobToDeadlineDialog,
    )

    dialog = MagicMock()
    with patch(
        "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.api.logout"
    ) as logout_mock:
        SubmitJobToDeadlineDialog.on_logout(dialog)

    assert logout_mock.call_args.kwargs["from_gui"] is True
