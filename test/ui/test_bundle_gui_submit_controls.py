# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI tests for ``deadline bundle gui-submit`` — dialog controls.

Exercises a representative sample of controls across the main tabs:
Priority spin box, Initial state combo, Host requirements radios,
and Job attachments tab reachability.
"""

from __future__ import annotations

from helpers import SubmitterDialog


class TestSharedJobSettingsControls:
    """Priority and initial state controls exist and have expected defaults."""

    def test_priority_spin_box_is_present(self, gui_submit: SubmitterDialog) -> None:
        assert gui_submit.locator('static_text[name="Priority"]').exists(), (
            "Priority label not found"
        )
        assert any(
            (getattr(sb, "value", "") or "") == "50"
            for sb in gui_submit.elements_by_role("spin_button")
        ), "No spin_button with the default Priority value of 50 found"

    def test_initial_state_combo_is_present(self, gui_submit: SubmitterDialog) -> None:
        assert gui_submit.locator('static_text[name="Initial state"]').exists(), (
            "Initial state label not found"
        )
        valid_states = {"READY", "SUSPENDED"}
        combos = gui_submit.elements_by_role("combo_box")
        assert any(
            (getattr(c, "name", None) or "") in valid_states
            or (getattr(c, "value", None) or "") in valid_states
            for c in combos
        ), f"No combo_box with Initial state value (READY/SUSPENDED) among {len(combos)} combos"


class TestHostRequirementsControls:
    """The host-requirements tab exposes the two top-level radio buttons."""

    def test_default_and_custom_radios_present(self, gui_submit: SubmitterDialog) -> None:
        gui_submit.activate_tab("Host requirements")
        assert gui_submit.locator('radio_button[name="Run on all available worker hosts"]').exists()
        assert gui_submit.locator(
            'radio_button[name="Run on worker hosts that meet the following requirements"]'
        ).exists()


class TestJobAttachmentsTab:
    """The job-attachments tab is reachable and renders."""

    def test_job_attachments_tab_activates(self, gui_submit: SubmitterDialog) -> None:
        gui_submit.activate_tab("Job attachments")
        assert gui_submit.dialog().element().visible
