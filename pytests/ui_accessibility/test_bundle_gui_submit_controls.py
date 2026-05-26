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


class TestQueueParameterControls:
    """Queue environment parameters render with correct control types."""

    QUEUE_ENV_TEMPLATE = """\
specificationVersion: "environment-2023-09"
parameterDefinitions:
  - name: TextParam
    type: STRING
    default: "hello"
    userInterface:
      control: LINE_EDIT
      label: Text Parameter
  - name: DropdownParam
    type: STRING
    default: "ACTIVATE"
    allowedValues: ["ACTIVATE", "REMOVE_AND_CREATE"]
    userInterface:
      control: DROPDOWN_LIST
      label: Dropdown Parameter
  - name: NumberParam
    type: INT
    default: 600
    userInterface:
      control: SPIN_BOX
      label: Number Parameter
  - name: CheckParam
    type: STRING
    default: "False"
    allowedValues: ["True", "False"]
    userInterface:
      control: CHECK_BOX
      label: Check Parameter
  - name: HiddenParam
    type: STRING
    default: "secret"
    userInterface:
      control: HIDDEN
environment:
  name: TestEnv
  script:
    actions:
      onEnter:
        command: "bash"
        args: ["-c", "true"]
"""

    def test_all_control_types_render_correctly(self, tmp_path, mock_backend) -> None:
        """Verify LINE_EDIT, DROPDOWN_LIST, SPIN_BOX, CHECK_BOX render as
        the correct accessibility roles, and HIDDEN is not visible."""
        import json
        import os

        backend, deadline_url = mock_backend
        farm = backend.create_farm(displayName="TestFarm", description="")
        queue = backend.create_queue(
            farmId=farm["farmId"], displayName="TestQueue", description=""
        )
        backend.create_queue_environment(
            farmId=farm["farmId"],
            queueId=queue["queueId"],
            name="TestEnv",
            priority=10,
            template=self.QUEUE_ENV_TEMPLATE,
        )

        config_file = tmp_path / "deadline.config"
        config_file.write_text(
            "[defaults]\n"
            "aws_profile_name = (default)\n"
            "\n"
            "[profile-(default) defaults]\n"
            f"farm_id = {farm['farmId']}\n"
            "\n"
            f"[profile-(default) {farm['farmId']} defaults]\n"
            f"queue_id = {queue['queueId']}\n"
            "\n"
            "[profile-(default) settings]\n"
            f"job_history_dir = {tmp_path / 'history'}\n"
        )

        env = {
            **os.environ,
            "HOME": str(tmp_path / "home"),
            "AWS_ENDPOINT_URL_DEADLINE": deadline_url,
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_DEFAULT_REGION": "us-west-2",
            "DEADLINE_CONFIG_FILE_PATH": str(config_file),
            "DEADLINE_CLOUD_TELEMETRY_OPT_OUT": "true",
        }
        (tmp_path / "home").mkdir(exist_ok=True)

        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.json").write_text(
            json.dumps(
                {
                    "specificationVersion": "jobtemplate-2023-09",
                    "name": "Test",
                    "steps": [
                        {
                            "name": "Step",
                            "script": {
                                "actions": {
                                    "onRun": {"command": "bash", "args": ["-c", "echo hi"]}
                                }
                            },
                        }
                    ],
                }
            )
        )

        from helpers import SubmitterDialog
        import time

        with SubmitterDialog.open(str(bundle), env=env) as app:
            app.wait_farm_resolved()
            # Wait for queue parameters to load
            time.sleep(2)
            tree = app._app.dump()

            # LINE_EDIT: text field with "Text Parameter" label visible
            assert "Text Parameter" in tree, "LINE_EDIT label not found"

            # DROPDOWN_LIST: combo_box present with ACTIVATE value
            assert "ACTIVATE" in tree, "DROPDOWN_LIST value not found"
            combos = app.elements_by_role("combo_box")
            dropdown_found = any(
                "ACTIVATE" in (getattr(c, "value", "") or getattr(c, "name", "") or "")
                for c in combos
            )
            assert dropdown_found, f"No combo_box with ACTIVATE among {len(combos)} combos"

            # SPIN_BOX: spin_button present with value 600
            spinners = app.elements_by_role("spin_button")
            spin_found = any("600" in (getattr(s, "value", "") or "") for s in spinners)
            assert spin_found, f"No spin_button with value 600 among {len(spinners)} spinners"

            # CHECK_BOX: check_box present with "Check Parameter" label
            checks = app.elements_by_role("check_box")
            check_found = any(
                "Check Parameter" in (getattr(c, "name", "") or "")
                for c in checks
            )
            assert check_found, f"No check_box with 'Check Parameter' among {len(checks)} checkboxes"

            # HIDDEN: should NOT appear in the tree
            assert "HiddenParam" not in tree, "HIDDEN parameter should not be visible"
            assert "secret" not in tree, "HIDDEN parameter value should not be visible"
