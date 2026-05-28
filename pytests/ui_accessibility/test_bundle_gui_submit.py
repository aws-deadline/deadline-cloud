# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI tests for ``deadline bundle gui-submit`` against the mock backend."""

from __future__ import annotations

import json
import os

from helpers import SubmitterDialog


class TestSubmitterOpens:
    def test_dialog_is_visible(self, gui_submit: SubmitterDialog):
        assert gui_submit.dialog().element().visible

    def test_farm_name_resolved(self, gui_submit: SubmitterDialog):
        assert gui_submit.tree_contains_text("TestFarm")

    def test_queue_name_resolved(self, gui_submit: SubmitterDialog):
        assert gui_submit.tree_contains_text("TestQueue")

    def test_job_name_displayed(self, gui_submit: SubmitterDialog):
        assert gui_submit.job_name == "Test Render Job"

    def test_has_tabs(self, gui_submit: SubmitterDialog):
        tab_group = gui_submit.locator("tab_group")
        assert tab_group.exists()
        for tab_name in (
            "Shared job settings",
            "Job-specific settings",
            "Job attachments",
            "Host requirements",
        ):
            assert gui_submit.tab_exists(tab_name), f"Tab {tab_name!r} not found"

    def test_has_submit_and_export_buttons(self, gui_submit: SubmitterDialog):
        assert gui_submit.button("Submit").exists()
        assert gui_submit.button("Export bundle").exists()


class TestExportBundle:
    def test_export_creates_bundle(self, bundle_dir, submitter_env):
        job_history_dir = submitter_env["_JOB_HISTORY_DIR"]
        with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
            app.wait_farm_resolved()
            app.export_bundle()

            assert os.path.isdir(job_history_dir), "Job history dir was not created"
            templates = []
            for root, _dirs, files in os.walk(job_history_dir):
                for fn in files:
                    if fn.startswith("template."):
                        templates.append(os.path.join(root, fn))
            assert templates, "No template file found in exported bundle"
            with open(templates[0]) as f:
                exported = json.load(f)
            assert exported["name"] == "Test Render Job"


class TestQueueSelection:
    """Configured queue is selected even when not first alphabetically."""

    def test_configured_queue_is_selected_when_not_first(self, tmp_path, mock_backend):
        backend, deadline_url = mock_backend
        farm = backend.create_farm(displayName="TestFarm", description="")
        # Create queues where the configured one sorts AFTER another
        backend.create_queue(farmId=farm["farmId"], displayName="AAA Queue", description="")
        queue_b = backend.create_queue(farmId=farm["farmId"], displayName="ZZZ Queue", description="")

        config_file = tmp_path / "deadline.config"
        config_file.write_text(
            "[defaults]\n"
            "aws_profile_name = (default)\n"
            "\n"
            "[profile-(default) defaults]\n"
            f"farm_id = {farm['farmId']}\n"
            "\n"
            f"[profile-(default) {farm['farmId']} defaults]\n"
            f"queue_id = {queue_b['queueId']}\n"
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
                            "script": {"actions": {"onRun": {"command": "bash", "args": ["-c", "echo hi"]}}},
                        }
                    ],
                }
            )
        )

        with SubmitterDialog.open(str(bundle), env=env) as app:
            app.wait_farm_resolved()
            assert app.tree_contains_text("ZZZ Queue"), (
                "Configured queue (ZZZ Queue) should be selected, not the first alphabetically"
            )


class TestFarmSelection:
    """Configured farm is selected even when not first alphabetically."""

    def test_configured_farm_is_selected_when_not_first(self, tmp_path, mock_backend):
        backend, deadline_url = mock_backend
        # Create farms where the configured one sorts AFTER another
        backend.create_farm(displayName="AAA Farm", description="")
        farm_b = backend.create_farm(displayName="ZZZ Farm", description="")
        backend.create_queue(farmId=farm_b["farmId"], displayName="MyQueue", description="")

        config_file = tmp_path / "deadline.config"
        config_file.write_text(
            "[defaults]\n"
            "aws_profile_name = (default)\n"
            "\n"
            "[profile-(default) defaults]\n"
            f"farm_id = {farm_b['farmId']}\n"
            "\n"
            f"[profile-(default) {farm_b['farmId']} defaults]\n"
            "queue_id = \n"
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
                            "script": {"actions": {"onRun": {"command": "bash", "args": ["-c", "echo hi"]}}},
                        }
                    ],
                }
            )
        )

        with SubmitterDialog.open(str(bundle), env=env) as app:
            app.wait_farm_resolved(farm_name="ZZZ Farm")
            assert app.tree_contains_text("ZZZ Farm"), (
                "Configured farm (ZZZ Farm) should be selected, not the first alphabetically"
            )
