# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""UI tests for ``deadline bundle gui-submit`` — submission flows.

Covers: successful submission, --output json (success and cancel),
and --submitter-name behavior.
"""

from __future__ import annotations

import os

from helpers import SubmitterDialog, last_json_object


class TestSubmitJobSuccess:
    """A successful submission hits the mock backend and writes a bundle."""

    def test_submit_creates_job_history_bundle(
        self, bundle_dir, submitter_env, deadline_env
    ) -> None:
        backend, _ = deadline_env
        job_history_dir = submitter_env["_JOB_HISTORY_DIR"]
        with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
            app.wait_farm_resolved()
            app.submit_and_ok()

        assert backend.call_counts.get("CreateJob", 0) == 1, backend.call_counts
        assert os.path.isdir(job_history_dir)
        found_template = any(
            fn.startswith("template.")
            for root, _dirs, files in os.walk(job_history_dir)
            for fn in files
        )
        assert found_template, "No template file found in job-history bundle"


class TestOutputJsonSuccess:
    """--output json prints SUBMITTED JSON after a successful submission."""

    def test_json_output_contains_submitted_status_and_job_id(
        self, bundle_dir, submitter_env, deadline_env
    ) -> None:
        backend, _ = deadline_env
        with SubmitterDialog.open(
            bundle_dir,
            env=submitter_env,
            extra_args=["--output", "json"],
            capture_stdio=True,
        ) as app:
            app.wait_farm_resolved()
            app.submit_and_ok()
            app.close("Cancel")
            stdout, _ = app.proc.communicate(timeout=3)

        text = stdout.decode() if isinstance(stdout, bytes) else stdout
        payload = last_json_object(text)
        assert payload["status"] == "SUBMITTED", payload
        assert payload["jobId"].startswith("job-")
        assert "jobHistoryBundleDirectory" in payload
        assert backend.call_counts.get("CreateJob", 0) == 1


class TestOutputJsonCancel:
    """--output json prints CANCELED JSON when submission is canceled."""

    def test_json_output_reports_canceled(self, bundle_dir, submitter_env, deadline_env) -> None:
        backend, _ = deadline_env
        backend.create_job_delay = 3.0
        with SubmitterDialog.open(
            bundle_dir,
            env=submitter_env,
            extra_args=["--output", "json"],
            capture_stdio=True,
        ) as app:
            app.wait_farm_resolved()
            app.submit_then_cancel()
            app.dismiss_progress_close()
            app.close("Cancel")
            stdout, _ = app.proc.communicate(timeout=5)

        text = stdout.decode() if isinstance(stdout, bytes) else stdout
        payload = last_json_object(text)
        assert payload == {"status": "CANCELED"}, payload


class TestSubmitterName:
    """--submitter-name changes the window title and auto-closes on submit."""

    def test_window_title_includes_submitter_name(self, bundle_dir, submitter_env) -> None:
        dialog_title = "Deadline Cloud Testing Submitter"
        with SubmitterDialog.open(
            bundle_dir,
            env=submitter_env,
            extra_args=["--submitter-name", "Testing"],
            dialog_name=dialog_title,
        ) as app:
            assert app.dialog().element().visible
            assert app.dialog_name == dialog_title

    def test_submit_exits_process(self, bundle_dir, submitter_env) -> None:
        """With --submitter-name set, close-on-success is enabled."""
        dialog_title = "Deadline Cloud Testing Submitter"
        with SubmitterDialog.open(
            bundle_dir,
            env=submitter_env,
            extra_args=["--submitter-name", "Testing"],
            dialog_name=dialog_title,
        ) as app:
            app.wait_farm_resolved()
            app.submit_and_ok()
            rc = app.proc.wait(timeout=5)
            assert rc == 0, f"unexpected exit code {rc}"
