"""Tests for #16d Batches 2-4 — UI module port into gui/deadline/client/ui/.

Coverage strategy:
- Section 1: Import paths — every public symbol DCC submitters use
- Section 2: New _compat.py additions (enums, str2bool)
- Section 3: New FFI methods (get_farm, get_queue) via stub server
- Section 4: Pure dataclass tests (no Qt)
- Section 5: Rewire audit — source grep for banned imports
- Section 6: Translations — locale key consistency
- Section 7: Controller rewiring (Qt, mocks _ffi not api)
- Section 8: Job submission worker rewiring (Qt)
- Section 9: Top-level submitter import paths

Prerequisites: maturin develop
"""

import os
import pytest


# ── Section 1: Import paths for ui/ modules ──────────────────────
# DCC submitters use these exact import paths.


class TestUIImportPaths:
    """Every public import path from ui/ that DCC submitters depend on."""

    # Root ui/ package
    def test_import_ui_init(self):
        from deadline.client.ui import block_signals, gui_error_handler

    def test_import_utils_tr(self):
        from deadline.client.ui._utils import tr
        assert callable(tr)

    def test_import_utils_block_signals(self):
        from deadline.client.ui._utils import block_signals

    def test_import_utils_gui_error_handler(self):
        from deadline.client.ui._utils import gui_error_handler

    def test_import_utils_cancelation_flag(self):
        from deadline.client.ui import CancelationFlag

    # dataclasses/
    def test_import_dataclasses_job_bundle_settings(self):
        from deadline.client.ui.dataclasses import JobBundleSettings
        s = JobBundleSettings()
        assert s.name == "Job bundle"

    def test_import_dataclasses_cli_job_settings(self):
        from deadline.client.ui.dataclasses import CliJobSettings
        s = CliJobSettings()
        assert s.name == "CLI job"

    def test_import_dataclasses_host_requirements(self):
        from deadline.client.ui.dataclasses import (
            HostRequirements, OsRequirements, HardwareRequirements,
            CustomRequirements, CustomAmountRequirement, CustomAttributeRequirement,
        )

    def test_import_dataclasses_timeouts(self):
        from deadline.client.ui.dataclasses.timeouts import TimeoutEntry, TimeoutTableEntries

    def test_import_dataclasses_environment_info(self):
        from deadline.client.ui.dataclasses._environment_info import _EnvironmentInfo

    # dialogs/
    def test_import_dialogs_types(self):
        from deadline.client.ui.dialogs._types import JobBundlePurpose
        assert JobBundlePurpose.SUBMISSION == "submission"
        assert JobBundlePurpose.EXPORT == "export"

    def test_import_dialogs_init_exports(self):
        from deadline.client.ui.dialogs import (
            DeadlineConfigDialog, DeadlineLoginDialog,
            SubmitJobProgressDialog, SubmitJobToDeadlineDialog,
            UpdateAvailableDialog, JobBundlePurpose,
        )

    # controllers/
    def test_import_controllers_all(self):
        from deadline.client.ui.controllers import (
            AsyncTask, AsyncTaskRunner, DeadlineThreadPool,
            DeadlineUIController, WorkerSignals,
        )

    # widgets/
    def test_import_widgets_init_exports(self):
        from deadline.client.ui.widgets import (
            DirectoryPickerWidget, InputFilePickerWidget, OutputFilePickerWidget,
            DeadlineAuthenticationStatusWidget,
            HostRequirementsWidget, JobAttachmentsWidget,
            JobBundleSettingsWidget, OpenJDParametersWidget,
            SharedJobSettingsWidget,
            DeadlineFarmListComboBoxController,
            DeadlineQueueListComboBoxController,
            DeadlineStorageProfileListComboBoxController,
        )

    # Top-level submitters
    def test_import_cli_job_submitter(self):
        from deadline.client.ui.cli_job_submitter import show_cli_job_submitter

    def test_import_job_bundle_submitter(self):
        from deadline.client.ui.job_bundle_submitter import show_job_bundle_submitter

    # deadline_authentication_status
    def test_import_deadline_authentication_status(self):
        from deadline.client.ui.deadline_authentication_status import DeadlineAuthenticationStatus

    # _job_submission_worker
    def test_import_job_submission_worker(self):
        from deadline.client.ui.dialogs._job_submission_worker import JobSubmissionWorker

    # _help_dialog
    def test_import_help_dialog(self):
        from deadline.client.ui.dialogs._help_dialog import _HelpDialog


# ── Section 2: New _compat.py additions ──────────────────────────


class TestCompatAdditions:
    def test_file_conflict_resolution_values(self):
        from deadline.client._compat import FileConflictResolution
        assert FileConflictResolution.CREATE_COPY == "CREATE_COPY"
        assert FileConflictResolution.SKIP == "SKIP"
        assert FileConflictResolution.OVERWRITE == "OVERWRITE"

    def test_file_conflict_resolution_is_enum(self):
        from deadline.client._compat import FileConflictResolution
        assert len(list(FileConflictResolution)) == 3

    def test_job_attachments_file_system_values(self):
        from deadline.client._compat import JobAttachmentsFileSystem
        assert JobAttachmentsFileSystem.COPIED == "COPIED"
        assert JobAttachmentsFileSystem.VIRTUAL == "VIRTUAL"

    def test_job_attachments_file_system_is_enum(self):
        from deadline.client._compat import JobAttachmentsFileSystem
        assert len(list(JobAttachmentsFileSystem)) == 2

    def test_str2bool_true_values(self):
        from deadline.client._compat import str2bool
        for val in ("true", "True", "TRUE", "yes", "Yes", "1", "on"):
            assert str2bool(val) is True, f"str2bool({val!r}) should be True"

    def test_str2bool_false_values(self):
        from deadline.client._compat import str2bool
        for val in ("false", "False", "FALSE", "no", "No", "0", "off"):
            assert str2bool(val) is False, f"str2bool({val!r}) should be False"

    def test_str2bool_invalid_raises(self):
        from deadline.client._compat import str2bool
        with pytest.raises(ValueError):
            str2bool("maybe")

    def test_str2bool_empty_raises(self):
        from deadline.client._compat import str2bool
        with pytest.raises(ValueError):
            str2bool("")


# ── Section 3: New FFI methods (get_farm, get_queue) ─────────────


class TestFFINewMethods:
    @pytest.fixture(autouse=True)
    def _use_server(self, test_server):
        self.ffi = ffi

    def test_get_farm_returns_display_name(self):
        result = self.ffi.get_farm("farm-abc123def4567890abc123def4567890")
        assert "displayName" in result
        assert result["farmId"] == "farm-abc123def4567890abc123def4567890"

    def test_get_farm_invalid_id_raises(self):
        from deadline.client._ffi import DeadlineOperationError
        with pytest.raises(DeadlineOperationError):
            self.ffi.get_farm("farm-nonexistent")

    def test_get_queue_returns_display_name(self):
        result = self.ffi.get_queue(
            "farm-abc123def4567890abc123def4567890",
            "queue-abc123def4567890abc123def4567890",
        )
        assert "displayName" in result
        assert result["queueId"] == "queue-abc123def4567890abc123def4567890"

    def test_get_queue_invalid_id_raises(self):
        from deadline.client._ffi import DeadlineOperationError
        with pytest.raises(DeadlineOperationError):
            self.ffi.get_queue(
                "farm-abc123def4567890abc123def4567890",
                "queue-nonexistent",
            )


# ── Section 4: Pure dataclass tests (no Qt) ──────────────────────


class TestHostRequirementsSerialize:
    def test_serialize_with_objects(self):
        from deadline.client.ui.dataclasses import (
            HostRequirements, OsRequirements, HardwareRequirements,
            CustomRequirements, CustomAmountRequirement, CustomAttributeRequirement,
        )
        reqs = HostRequirements(
            os_requirements=OsRequirements(
                operating_systems=["windows"], cpu_archs=["x86_64"],
            ),
            hardware_requirements=HardwareRequirements(
                cpu_min=8, cpu_max=64, memory_min=16384, memory_max=131072,
            ),
            custom_requirements=CustomRequirements(
                amounts=[CustomAmountRequirement(name="Bugs", min=1, max=10)],
                attributes=[CustomAttributeRequirement(
                    name="pipelineFeatures", option="anyOf",
                    values=["feature1", "feature2"],
                )],
            ),
        )
        result = reqs.serialize()
        assert "amounts" in result
        assert "attributes" in result
        vcpu = [a for a in result["amounts"] if a["name"] == "amount.worker.vcpu"]
        assert len(vcpu) == 1
        assert vcpu[0]["min"] == 8
        assert vcpu[0]["max"] == 64

    def test_serialize_with_dicts(self):
        from deadline.client.ui.dataclasses import HostRequirements
        reqs = HostRequirements(
            os_requirements={"operating_systems": ["linux"], "cpu_archs": ["arm64"]},
            hardware_requirements={"cpu_min": 4, "cpu_max": 32},
            custom_requirements={"amounts": [], "attributes": []},
        )
        result = reqs.serialize()
        os_attr = [a for a in result["attributes"] if a["name"] == "attr.worker.os.family"]
        assert os_attr[0]["anyOf"] == ["linux"]

    def test_invalid_os_raises(self):
        from deadline.client.ui.dataclasses import OsRequirements
        with pytest.raises(ValueError):
            OsRequirements(operating_systems=["freebsd"], cpu_archs=["x86_64"])

    def test_invalid_arch_raises(self):
        from deadline.client.ui.dataclasses import OsRequirements
        with pytest.raises(ValueError):
            OsRequirements(operating_systems=["linux"], cpu_archs=["risc-v"])


class TestTimeouts:
    def test_valid_timeout_entry(self):
        from deadline.client.ui.dataclasses.timeouts import TimeoutEntry
        entry = TimeoutEntry(tooltip="Test", is_activated=True, seconds=3600)
        assert entry.seconds == 3600

    def test_negative_seconds_raises(self):
        from deadline.client.ui.dataclasses.timeouts import TimeoutEntry
        with pytest.raises(ValueError, match="negative or zero"):
            TimeoutEntry(tooltip="Test", seconds=-1)

    def test_zero_seconds_raises(self):
        from deadline.client.ui.dataclasses.timeouts import TimeoutEntry
        with pytest.raises(ValueError, match="negative or zero"):
            TimeoutEntry(tooltip="Test", seconds=0)

    def test_empty_tooltip_raises(self):
        from deadline.client.ui.dataclasses.timeouts import TimeoutEntry
        with pytest.raises(ValueError, match="empty"):
            TimeoutEntry(tooltip="")

    def test_table_entries_roundtrip(self):
        from deadline.client.ui.dataclasses.timeouts import TimeoutEntry, TimeoutTableEntries
        entries = TimeoutTableEntries(entries={
            "e1": TimeoutEntry(tooltip="First", is_activated=True, seconds=3600),
            "e2": TimeoutEntry(tooltip="Second", is_activated=False, seconds=7200),
        })
        d = entries.to_sticky_settings_dict()
        assert d["e1"] == {"is_activated": True, "seconds": 3600}
        assert d["e2"] == {"is_activated": False, "seconds": 7200}

    def test_table_entries_update_from_sticky(self):
        from deadline.client.ui.dataclasses.timeouts import TimeoutEntry, TimeoutTableEntries
        entries = TimeoutTableEntries(entries={
            "e1": TimeoutEntry(tooltip="First", is_activated=True, seconds=3600),
        })
        entries.update_from_sticky_settings({"e1": {"is_activated": False, "seconds": 1800}})
        assert entries.entries["e1"].is_activated is False
        assert entries.entries["e1"].seconds == 1800

    def test_table_entries_validate_zero_timeout(self):
        from deadline.client.ui.dataclasses.timeouts import TimeoutEntry, TimeoutTableEntries
        from deadline.client.exceptions import NonValidInputError
        entries = TimeoutTableEntries(entries={
            "e1": TimeoutEntry(tooltip="First", is_activated=True, seconds=3600),
        })
        entries.entries["e1"].seconds = 0
        with pytest.raises(NonValidInputError, match="e1"):
            entries.validate_entries()


# ── Section 5: Rewire audit ──────────────────────────────────────


class TestRewireAudit:
    """Source-file grep: verify no ui/ file imports banned modules."""

    @staticmethod
    def _ui_python_files():
        import pathlib
        ui_dir = pathlib.Path(__file__).resolve().parent.parent / "deadline" / "client" / "ui"
        if not ui_dir.exists():
            pytest.skip("ui/ directory not yet created")
        return list(ui_dir.rglob("*.py"))

    def test_no_from_api_imports(self):
        """No ui/ file should contain 'from ... import api' or 'from ...api.'."""
        import re
        pattern = re.compile(r"from\s+\.{2,}\s+import\s+api|from\s+\.{2,}api\.")
        violations = []
        for f in self._ui_python_files():
            content = f.read_text()
            for i, line in enumerate(content.splitlines(), 1):
                if pattern.search(line) and not line.strip().startswith("#"):
                    violations.append(f"{f.name}:{i}: {line.strip()}")
        assert not violations, "Files still import api:\n" + "\n".join(violations)

    def test_no_boto3_imports(self):
        violations = []
        for f in self._ui_python_files():
            content = f.read_text()
            for i, line in enumerate(content.splitlines(), 1):
                if ("import boto3" in line or "from botocore" in line) and not line.strip().startswith("#"):
                    violations.append(f"{f.name}:{i}: {line.strip()}")
        assert not violations, "Files still import boto3:\n" + "\n".join(violations)

    def test_no_job_attachments_imports(self):
        violations = []
        for f in self._ui_python_files():
            content = f.read_text()
            for i, line in enumerate(content.splitlines(), 1):
                if "deadline.job_attachments" in line and not line.strip().startswith("#"):
                    violations.append(f"{f.name}:{i}: {line.strip()}")
        assert not violations, "Files still import job_attachments:\n" + "\n".join(violations)


# ── Section 6: Translations ──────────────────────────────────────


class TestTranslations:
    def test_all_locales_have_same_keys(self):
        import json
        from pathlib import Path
        locales_dir = (
            Path(__file__).resolve().parent.parent
            / "deadline" / "client" / "ui" / "translations" / "locales"
        )
        if not locales_dir.exists():
            pytest.skip("translations/locales/ not yet copied")
        en_file = locales_dir / "en_US.json"
        if not en_file.exists():
            pytest.skip("en_US.json not found")
        with open(en_file) as f:
            en_keys = set(json.load(f).keys())
        for locale_file in locales_dir.glob("*.json"):
            if locale_file.name == "en_US.json":
                continue
            with open(locale_file) as f:
                locale_keys = set(json.load(f).keys())
            missing = en_keys - locale_keys
            extra = locale_keys - en_keys
            assert not missing, f"{locale_file.name} missing keys: {missing}"
            assert not extra, f"{locale_file.name} has extra keys: {extra}"


# ── Section 7: Controller rewiring (Qt) ──────────────────────────
# These verify the controller calls _ffi, not api.


@pytest.mark.qt
class TestDeadlineUIControllerRewired:
    """Verify DeadlineUIController uses FFI, not api module."""

    def setup_method(self):
        from deadline.client.ui.controllers._deadline_controller import DeadlineUIController
        from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.reset()

    def teardown_method(self):
        from deadline.client.ui.controllers._deadline_controller import DeadlineUIController
        from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
        DeadlineUIController.resetInstance()
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_singleton(self, qtbot):
        from deadline.client.ui.controllers import DeadlineUIController
        c1 = DeadlineUIController.getInstance()
        c2 = DeadlineUIController.getInstance()
        assert c1 is c2

    def test_reset_instance(self, qtbot):
        from deadline.client.ui.controllers import DeadlineUIController
        c1 = DeadlineUIController.getInstance()
        DeadlineUIController.resetInstance()
        c2 = DeadlineUIController.getInstance()
        assert c1 is not c2

    def test_initial_farm_id_empty(self, qtbot):
        from deadline.client.ui.controllers import DeadlineUIController
        c = DeadlineUIController.getInstance()
        assert c.current_farm_id == ""

    def test_initial_queue_id_empty(self, qtbot):
        from deadline.client.ui.controllers import DeadlineUIController
        c = DeadlineUIController.getInstance()
        assert c.current_queue_id == ""

    def test_set_config(self, qtbot):
        from configparser import ConfigParser
        from deadline.client.ui.controllers import DeadlineUIController
        c = DeadlineUIController.getInstance()
        config = ConfigParser()
        config["defaults"] = {"aws_profile_name": "test"}
        c.set_config(config)
        assert c.config["defaults"]["aws_profile_name"] == "test"

    def test_refresh_farms_emits_loading(self, qtbot, ffi):
        """refresh_farms should emit farms_loading(True) then farms_loading(False)."""
        from deadline.client.ui.controllers import DeadlineUIController
        c = DeadlineUIController.getInstance()
        loading_states = []
        c.farms_loading.connect(lambda x: loading_states.append(x))
        c.refresh_farms()
        qtbot.waitUntil(lambda: len(loading_states) >= 2, timeout=5000)
        assert loading_states[0] is True
        assert loading_states[-1] is False

    def test_refresh_farms_emits_farm_list(self, qtbot, ffi):
        """refresh_farms should emit farms_updated with farm data from FFI."""
        from deadline.client.ui.controllers import DeadlineUIController
        c = DeadlineUIController.getInstance()
        farms_received = []
        c.farms_updated.connect(lambda x: farms_received.append(x))
        c.refresh_farms()
        qtbot.waitUntil(lambda: len(farms_received) > 0, timeout=5000)
        # Should have at least one farm from the stub server
        assert len(farms_received) == 1
        farms = farms_received[0]
        assert len(farms) >= 1
        # Each farm is (displayName, farmId)
        names = [f[0] for f in farms]
        assert "Test Farm" in names

    def test_refresh_queues_no_farm_emits_empty(self, qtbot, ffi):
        from deadline.client.ui.controllers import DeadlineUIController
        c = DeadlineUIController.getInstance()
        queues_received = []
        c.queues_updated.connect(lambda x: queues_received.append(x))
        c.refresh_queues()
        qtbot.waitUntil(lambda: len(queues_received) > 0, timeout=2000)
        assert queues_received[0] == []

    def test_on_farm_selected_updates_id(self, qtbot, ffi):
        from deadline.client.ui.controllers import DeadlineUIController
        c = DeadlineUIController.getInstance()
        c.on_farm_selected("farm-abc123def4567890abc123def4567890")
        assert c.current_farm_id == "farm-abc123def4567890abc123def4567890"

    def test_on_farm_selected_clears_queue(self, qtbot, ffi):
        from deadline.client.ui.controllers import DeadlineUIController
        c = DeadlineUIController.getInstance()
        c._current_queue_id = "old-queue"
        c.on_farm_selected("farm-abc123def4567890abc123def4567890")
        assert c.current_queue_id == ""


# ── Section 8: Job submission worker (Qt) ─────────────────────────


@pytest.mark.qt
class TestJobSubmissionWorkerRewired:
    """Verify JobSubmissionWorker uses FFI, not api module."""

    def test_init(self, qtbot):
        from deadline.client.ui.dialogs._job_submission_worker import JobSubmissionWorker
        w = JobSubmissionWorker()
        assert w.is_canceled is False

    def test_cancel_sets_flag(self, qtbot):
        from deadline.client.ui.dialogs._job_submission_worker import JobSubmissionWorker
        w = JobSubmissionWorker()
        w.cancel()
        assert w.is_canceled is True

    def test_cancel_releases_confirmation_event(self, qtbot):
        from deadline.client.ui.dialogs._job_submission_worker import JobSubmissionWorker
        w = JobSubmissionWorker()
        w._confirmation_event.clear()
        w.cancel()
        assert w._confirmation_event.is_set()

    def test_set_confirmation_result(self, qtbot):
        from deadline.client.ui.dialogs._job_submission_worker import JobSubmissionWorker
        w = JobSubmissionWorker()
        w._confirmation_event.clear()
        w.set_confirmation_result(True)
        assert w._confirmation_result is True
        assert w._confirmation_event.is_set()


# ── Section 9: Async infrastructure (Qt) ─────────────────────────


@pytest.mark.qt
class TestAsyncTaskRewired:
    """Verify AsyncTask and WorkerSignals work in the ported code."""

    def test_worker_signals_exist(self, qtbot):
        from deadline.client.ui.controllers._async_task import WorkerSignals
        s = WorkerSignals()
        assert hasattr(s, "finished")
        assert hasattr(s, "error")
        assert hasattr(s, "result")

    def test_async_task_runs_function(self, qtbot):
        from deadline.client.ui.controllers._async_task import AsyncTask
        results = []
        task = AsyncTask(lambda: "hello")
        task.signals.result.connect(lambda x: results.append(x))
        task.run()
        assert results == ["hello"]

    def test_async_task_emits_error(self, qtbot):
        from deadline.client.ui.controllers._async_task import AsyncTask
        errors = []
        task = AsyncTask(lambda: (_ for _ in ()).throw(ValueError("boom")))
        def raise_fn():
            raise ValueError("boom")
        task2 = AsyncTask(raise_fn)
        task2.signals.error.connect(lambda e: errors.append(e))
        task2.run()
        assert len(errors) == 1
        assert str(errors[0]) == "boom"

    def test_async_task_cancel_prevents_signals(self, qtbot):
        from deadline.client.ui.controllers._async_task import AsyncTask
        results = []
        task = AsyncTask(lambda: "hello")
        task.signals.result.connect(lambda x: results.append(x))
        task.cancel()
        task.run()
        assert results == []


@pytest.mark.qt
class TestThreadPoolRewired:
    def setup_method(self):
        from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
        DeadlineThreadPool.reset()

    def teardown_method(self):
        from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
        DeadlineThreadPool.reset()

    def test_singleton(self, qtbot):
        from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
        p1 = DeadlineThreadPool.instance()
        p2 = DeadlineThreadPool.instance()
        assert p1 is p2

    def test_set_max_threads_invalid(self, qtbot):
        from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
        with pytest.raises(ValueError):
            DeadlineThreadPool.set_max_threads(0)


@pytest.mark.qt
class TestAsyncRunnerRewired:
    def setup_method(self):
        from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
        DeadlineThreadPool.reset()

    def teardown_method(self):
        from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_run_returns_operation_id(self, qtbot):
        from deadline.client.ui.controllers._async_runner import AsyncTaskRunner
        runner = AsyncTaskRunner()
        op_id = runner.run("test", lambda: "result")
        assert isinstance(op_id, int) and op_id > 0

    def test_run_calls_on_success(self, qtbot):
        from deadline.client.ui.controllers._async_runner import AsyncTaskRunner
        runner = AsyncTaskRunner()
        results = []
        runner.run("test", lambda: "hello", on_success=lambda x: results.append(x))
        qtbot.waitUntil(lambda: len(results) > 0, timeout=3000)
        assert results[0] == "hello"

    def test_run_calls_on_error(self, qtbot):
        from deadline.client.ui.controllers._async_runner import AsyncTaskRunner
        runner = AsyncTaskRunner()
        errors = []
        def fail():
            raise ValueError("boom")
        runner.run("test", fail, on_error=lambda e: errors.append(e))
        qtbot.waitUntil(lambda: len(errors) > 0, timeout=3000)
        assert str(errors[0]) == "boom"

    def test_same_key_cancels_previous(self, qtbot):
        import time
        from deadline.client.ui.controllers._async_runner import AsyncTaskRunner
        runner = AsyncTaskRunner()
        results = []
        runner.run("key", lambda: (time.sleep(0.3), "first")[1],
                   on_success=lambda x: results.append(x))
        runner.run("key", lambda: (time.sleep(0.3), "second")[1],
                   on_success=lambda x: results.append(x))
        qtbot.waitUntil(lambda: len(results) > 0, timeout=3000)
        assert results == ["second"]
