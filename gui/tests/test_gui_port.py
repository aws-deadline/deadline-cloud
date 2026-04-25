"""Tests for #16d Batch 1 — Foundation modules in gui/deadline/client/.

Batch 1 scope: exceptions, dataclasses, job_bundle (copied from Python),
config shim (new, routes through FFI), _compat (new, enum types).

All tests run against the ffi-test-server stub. No real AWS calls.
No Qt/PySide required.

Prerequisites: maturin develop
"""

import os
import pytest


# ── Section 1: Import paths (DCC submitter compatibility) ────────
# DCC submitters use these exact import paths. If any fails, a DCC breaks.


class TestImportPaths:
    def test_import_exceptions(self):
        from deadline.client.exceptions import DeadlineOperationError
        assert issubclass(DeadlineOperationError, Exception)

    def test_import_user_initiated_cancel(self):
        from deadline.client.exceptions import UserInitiatedCancel
        assert issubclass(UserInitiatedCancel, Exception)

    def test_import_non_valid_input_error(self):
        from deadline.client.exceptions import NonValidInputError
        assert issubclass(NonValidInputError, Exception)

    def test_import_submitter_info(self):
        from deadline.client.dataclasses import SubmitterInfo
        info = SubmitterInfo(submitter_name="Test")
        assert info.submitter_name == "Test"

    def test_import_asset_references(self):
        from deadline.client.job_bundle.submission import AssetReferences
        refs = AssetReferences()
        assert not bool(refs)

    def test_import_deadline_yaml_dump(self):
        from deadline.client.job_bundle import deadline_yaml_dump

    def test_import_create_job_history_bundle_dir(self):
        from deadline.client.job_bundle import create_job_history_bundle_dir

    def test_import_config_get_setting(self):
        from deadline.client.config import config_file
        assert callable(config_file.get_setting)

    def test_import_config_set_setting(self):
        from deadline.client.config import config_file
        assert callable(config_file.set_setting)

    def test_import_compat_types(self):
        from deadline.client._compat import (
            AwsCredentialsSource,
            AwsAuthenticationStatus,
            ProgressReportMetadata,
        )


# ── Section 2: Exceptions ────────────────────────────────────────


class TestExceptions:
    def test_operation_error_message(self):
        from deadline.client.exceptions import DeadlineOperationError
        e = DeadlineOperationError("test message")
        assert str(e) == "test message"

    def test_cancel_default_message(self):
        from deadline.client.exceptions import UserInitiatedCancel
        e = UserInitiatedCancel()
        assert "canceled" in str(e).lower()

    def test_cancel_inherits_from_operation_error(self):
        from deadline.client.exceptions import (
            DeadlineOperationError,
            UserInitiatedCancel,
        )
        assert issubclass(UserInitiatedCancel, DeadlineOperationError)

    def test_create_job_waiter_canceled(self):
        from deadline.client.exceptions import (
            CreateJobWaiterCanceled,
            DeadlineOperationCanceled,
        )
        assert issubclass(CreateJobWaiterCanceled, DeadlineOperationCanceled)


# ── Section 3: AssetReferences ───────────────────────────────────


class TestAssetReferences:
    def test_empty_is_falsy(self):
        from deadline.client.job_bundle.submission import AssetReferences
        assert not AssetReferences()

    def test_with_inputs_is_truthy(self):
        from deadline.client.job_bundle.submission import AssetReferences
        refs = AssetReferences(input_filenames={"/tmp/file.txt"})
        assert bool(refs)

    def test_from_dict_roundtrip(self):
        from deadline.client.job_bundle.submission import AssetReferences
        data = {
            "assetReferences": {
                "inputs": {
                    "filenames": ["/a.txt"],
                    "directories": ["/dir"],
                },
                "outputs": {"directories": ["/out"]},
                "referencedPaths": ["/ref"],
            }
        }
        refs = AssetReferences.from_dict(data)
        result = refs.to_dict()
        assert "/a.txt" in result["assetReferences"]["inputs"]["filenames"]
        assert "/dir" in result["assetReferences"]["inputs"]["directories"]
        assert "/out" in result["assetReferences"]["outputs"]["directories"]
        assert "/ref" in result["assetReferences"]["referencedPaths"]

    def test_from_dict_none(self):
        from deadline.client.job_bundle.submission import AssetReferences
        refs = AssetReferences.from_dict(None)
        assert not bool(refs)

    def test_union(self):
        from deadline.client.job_bundle.submission import AssetReferences
        a = AssetReferences(input_filenames={"/a.txt"})
        b = AssetReferences(input_filenames={"/b.txt"})
        merged = a.union(b)
        assert "/a.txt" in merged.input_filenames
        assert "/b.txt" in merged.input_filenames


# ── Section 4: SubmitterInfo ─────────────────────────────────────


class TestSubmitterInfo:
    def test_required_fields(self):
        from deadline.client.dataclasses import SubmitterInfo
        info = SubmitterInfo(submitter_name="Blender")
        assert info.submitter_name == "Blender"
        assert info.submitter_package_name is None

    def test_all_fields(self):
        from deadline.client.dataclasses import SubmitterInfo
        info = SubmitterInfo(
            submitter_name="Maya",
            submitter_package_name="deadline-cloud-for-maya",
            submitter_package_version="1.0.0",
            host_application_name="Maya",
            host_application_version="2025",
        )
        assert info.host_application_version == "2025"


# ── Section 5: Config shim ──────────────────────────────────────
# The config shim is the only NEW code in Batch 1. It must route
# get_setting/set_setting through FFI to Rust, preserving the same
# hierarchical scoping behavior as Python's ConfigParser-based impl.


class TestConfigShim:
    @pytest.fixture(autouse=True)
    def _use_empty_config(self, ffi, tmp_path):
        """Each test gets a fresh empty config file via env var."""
        config = tmp_path / "config"
        config.write_text("")
        self._old = os.environ.get("DEADLINE_CONFIG_FILE_PATH")
        os.environ["DEADLINE_CONFIG_FILE_PATH"] = str(config)
        yield
        if self._old is None:
            os.environ.pop("DEADLINE_CONFIG_FILE_PATH", None)
        else:
            os.environ["DEADLINE_CONFIG_FILE_PATH"] = self._old

    def test_get_setting_returns_default(self):
        """Unset setting returns its default value."""
        from deadline.client.config import config_file
        assert config_file.get_setting("defaults.farm_id") == ""

    def test_get_setting_aws_profile_default(self):
        """Default AWS profile is '(default)'."""
        from deadline.client.config import config_file
        assert config_file.get_setting("defaults.aws_profile_name") == "(default)"

    def test_set_and_get_roundtrip(self):
        """set_setting persists, get_setting reads back."""
        from deadline.client.config import config_file
        config_file.set_setting("defaults.farm_id", "farm-roundtrip")
        assert config_file.get_setting("defaults.farm_id") == "farm-roundtrip"

    def test_invalid_setting_name_raises(self):
        """Unknown setting name raises DeadlineOperationError."""
        from deadline.client.config import config_file
        from deadline.client.exceptions import DeadlineOperationError
        with pytest.raises(DeadlineOperationError):
            config_file.get_setting("not.a.real.setting")

    def test_hierarchy_profile_isolates_farm(self):
        """Switching profile isolates farm_id — Python's core scoping behavior."""
        from deadline.client.config import config_file
        # Set farm under default profile
        config_file.set_setting("defaults.farm_id", "farm-default")
        # Switch to a different profile
        config_file.set_setting("defaults.aws_profile_name", "OtherProfile")
        # Farm should be empty under the new profile
        assert config_file.get_setting("defaults.farm_id") == ""
        # Switch back — farm should be restored
        config_file.set_setting("defaults.aws_profile_name", "(default)")
        assert config_file.get_setting("defaults.farm_id") == "farm-default"

    def test_hierarchy_farm_isolates_queue(self):
        """Switching farm isolates queue_id."""
        from deadline.client.config import config_file
        config_file.set_setting("defaults.farm_id", "farm-A")
        config_file.set_setting("defaults.queue_id", "queue-A")
        # Switch farm
        config_file.set_setting("defaults.farm_id", "farm-B")
        assert config_file.get_setting("defaults.queue_id") == ""
        # Switch back
        config_file.set_setting("defaults.farm_id", "farm-A")
        assert config_file.get_setting("defaults.queue_id") == "queue-A"

    def test_hierarchy_full_chain(self):
        """Full chain: profile → farm → queue → job_id."""
        from deadline.client.config import config_file
        config_file.set_setting("defaults.farm_id", "farm-1")
        config_file.set_setting("defaults.queue_id", "queue-1")
        config_file.set_setting("defaults.job_id", "job-1")
        assert config_file.get_setting("defaults.job_id") == "job-1"
        # Change queue — job_id should reset
        config_file.set_setting("defaults.queue_id", "queue-2")
        assert config_file.get_setting("defaults.job_id") == ""

    def test_str2bool(self):
        """str2bool converts various true/false representations."""
        from deadline.client.config.config_file import str2bool
        assert str2bool("true") is True
        assert str2bool("false") is False
        assert str2bool("yes") is True
        assert str2bool("0") is False

    def test_str2bool_invalid_raises(self):
        from deadline.client.config.config_file import str2bool
        with pytest.raises(ValueError):
            str2bool("maybe")


# ── Section 6: Compat types ─────────────────────────────────────


class TestCompatTypes:
    def test_credentials_source_values(self):
        from deadline.client._compat import AwsCredentialsSource
        assert hasattr(AwsCredentialsSource, "HOST_PROVIDED")
        assert hasattr(AwsCredentialsSource, "DEADLINE_CLOUD_MONITOR_LOGIN")

    def test_authentication_status_values(self):
        from deadline.client._compat import AwsAuthenticationStatus
        assert hasattr(AwsAuthenticationStatus, "AUTHENTICATED")
        assert hasattr(AwsAuthenticationStatus, "CONFIGURATION_ERROR")
        assert hasattr(AwsAuthenticationStatus, "NEEDS_LOGIN")

    def test_progress_report_metadata(self):
        from deadline.client._compat import ProgressReportMetadata
        meta = ProgressReportMetadata(
            status="Hashing",
            progress=0.5,
            transfer_rate=1024.0,
            progress_message="50%",
            processed_files=10,
        )
        assert meta.status == "Hashing"
        assert meta.progress == 0.5
        assert meta.processed_files == 10
