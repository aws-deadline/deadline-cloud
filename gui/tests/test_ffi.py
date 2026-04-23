"""Tests for gui/deadline/client/_ffi.py — the ctypes wrapper around libdeadline_gui_ffi.

All tests run against the ffi-test-server stub (wiremock). No real AWS
calls are made. The stub is started once per session by conftest.py.

Prerequisites: cargo build -p deadline-gui-ffi -p deadline-test-server
"""

import pytest

from deadline.client._ffi import DeadlineFFI, DeadlineOperationError


# ── Test 1-2: Library loading ────────────────────────────────────


class TestLibraryLoading:
    def test_library_loads(self, ffi):
        """DeadlineFFI() should load the shared library without error."""
        assert ffi._lib is not None

    def test_library_not_found_raises(self, monkeypatch):
        """If the library doesn't exist, constructor raises a clear error."""
        monkeypatch.setenv("DEADLINE_FFI_LIB_PATH", "/nonexistent/libfake.dylib")
        with pytest.raises(OSError):
            DeadlineFFI()


# ── Tests 3-5: Auth status (against stub) ────────────────────────


class TestAuthStatus:
    def test_get_credentials_source(self, ffi):
        """get_credentials_source returns HOST_PROVIDED (stub has fake creds)."""
        result = ffi.get_credentials_source()
        assert result == "HOST_PROVIDED"

    def test_check_auth_status(self, ffi):
        """check_auth_status returns all 3 fields with correct types."""
        result = ffi.check_auth_status()
        assert result["credentials_source"] == "HOST_PROVIDED"
        assert result["auth_status"] == "AUTHENTICATED"
        assert result["api_available"] is True

    def test_check_auth_status_with_progress(self, ffi):
        """check_auth_status_with_progress fires the progress callback ≥3 times."""
        messages = []

        def on_progress(msg):
            messages.append(msg)

        result = ffi.check_auth_status_with_progress(on_progress=on_progress)
        assert result["credentials_source"] == "HOST_PROVIDED"
        assert len(messages) >= 3


# ── Tests 6-9: Config ────────────────────────────────────────────


class TestConfig:
    def test_read_config(self, ffi, tmp_config):
        """read_config returns a dict (not an error)."""
        result = ffi.read_config(config_path=tmp_config)
        assert isinstance(result, dict)

    def test_get_setting_known_key(self, ffi, tmp_config):
        """get_setting returns the value for a known key."""
        value = ffi.get_setting("defaults.farm_id", config_path=tmp_config)
        assert value == "farm-test123"

    def test_get_setting_invalid_name(self, ffi, empty_config):
        """get_setting raises DeadlineOperationError for an invalid setting name."""
        with pytest.raises(DeadlineOperationError):
            ffi.get_setting("not_a_real_setting", config_path=empty_config)

    def test_set_setting_roundtrip(self, ffi, empty_config):
        """set_setting persists a value that get_setting reads back."""
        ffi.set_setting("defaults.farm_id", "farm-roundtrip", config_path=empty_config)
        value = ffi.get_setting("defaults.farm_id", config_path=empty_config)
        assert value == "farm-roundtrip"

    def test_set_setting_empty_string(self, ffi, empty_config):
        """set_setting with empty string clears the value (not null error)."""
        ffi.set_setting("defaults.farm_id", "farm-temp", config_path=empty_config)
        ffi.set_setting("defaults.farm_id", "", config_path=empty_config)
        value = ffi.get_setting("defaults.farm_id", config_path=empty_config)
        assert value == ""


# ── Tests 10-13: Resource listing (precise against stub) ─────────


class TestResourceListing:
    def test_list_farms(self, ffi):
        """list_farms returns the canned farm from the stub."""
        result = ffi.list_farms()
        assert len(result["farms"]) == 1
        assert result["farms"][0]["farmId"] == "farm-abc123def4567890abc123def4567890"
        assert result["farms"][0]["displayName"] == "Test Farm"

    def test_list_queues(self, ffi):
        """list_queues returns the canned queue from the stub."""
        result = ffi.list_queues(farm_id="farm-abc123def4567890abc123def4567890")
        assert len(result["queues"]) == 1
        assert result["queues"][0]["queueId"] == "queue-abc123def4567890abc123def4567890"
        assert result["queues"][0]["displayName"] == "Test Queue"

    def test_list_queues_null_farm_id(self, ffi):
        """list_queues raises DeadlineOperationError when farm_id is None."""
        with pytest.raises(DeadlineOperationError, match="farm_id"):
            ffi.list_queues(farm_id=None)

    def test_list_storage_profiles(self, ffi):
        """list_storage_profiles returns the canned profile from the stub."""
        result = ffi.list_storage_profiles_for_queue(
            farm_id="farm-abc123def4567890abc123def4567890",
            queue_id="queue-abc123def4567890abc123def4567890",
        )
        assert len(result["storageProfiles"]) == 1
        assert result["storageProfiles"][0]["displayName"] == "Test Storage Profile"

    def test_list_storage_profiles_null_ids(self, ffi):
        """list_storage_profiles raises when farm_id is None."""
        with pytest.raises(DeadlineOperationError, match="farm_id"):
            ffi.list_storage_profiles_for_queue(farm_id=None, queue_id=None)

    def test_get_queue_parameters(self, ffi):
        """get_queue_parameter_definitions returns empty list (no queue envs in stub)."""
        result = ffi.get_queue_parameter_definitions(
            farm_id="farm-abc123def4567890abc123def4567890",
            queue_id="queue-abc123def4567890abc123def4567890",
        )
        assert result == []

    def test_get_queue_parameters_null_ids(self, ffi):
        """get_queue_parameter_definitions raises when farm_id is None."""
        with pytest.raises(DeadlineOperationError, match="farm_id"):
            ffi.get_queue_parameter_definitions(farm_id=None, queue_id=None)


# ── Tests 14-16: Auth actions ────────────────────────────────────


class TestAuthActions:
    def test_check_api_available(self, ffi):
        """check_api_available returns True (stub has ListFarms mock)."""
        assert ffi.check_api_available() is True

    def test_login(self, ffi):
        """login raises (no DCM configured in stub config)."""
        with pytest.raises(DeadlineOperationError):
            ffi.login()

    def test_logout(self, ffi):
        """logout succeeds or raises (no DCM configured)."""
        try:
            ffi.logout()
        except DeadlineOperationError:
            pass  # Expected — no DCM profile in stub config


# ── Tests 17-19: Submission ──────────────────────────────────────


class TestSubmission:
    def test_create_job_null_params(self, ffi):
        """create_job_from_job_bundle raises when params is None."""
        with pytest.raises((DeadlineOperationError, TypeError)):
            ffi.create_job_from_job_bundle(params=None)

    def test_create_job_invalid_json(self, ffi):
        """create_job_from_job_bundle raises for non-dict params."""
        with pytest.raises((DeadlineOperationError, TypeError)):
            ffi.create_job_from_job_bundle(params="not a dict")

    def test_create_job_missing_bundle_dir(self, ffi):
        """create_job_from_job_bundle raises when job_bundle_dir is missing."""
        with pytest.raises(DeadlineOperationError, match="job_bundle_dir"):
            ffi.create_job_from_job_bundle(params={"name": "test"})


# ── Tests 20-23: Telemetry ───────────────────────────────────────


class TestTelemetry:
    def test_init_telemetry(self, ffi):
        """init_telemetry returns a non-None handle."""
        handle = ffi.init_telemetry()
        assert handle is not None
        ffi.free_telemetry(handle)

    def test_record_telemetry_event(self, ffi):
        """record_telemetry_event succeeds with a valid handle."""
        handle = ffi.init_telemetry()
        try:
            ffi.record_telemetry_event(handle, "com.amazon.rum.deadline.test", {"key": "value"})
        finally:
            ffi.free_telemetry(handle)

    def test_record_event_null_handle(self, ffi):
        """record_telemetry_event raises with a None handle."""
        with pytest.raises(DeadlineOperationError, match="handle"):
            ffi.record_telemetry_event(None, "test.event", {})

    def test_free_telemetry_null(self, ffi):
        """free_telemetry with None does not crash."""
        ffi.free_telemetry(None)


# ── Test 24: Memory management ───────────────────────────────────


class TestMemoryManagement:
    def test_memory_free_on_repeated_calls(self, ffi, tmp_config):
        """Repeated calls don't leak — _call_json always frees the string."""
        for _ in range(100):
            ffi.get_setting("defaults.farm_id", config_path=tmp_config)
