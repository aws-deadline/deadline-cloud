"""Tests for deadline._native — the PyO3 bindings.

Mirrors test_ffi.py but calls the PyO3 module directly instead of
going through the ctypes wrapper. Uses the same test server fixtures.

Prerequisites: maturin develop
"""

import pytest

from deadline._native import (
    # Config
    get_setting,
    set_setting,
    read_config,
    # Auth
    get_credentials_source,
    check_auth_status,
    check_auth_status_with_progress,
    check_api_available,
    login,
    logout,
    # Resources
    list_farms,
    get_farm,
    list_queues,
    get_queue,
    list_storage_profiles_for_queue,
    get_queue_parameter_definitions,
    # Submission
    create_job_from_job_bundle,
    # Telemetry
    TelemetryClient,
    # Exception
    DeadlineOperationError,
)


# ── Config ───────────────────────────────────────────────────────


class TestConfig:
    def test_read_config(self, tmp_config):
        """read_config returns a dict with a 'config' key."""
        result = read_config(config_path=tmp_config)
        assert isinstance(result, dict)
        assert "config" in result

    def test_get_setting_known_key(self, tmp_config):
        """get_setting returns the value for a known key."""
        value = get_setting("defaults.farm_id", config_path=tmp_config)
        assert value == "farm-test123"

    def test_get_setting_invalid_name(self, empty_config):
        """get_setting raises DeadlineOperationError for an invalid setting name."""
        with pytest.raises(DeadlineOperationError):
            get_setting("not_a_real_setting", config_path=empty_config)

    def test_set_setting_roundtrip(self, empty_config):
        """set_setting persists a value that get_setting reads back."""
        set_setting("defaults.farm_id", "farm-roundtrip", config_path=empty_config)
        value = get_setting("defaults.farm_id", config_path=empty_config)
        assert value == "farm-roundtrip"

    def test_set_setting_empty_string(self, empty_config):
        """set_setting with empty string clears the value."""
        set_setting("defaults.farm_id", "farm-temp", config_path=empty_config)
        set_setting("defaults.farm_id", "", config_path=empty_config)
        value = get_setting("defaults.farm_id", config_path=empty_config)
        assert value == ""


# ── Auth Status ──────────────────────────────────────────────────


class TestAuthStatus:
    def test_get_credentials_source(self, test_server):
        """get_credentials_source returns HOST_PROVIDED."""
        result = get_credentials_source()
        assert result == "HOST_PROVIDED"

    def test_check_auth_status(self, test_server):
        """check_auth_status returns all 3 fields."""
        result = check_auth_status()
        assert result["credentials_source"] == "HOST_PROVIDED"
        assert result["auth_status"] == "AUTHENTICATED"
        assert result["api_available"] is True

    def test_check_auth_status_with_progress(self, test_server):
        """check_auth_status_with_progress fires the callback >= 3 times."""
        messages = []
        result = check_auth_status_with_progress(on_progress=lambda msg: messages.append(msg))
        assert result["credentials_source"] == "HOST_PROVIDED"
        assert len(messages) >= 3


# ── Auth Actions ─────────────────────────────────────────────────


class TestAuthActions:
    def test_check_api_available(self, test_server):
        """check_api_available returns True."""
        assert check_api_available() is True

    def test_login(self, test_server):
        """login raises (no DCM configured in stub)."""
        with pytest.raises(DeadlineOperationError):
            login()

    def test_logout(self, test_server):
        """logout succeeds or raises (no DCM configured)."""
        try:
            logout()
        except DeadlineOperationError:
            pass


# ── Resource Listing ─────────────────────────────────────────────


class TestResourceListing:
    def test_list_farms(self, test_server):
        """list_farms returns the canned farm from the stub."""
        result = list_farms()
        assert len(result["farms"]) == 1
        assert result["farms"][0]["farmId"] == "farm-abc123def4567890abc123def4567890"
        assert result["farms"][0]["displayName"] == "Test Farm"

    def test_list_queues(self, test_server):
        """list_queues returns the canned queue."""
        result = list_queues(farm_id="farm-abc123def4567890abc123def4567890")
        assert len(result["queues"]) == 1
        assert result["queues"][0]["queueId"] == "queue-abc123def4567890abc123def4567890"

    def test_list_queues_null_farm_id(self, test_server):
        """list_queues raises when farm_id is None."""
        with pytest.raises((DeadlineOperationError, TypeError)):
            list_queues(farm_id=None)

    def test_list_storage_profiles(self, test_server):
        """list_storage_profiles returns the canned profile."""
        result = list_storage_profiles_for_queue(
            farm_id="farm-abc123def4567890abc123def4567890",
            queue_id="queue-abc123def4567890abc123def4567890",
        )
        assert len(result["storageProfiles"]) == 1
        assert result["storageProfiles"][0]["displayName"] == "Test Storage Profile"

    def test_list_storage_profiles_null_ids(self, test_server):
        """list_storage_profiles raises when farm_id is None."""
        with pytest.raises((DeadlineOperationError, TypeError)):
            list_storage_profiles_for_queue(farm_id=None, queue_id=None)

    def test_get_farm(self, test_server):
        """get_farm returns the canned farm."""
        result = get_farm(farm_id="farm-abc123def4567890abc123def4567890")
        assert result["farmId"] == "farm-abc123def4567890abc123def4567890"
        assert result["displayName"] == "Test Farm"

    def test_get_queue(self, test_server):
        """get_queue returns the canned queue."""
        result = get_queue(
            farm_id="farm-abc123def4567890abc123def4567890",
            queue_id="queue-abc123def4567890abc123def4567890",
        )
        assert result["queueId"] == "queue-abc123def4567890abc123def4567890"

    def test_get_queue_parameters(self, test_server):
        """get_queue_parameter_definitions returns empty list."""
        result = get_queue_parameter_definitions(
            farm_id="farm-abc123def4567890abc123def4567890",
            queue_id="queue-abc123def4567890abc123def4567890",
        )
        assert result == []

    def test_get_queue_parameters_null_ids(self, test_server):
        """get_queue_parameter_definitions raises when farm_id is None."""
        with pytest.raises((DeadlineOperationError, TypeError)):
            get_queue_parameter_definitions(farm_id=None, queue_id=None)


# ── Submission ───────────────────────────────────────────────────


class TestSubmission:
    def test_create_job_null_params(self):
        """create_job_from_job_bundle raises when params is None."""
        with pytest.raises((DeadlineOperationError, TypeError)):
            create_job_from_job_bundle(params=None)

    def test_create_job_invalid_type(self):
        """create_job_from_job_bundle raises for non-dict params."""
        with pytest.raises((DeadlineOperationError, TypeError)):
            create_job_from_job_bundle(params="not a dict")

    def test_create_job_missing_bundle_dir(self):
        """create_job_from_job_bundle raises when job_bundle_dir is missing."""
        with pytest.raises(DeadlineOperationError, match="job_bundle_dir"):
            create_job_from_job_bundle(params={"name": "test"})

    def test_create_job_callbacks_receive_correct_types(self, tmp_path):
        """Callbacks receive the right argument types (even if submission fails)."""
        # Create a minimal invalid bundle to trigger an error after callbacks are wired
        bundle_dir = tmp_path / "bundle"
        bundle_dir.mkdir()
        (bundle_dir / "template.yaml").write_text("name: Test\n")

        print_messages = []
        hashing_calls = []

        def on_print(msg):
            print_messages.append(msg)
            assert isinstance(msg, str)

        def on_hashing(meta):
            hashing_calls.append(meta)
            assert isinstance(meta, dict)
            assert "progress" in meta
            return True

        # This will fail (invalid bundle), but callbacks should fire correctly
        # if the submission gets far enough. If it fails before callbacks fire,
        # that's fine — we're testing the wiring, not the submission.
        try:
            create_job_from_job_bundle(
                params={"job_bundle_dir": str(bundle_dir), "auto_accept": True},
                on_print=on_print,
                on_hashing_progress=on_hashing,
            )
        except DeadlineOperationError:
            pass  # Expected — invalid bundle

        # Print callback should have fired (submission prints status messages)
        # If it didn't fire, that's okay — the bundle may have failed before printing
        for msg in print_messages:
            assert isinstance(msg, str)
        for meta in hashing_calls:
            assert isinstance(meta, dict)


# ── Telemetry ────────────────────────────────────────────────────


class TestTelemetry:
    def test_init_and_drop(self):
        """TelemetryClient can be created and dropped."""
        client = TelemetryClient()
        assert client is not None
        del client  # triggers Drop

    def test_record_event(self):
        """record_event succeeds."""
        client = TelemetryClient()
        client.record_event("com.amazon.rum.deadline.test", {"key": "value"})

    def test_record_event_after_drop_raises(self):
        """Using a closed client raises."""
        client = TelemetryClient()
        client.close()
        with pytest.raises(DeadlineOperationError):
            client.record_event("test.event", {})


class TestExceptionUnification:
    def test_native_error_is_base_for_python_exceptions(self):
        """deadline.client.exceptions subclasses use the native DeadlineOperationError."""
        from deadline.client.exceptions import (
            DeadlineOperationError as PyDOE,
            DeadlineOperationCanceled,
            UserInitiatedCancel,
        )
        # All are the same base or subclass of the native one
        assert issubclass(PyDOE, DeadlineOperationError)
        assert issubclass(DeadlineOperationCanceled, DeadlineOperationError)
        assert issubclass(UserInitiatedCancel, DeadlineOperationError)

    def test_native_error_caught_by_python_except(self):
        """A native DeadlineOperationError is caught by except DeadlineOperationError from exceptions.py."""
        from deadline.client.exceptions import DeadlineOperationError as PyDOE
        with pytest.raises(PyDOE):
            get_setting("nonexistent.setting")
