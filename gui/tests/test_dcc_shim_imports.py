"""Tests for DCC submitter import compatibility (work item #16f).

Verifies that all import paths used by the 9 DCC submitters resolve
correctly when importing from deadline-cloud-rs's gui/ package.

These tests do NOT require the stub server — they verify Python-level
import resolution and type contracts only.

Prerequisites: maturin develop
"""

import pytest


class TestApiModuleImports:
    """Verify deadline.client.api exports all names DCC submitters need."""

    def test_telemetry_client_importable(self):
        """TelemetryClient is importable from deadline.client.api."""
        from deadline.client.api import TelemetryClient

        assert TelemetryClient is not None

    def test_get_deadline_cloud_library_telemetry_client_importable(self):
        """get_deadline_cloud_library_telemetry_client is importable from deadline.client.api."""
        from deadline.client.api import get_deadline_cloud_library_telemetry_client

        assert callable(get_deadline_cloud_library_telemetry_client)

    def test_create_job_from_job_bundle_importable(self):
        """create_job_from_job_bundle is importable from deadline.client.api."""
        from deadline.client.api import create_job_from_job_bundle

        assert callable(create_job_from_job_bundle)

    def test_aws_credentials_source_importable(self):
        """AwsCredentialsSource is importable from deadline.client.api."""
        from deadline.client.api import AwsCredentialsSource

        assert hasattr(AwsCredentialsSource, "HOST_PROVIDED")
        assert hasattr(AwsCredentialsSource, "DEADLINE_CLOUD_MONITOR_LOGIN")

    def test_aws_authentication_status_importable(self):
        """AwsAuthenticationStatus is importable from deadline.client.api."""
        from deadline.client.api import AwsAuthenticationStatus

        assert hasattr(AwsAuthenticationStatus, "AUTHENTICATED")
        assert hasattr(AwsAuthenticationStatus, "CONFIGURATION_ERROR")
        assert hasattr(AwsAuthenticationStatus, "NEEDS_LOGIN")

    def test_precache_clients_importable(self):
        """precache_clients is importable from deadline.client.api."""
        from deadline.client.api import precache_clients

        assert callable(precache_clients)

    def test_get_queue_parameter_definitions_importable(self):
        """get_queue_parameter_definitions is importable from deadline.client.api."""
        from deadline.client.api import get_queue_parameter_definitions

        assert callable(get_queue_parameter_definitions)


class TestApiModuleBehavior:
    """Verify behavior of shim functions (not just importability)."""

    def test_get_deadline_cloud_library_telemetry_client_returns_client(self):
        """get_deadline_cloud_library_telemetry_client returns a TelemetryClient instance."""
        from deadline.client.api import (
            TelemetryClient,
            get_deadline_cloud_library_telemetry_client,
        )

        client = get_deadline_cloud_library_telemetry_client()
        assert isinstance(client, TelemetryClient)

    def test_precache_clients_returns_tuple(self):
        """precache_clients returns a tuple without crashing."""
        from deadline.client.api import precache_clients

        result = precache_clients()
        assert isinstance(result, tuple)
        assert len(result) == 2


class TestJobAttachmentsModelsImports:
    """Verify deadline.job_attachments.models exports needed types."""

    def test_file_conflict_resolution_importable(self):
        """FileConflictResolution is importable from deadline.job_attachments.models."""
        from deadline.job_attachments.models import FileConflictResolution

        assert hasattr(FileConflictResolution, "CREATE_COPY")
        assert hasattr(FileConflictResolution, "SKIP")
        assert hasattr(FileConflictResolution, "OVERWRITE")

    def test_file_conflict_resolution_is_enum(self):
        """FileConflictResolution members are string enums."""
        from deadline.job_attachments.models import FileConflictResolution

        assert FileConflictResolution.CREATE_COPY == "CREATE_COPY"
        assert FileConflictResolution.SKIP == "SKIP"
        assert FileConflictResolution.OVERWRITE == "OVERWRITE"


class TestJobAttachmentsProgressTrackerImports:
    """Verify deadline.job_attachments.progress_tracker exports needed types."""

    def test_progress_report_metadata_importable(self):
        """ProgressReportMetadata is importable from deadline.job_attachments.progress_tracker."""
        from deadline.job_attachments.progress_tracker import ProgressReportMetadata

        assert ProgressReportMetadata is not None

    def test_progress_report_metadata_from_dict(self):
        """ProgressReportMetadata.from_dict constructs from a dict."""
        from deadline.job_attachments.progress_tracker import ProgressReportMetadata

        data = {
            "status": "UPLOADING",
            "progress": 0.5,
            "transfer_rate": 1024.0,
            "progress_message": "50%",
            "processed_files": 3,
        }
        meta = ProgressReportMetadata.from_dict(data)
        assert meta.status == "UPLOADING"
        assert meta.progress == 0.5
        assert meta.transfer_rate == 1024.0
        assert meta.progress_message == "50%"
        assert meta.processed_files == 3

    def test_progress_status_importable(self):
        """ProgressStatus is importable from deadline.job_attachments.progress_tracker."""
        from deadline.job_attachments.progress_tracker import ProgressStatus

        assert ProgressStatus is not None


class TestUnrealImportPaths:
    """Verify import paths used specifically by the Unreal submitter."""

    def test_unreal_api_imports(self):
        """All imports from deadline.client.api used by Unreal resolve."""
        from deadline.client.api import (
            AwsAuthenticationStatus,
            AwsCredentialsSource,
            create_job_from_job_bundle,
            get_deadline_cloud_library_telemetry_client,
            precache_clients,
            TelemetryClient,
        )

        # All should be non-None
        assert all([
            AwsAuthenticationStatus,
            AwsCredentialsSource,
            create_job_from_job_bundle,
            get_deadline_cloud_library_telemetry_client,
            precache_clients,
            TelemetryClient,
        ])

    def test_unreal_job_attachments_imports(self):
        """All imports from deadline.job_attachments used by Unreal resolve."""
        from deadline.job_attachments.models import FileConflictResolution
        from deadline.job_attachments.progress_tracker import (
            ProgressReportMetadata,
            ProgressStatus,
        )

        assert all([FileConflictResolution, ProgressReportMetadata, ProgressStatus])
