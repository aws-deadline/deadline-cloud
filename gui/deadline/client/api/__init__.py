"""API shim — routes through FFI to Rust."""

from deadline._native import (
    TelemetryClient,
    create_job_from_job_bundle,
    get_queue_parameter_definitions,
)
from deadline.client._compat import AwsAuthenticationStatus, AwsCredentialsSource

session_context = {}


def get_deadline_cloud_library_telemetry_client(config=None):
    """Return a TelemetryClient for the Deadline Cloud library."""
    return TelemetryClient(config_path=None)


def precache_clients(deadline=None, config=None, farm_id=None, queue_id=None, queue_display_name=None):
    """No-op — Rust handles connection pooling internally."""
    return (None, None)


__all__ = [
    "AwsAuthenticationStatus",
    "AwsCredentialsSource",
    "TelemetryClient",
    "create_job_from_job_bundle",
    "get_deadline_cloud_library_telemetry_client",
    "get_queue_parameter_definitions",
    "precache_clients",
]
