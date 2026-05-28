"""API shim — routes through FFI to Rust."""

from deadline._native import (
    TelemetryClient,
    get_queue_parameter_definitions,
)
from deadline._native import (
    check_api_available as _native_check_api_available,
)
from deadline._native import (
    check_auth_status as _native_check_auth_status,
)
from deadline._native import (
    create_job_from_job_bundle as _native_create_job,
)
from deadline._native import (
    get_credentials_source as _native_get_credentials_source,
)
from deadline._native import (
    list_farms as _native_list_farms,
)
from deadline._native import (
    list_queues as _native_list_queues,
)
from deadline._native import (
    list_storage_profiles_for_queue as _native_list_storage_profiles,
)
from deadline._native import (
    login as _native_login,
)
from deadline._native import (
    logout as _native_logout,
)
from deadline.client._compat import AwsAuthenticationStatus, AwsCredentialsSource

session_context = {}


def get_deadline_cloud_library_telemetry_client(config=None):
    """Return a TelemetryClient for the Deadline Cloud library."""
    return TelemetryClient(config_path=None)


def precache_clients(deadline=None, config=None, farm_id=None, queue_id=None, queue_display_name=None):
    """No-op — Rust handles connection pooling internally."""
    return (None, None)


def list_farms(config=None, **kwargs):
    """List farms. Accepts camelCase kwargs for compatibility."""
    return _native_list_farms()


def list_queues(config=None, **kwargs):
    """List queues. Accepts farmId as camelCase kwarg."""
    farm_id = kwargs.get("farmId")
    return _native_list_queues(farm_id=farm_id)


def list_storage_profiles_for_queue(config=None, **kwargs):
    """List storage profiles for a queue. Accepts camelCase kwargs."""
    farm_id = kwargs.get("farmId")
    queue_id = kwargs.get("queueId")
    return _native_list_storage_profiles(farm_id=farm_id, queue_id=queue_id)


def get_credentials_source(config=None):
    """Return AwsCredentialsSource enum."""
    source_str = _native_get_credentials_source()
    return AwsCredentialsSource(source_str)


def check_authentication_status(config=None):
    """Return AwsAuthenticationStatus enum."""
    result = _native_check_auth_status()
    return AwsAuthenticationStatus(result["auth_status"])


def check_deadline_api_available(config=None):
    """Return True if Deadline API is reachable."""
    return _native_check_api_available()


def login(on_pending_authorization=None, on_cancellation_check=None, config=None):
    """Login via Deadline Cloud Monitor."""
    return _native_login(
        on_pending_authorization=on_pending_authorization,
        on_cancellation_check=on_cancellation_check,
    )


def logout(config=None):
    """Logout from Deadline Cloud Monitor."""
    return _native_logout()


def get_boto3_client(service_name, config=None):
    """Stub — returns None. Only used for precache_clients which is a no-op."""
    return None


def create_job_from_job_bundle(
    job_bundle_dir=None,
    job_parameters=None,
    *,
    name=None,
    queue_parameter_definitions=None,
    job_attachments_file_system=None,
    config=None,
    priority=None,
    max_failed_tasks_count=None,
    max_retries_per_task=None,
    max_worker_count=None,
    target_task_run_status=None,
    require_paths_exist=False,
    submitter_name=None,
    submitter_version=None,
    known_asset_paths=None,
    debug_snapshot_dir=None,
    from_gui=False,
    print_function_callback=None,
    interactive_confirmation_callback=None,
    hashing_progress_callback=None,
    upload_progress_callback=None,
    create_job_result_callback=None,
    force_s3_check=None,
    auto_accept=False,
):
    """Submit a job bundle. Flat keyword-arg signature matching original Python API."""
    from deadline.client._compat import ProgressReportMetadata

    # Build params dict — only include non-None/non-default values
    params = {"job_bundle_dir": job_bundle_dir}
    _optional = {
        "job_parameters": job_parameters,
        "name": name,
        "priority": priority,
        "max_failed_tasks_count": max_failed_tasks_count,
        "max_retries_per_task": max_retries_per_task,
        "max_worker_count": max_worker_count,
        "target_task_run_status": target_task_run_status,
        "job_attachments_file_system": job_attachments_file_system,
        "submitter_name": submitter_name,
        "force_s3_check": force_s3_check,
        "debug_snapshot_dir": debug_snapshot_dir,
    }
    params.update({k: v for k, v in _optional.items() if v is not None})
    if require_paths_exist:
        params["require_paths_exist"] = True
    if auto_accept:
        params["auto_accept"] = True
    if known_asset_paths is not None:
        params["known_asset_paths"] = list(known_asset_paths)

    # Wrap hashing/upload callbacks to convert dict → ProgressReportMetadata
    on_hashing = None
    if hashing_progress_callback is not None:

        def on_hashing(raw_dict):
            return hashing_progress_callback(ProgressReportMetadata.from_dict(raw_dict))

    on_upload = None
    if upload_progress_callback is not None:

        def on_upload(raw_dict):
            return upload_progress_callback(ProgressReportMetadata.from_dict(raw_dict))

    result = _native_create_job(
        params,
        on_print=print_function_callback,
        on_hashing_progress=on_hashing,
        on_upload_progress=on_upload,
        on_confirm=interactive_confirmation_callback,
        on_continue=create_job_result_callback,
    )
    return result.get("job_id") if isinstance(result, dict) else result


__all__ = [
    "AwsAuthenticationStatus",
    "AwsCredentialsSource",
    "TelemetryClient",
    "check_authentication_status",
    "check_deadline_api_available",
    "create_job_from_job_bundle",
    "get_boto3_client",
    "get_credentials_source",
    "get_deadline_cloud_library_telemetry_client",
    "get_queue_parameter_definitions",
    "list_farms",
    "list_queues",
    "list_storage_profiles_for_queue",
    "login",
    "logout",
    "precache_clients",
]
