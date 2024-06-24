"""
The Create Job Bundle Callback is a custom developer callback that can overwrite the default job template and
parameters.
"""
import inspect
from typing import Any, Optional, get_type_hints

from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (  # type: ignore
    SubmitJobToDeadlineDialog,
    JobBundlePurpose
)
from deadline.client.job_bundle.submission import AssetReferences

from .callback_loader import import_module_function, validate_function_signature


def _reference_create_job_bundle_callback_type(
        widget: SubmitJobToDeadlineDialog,
        job_bundle_dir: str,
        settings: object,
        queue_parameters: list[dict[str, Any]],
        asset_references: AssetReferences,
        host_requirements: Optional[dict[str, Any]] = None,
        purpose: JobBundlePurpose = JobBundlePurpose.SUBMISSION,
    ):
    pass


CALLBACK_REFERENCE_SIGNATURE = inspect.signature(_reference_create_job_bundle_callback_type).parameters
CALLBACK_REFERENCE_HINTS = get_type_hints(_reference_create_job_bundle_callback_type)


def load_create_job_bundle_callback(module_path, module_name="create_job_bundle_callback"):
    callback = import_module_function(
        module_path=module_path,
        module_name=module_name,
        function_name="on_create_job_bundle_callback",
    )
    if not validate_function_signature(callback, CALLBACK_REFERENCE_SIGNATURE, hints=CALLBACK_REFERENCE_HINTS):
        raise ImportError(
            "Python function at {path}:on_create_job_bundle_callback does not match function signature: {signature}."
            .format(
                path=module_path,
                signature=CALLBACK_REFERENCE_SIGNATURE,
            )
        )
    return callback
