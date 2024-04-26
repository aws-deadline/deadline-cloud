# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import importlib.util
import inspect
import sys
from typing import Any, Optional, get_type_hints

from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (  # type: ignore
    SubmitJobToDeadlineDialog,
    JobBundlePurpose,
)
from deadline.client.job_bundle.submission import AssetReferences


def _reference_callback_signature(
    widget: SubmitJobToDeadlineDialog,
    job_bundle_dir: str,
    settings: object,
    queue_parameters: list[dict[str, Any]],
    asset_references: AssetReferences,
    host_requirements: Optional[dict[str, Any]] = None,
    purpose: JobBundlePurpose = JobBundlePurpose.SUBMISSION,
):
    pass


CALLBACK_REFERENCE_SIGNATURE = inspect.signature(_reference_callback_signature).parameters
CALLBACK_REFERENCE_HINTS = get_type_hints(_reference_callback_signature)


def import_module_function(module_path, module_name, function_name):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return getattr(mod, function_name)


def _validate_parameter_type(parameter_type, other_type):
    if parameter_type == other_type:
        return True

    if parameter_type in other_type.__bases__:
        return True
    return False


def validate_function_signature(function, reference_signature=CALLBACK_REFERENCE_SIGNATURE, hints=CALLBACK_REFERENCE_HINTS):
    parameters = inspect.signature(function).parameters
    if parameters == reference_signature:
        return True

    function_hints = get_type_hints(function)

    for param_name in hints:
        if not function_hints.get(param_name):
            return False
        if not _validate_parameter_type(
            hints[param_name], function_hints[param_name]
        ):
            return False
    return True
