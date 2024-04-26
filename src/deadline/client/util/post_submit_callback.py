"""
The Post Submit Callback is a custom developer callback that can be used to perform actions after a job has been
  submitted.
"""
import inspect
from typing import get_type_hints

from .callback_loader import import_module_function, validate_function_signature


def _reference_post_submit_callback_type(
    job_id: str,
):
    pass


CALLBACK_REFERENCE_SIGNATURE = inspect.signature(_reference_post_submit_callback_type).parameters
CALLBACK_REFERENCE_HINTS = get_type_hints(_reference_post_submit_callback_type)


def load_post_submit_callback(module_path, module_name="post_submit_callback"):
    callback = import_module_function(
        module_path=module_path,
        module_name=module_name,
        function_name="on_post_submit_callback",
    )
    if not validate_function_signature(callback, CALLBACK_REFERENCE_SIGNATURE, hints=CALLBACK_REFERENCE_HINTS):
        raise ImportError(
            "Python function at {path}:on_post_submit_callback does not match function signature: {signature}."
            .format(
                path=module_path,
                signature=CALLBACK_REFERENCE_SIGNATURE,
            )
        )