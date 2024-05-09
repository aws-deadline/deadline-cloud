"""
The UI Callback is a custom developer callback that can be used to do the following:
- Provide custom Job-Specific Settings Qt Widgets
- Modify default settings for the UI
"""
import dataclasses
import inspect
from typing import Any, Optional, get_type_hints

from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QWidget,
)

from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (  # type: ignore
    SubmitJobToDeadlineDialog,
)
from deadline.client.job_bundle.submission import AssetReferences

from .callback_loader import import_module_function, validate_function_signature


@dataclasses.dataclass
class UICallbackResponse:
    """Holds the resulting data from a UI Callback."""

    settings: object = dataclasses.field(default=None)
    """Settings that the submitter UI will use."""
    queue_parameters: list[dict[str, Any]] = dataclasses.field(default=None)
    """A list of OpenJD parameter definition dicts with a "value" key filled from the widget."""
    asset_references: AssetReferences = dataclasses.field(default=None)
    """An asset_references object that can be saved as the asset_references.json|yaml file in a Job Bundle."""
    host_requirements: Optional[dict[str, Any]] = dataclasses.field(default=None)
    """Returns a list of OpenJD parameter definition dicts with values filled from the widget or None."""
    job_specific_ui: QWidget = dataclasses.field(default=None)
    """An instantiated QWidget container that will be displayed in the Job-Specific Settings page"""


def _reference_ui_callback_type(
    dialog: SubmitJobToDeadlineDialog,
    settings: object,
    queue_parameters: list[dict[str, Any]],
    asset_references: AssetReferences,
    host_requirements: Optional[dict[str, Any]] = None,
) -> UICallbackResponse:
    return UICallbackResponse()


CALLBACK_REFERENCE_SIGNATURE = inspect.signature(_reference_ui_callback_type).parameters
CALLBACK_REFERENCE_HINTS = get_type_hints(_reference_ui_callback_type)


def load_ui_callback(module_path, module_name="ui_callback"):
    callback = import_module_function(
        module_path=module_path,
        module_name=module_name,
        function_name="on_ui_callback",
    )
    if not validate_function_signature(callback, CALLBACK_REFERENCE_SIGNATURE, hints=CALLBACK_REFERENCE_HINTS):
        raise ImportError(
            "Python function at {path}:on_ui_callback does not match function signature: {signature}."
            .format(
                path=module_path,
                signature=CALLBACK_REFERENCE_SIGNATURE,
            )
        )
    return callback
