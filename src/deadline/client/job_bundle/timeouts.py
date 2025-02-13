# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any, Optional, Dict
from dataclasses import dataclass

SECONDS_IN_A_MINUTE = 60
SECONDS_IN_AN_HOUR = 60 * 60
SECONDS_IN_A_DAY = SECONDS_IN_AN_HOUR * 24


@dataclass
class TimeoutSettings:
    is_activated: bool = True
    on_enter_timeout_seconds: int = SECONDS_IN_A_DAY
    on_exit_timeout_seconds: int = SECONDS_IN_A_DAY
    on_run_timeout_seconds: int = 5 * SECONDS_IN_A_DAY

    def __post_init__(self):
        # Validate timeout values
        for timeout in (
            self.on_enter_timeout_seconds,
            self.on_exit_timeout_seconds,
            self.on_run_timeout_seconds,
        ):
            if timeout <= 0:
                raise ValueError(f"Timeout value cannot be negative or zero: {timeout}")


def add_timeouts_to_job_template(
    template: Dict[str, Any], timeout_settings: Optional[TimeoutSettings] = None
) -> None:
    """
    Adds timeout values to actions in a job template.

    This function performs an in-place modification of the job template by adding or updating timeout values
    for all action types: onEnter, onExit, and onRun. According to OpenJD specification 2023-09,
    timeouts must be hard-coded in the job template.

    Args:
        template (Dict[str, Any]): The job template to modify.
        timeout_settings (Optional[TimeoutSettings]): Configuration for timeout values. If None,
            default timeout settings will be used. If activated is False, no timeouts will be added.

    Example:
        >>> template = {
        ...     "steps": [{
        ...         "stepEnvironments": [{
        ...             "script": {
        ...                 "actions": {
        ...                     "onEnter": {"command": "start"},
        ...                     "onExit": {"command": "stop"}
        ...                 }
        ...             }
        ...         }],
        ...         "script": {
        ...             "actions": {
        ...                 "onRun": {"command": "Do something"}
        ...             }
        ...         }
        ...     }]
        ... }
        >>> add_timeouts_to_job_template(template, TimeoutSettings())
    """
    if timeout_settings is None:
        timeout_settings = TimeoutSettings()

    if not timeout_settings.is_activated:
        return

    def _apply_timeouts_to_environment(environment: Dict):
        if "script" in environment:
            actions = environment["script"]["actions"]
            actions["onEnter"]["timeout"] = timeout_settings.on_enter_timeout_seconds
            if "onExit" in actions:
                actions["onExit"]["timeout"] = timeout_settings.on_exit_timeout_seconds

    def _apply_timeouts_to_step(step: Dict):
        for environment in step.get("stepEnvironments", []):
            _apply_timeouts_to_environment(environment)

        step["script"]["actions"]["onRun"]["timeout"] = timeout_settings.on_run_timeout_seconds

    for environment in template.get("jobEnvironments", []):
        _apply_timeouts_to_environment(environment)

    for step in template.get("steps", []):
        _apply_timeouts_to_step(step)
