# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["get_queue_parameter_definitions"]

from typing import Optional

import yaml

from .. import api
from ._list_apis import _call_paginated_deadline_list_api
from ._session import get_boto3_client
from ..exceptions import DeadlineOperationError
from ..job_bundle.parameters import (
    JobParameter,
    get_ui_control_for_parameter_definition,
    parameter_definition_difference,
    validate_job_parameter,
)
from ..ui._utils import tr


# The default Conda channel that Deadline Cloud configures automatically, and its v2 successor.
_DEADLINE_CLOUD_CHANNEL = "deadline-cloud"
_DEADLINE_CLOUD_V2_CHANNEL = "deadline-cloud-v2"


def _prepend_v2_channel(channels: str) -> str:
    """
    Inserts ``deadline-cloud-v2`` immediately ahead of the ``deadline-cloud`` channel in a
    space-separated channel list, returning the new list.

    Returns the input unchanged when ``deadline-cloud`` is absent, or when
    ``deadline-cloud-v2`` is already present (so the operation is idempotent across repeated
    queue-parameter reloads and queues that already list the v2 channel).
    """
    tokens = channels.split()
    if _DEADLINE_CLOUD_CHANNEL not in tokens or _DEADLINE_CLOUD_V2_CHANNEL in tokens:
        return channels
    result: list[str] = []
    for token in tokens:
        # Insert v2 just before v1 so it takes precedence under Conda's channel priority.
        if token == _DEADLINE_CLOUD_CHANNEL:
            result.append(_DEADLINE_CLOUD_V2_CHANNEL)
        result.append(token)
    return " ".join(result)


def _apply_deadline_cloud_v2_channel_migration(queue_parameters: list[JobParameter]) -> None:
    """
    Prepends ``deadline-cloud-v2`` ahead of ``deadline-cloud`` in the ``CondaChannels`` queue
    parameter (see :func:`_prepend_v2_channel`), rewriting both the ``default`` and ``value``
    fields in place.

    Args:
        queue_parameters (list[JobParameter]): The queue parameter definitions to modify.
    """
    for parameter in queue_parameters:
        if parameter.get("name") != "CondaChannels":
            continue
        for field in ("default", "value"):
            channels = parameter.get(field)
            if isinstance(channels, str):
                parameter[field] = _prepend_v2_channel(channels)


@api.record_function_latency_telemetry_event()
def get_queue_parameter_definitions(
    *, region: Optional[str] = None, farmId: str, queueId: str, config=None
) -> list[JobParameter]:
    """
    This gets all the queue parameter definitions for the specified [Deadline Cloud queue].

    It does so by getting all the full templates for [queue environments], and then combining
    them equivalently to the Deadline Cloud service logic.

    [Deadline Cloud queue]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/queues.html
    [queue environments]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/create-queue-environment.html

    Args:
        region (str, optional): The AWS region of the farm. When None, the region is
            resolved from `defaults.farm_region` (if set), otherwise the session/profile
            region is used.
        farmId (str): The farm the queue belongs to.
        queueId (str): The queue to get parameter definitions for.
        config (ConfigParser, optional): If provided, the AWS Deadline Cloud config to use.
    """
    deadline = get_boto3_client("deadline", config=config, region=region)
    response = _call_paginated_deadline_list_api(
        deadline.list_queue_environments,
        "environments",
        farmId=farmId,
        queueId=queueId,
    )
    queue_environments = sorted(
        (
            deadline.get_queue_environment(
                farmId=farmId,
                queueId=queueId,
                queueEnvironmentId=queue_env["queueEnvironmentId"],
            )
            for queue_env in response["environments"]
        ),
        key=lambda queue_env: queue_env["priority"],
    )
    queue_environment_templates = [
        yaml.safe_load(queue_env["template"]) for queue_env in queue_environments
    ]

    queue_parameters_definitions: dict[str, JobParameter] = {}
    for template in queue_environment_templates:
        for parameter in template.get("parameterDefinitions", []):
            parameter = validate_job_parameter(parameter, type_required=True, default_required=True)

            # If there is no group label, set it to the name of the Queue Environment
            if not parameter.get("userInterface", {}).get("groupLabel"):
                if "userInterface" not in parameter:
                    parameter["userInterface"] = {
                        "control": get_ui_control_for_parameter_definition(parameter)
                    }
                parameter["userInterface"]["groupLabel"] = tr("Queue Environment: {name}").format(
                    name=template["environment"]["name"]
                )
            existing_parameter = queue_parameters_definitions.get(parameter["name"])
            if existing_parameter:
                differences = parameter_definition_difference(existing_parameter, parameter)
                if differences:
                    raise DeadlineOperationError(
                        f"Job template parameter {parameter['name']} is duplicated across queue environments with mismatched fields:\n"
                        + " ".join(differences)
                    )
            else:
                queue_parameters_definitions[parameter["name"]] = parameter

    return list(queue_parameters_definitions.values())
