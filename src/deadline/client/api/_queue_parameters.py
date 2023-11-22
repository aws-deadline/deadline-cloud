# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["get_queue_parameter_definitions"]

import yaml

from ._list_apis import _call_paginated_deadline_list_api
from ._session import get_boto3_client
from ..exceptions import DeadlineOperationError
from ..job_bundle.parameters import (
    JobParameter,
    get_ui_control_for_parameter_definition,
    parameter_definition_difference,
    validate_job_parameter,
)


def get_queue_parameter_definitions(
    *, farmId: str, queueId: str, config=None
) -> list[JobParameter]:
    """
    This gets all the queue parameters definitions from the specified Queue. It does so
    by getting all the full templates for queue environments, and then combining
    them equivalently to the Deadline Cloud service logic.
    """
    deadline = get_boto3_client("deadline", config=config)
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
                parameter["userInterface"][
                    "groupLabel"
                ] = f"Queue Environment: {template['environment']['name']}"
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
