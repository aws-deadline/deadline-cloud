# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests that get_queue_parameter_definitions treats parameter type names as
case-insensitive when a queue environment template declares the EXPR extension.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from deadline.client.api import _queue_parameters
from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID

_ENV_TEMPLATE = """
specificationVersion: environment-2023-09
{extensions}
parameterDefinitions:
- name: UseGpu
  type: bool
  default: false
- name: Channel
  type: String
  default: conda-forge
environment:
  name: Env
  script:
    actions:
      onEnter:
        command: echo
"""


def _deadline_client_with_env(template: str) -> MagicMock:
    deadline_client = MagicMock()
    deadline_client.list_queue_environments.return_value = {
        "environments": [{"queueEnvironmentId": "queueenv-1", "name": "Env", "priority": 1}]
    }
    deadline_client.get_queue_environment.return_value = {
        "queueEnvironmentId": "queueenv-1",
        "name": "Env",
        "priority": 1,
        "templateType": "YAML",
        "template": template,
    }
    return deadline_client


def test_queue_parameter_type_names_case_insensitive_with_expr():
    deadline_client = _deadline_client_with_env(
        _ENV_TEMPLATE.format(extensions="extensions: [EXPR]")
    )

    with patch.object(_queue_parameters, "get_boto3_client", return_value=deadline_client):
        result = _queue_parameters.get_queue_parameter_definitions(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID
        )

    assert {p["name"]: p["type"] for p in result} == {"UseGpu": "BOOL", "Channel": "STRING"}


def test_queue_parameter_type_names_case_sensitive_without_expr():
    deadline_client = _deadline_client_with_env(_ENV_TEMPLATE.format(extensions=""))

    with patch.object(_queue_parameters, "get_boto3_client", return_value=deadline_client):
        with pytest.raises(ValueError, match='"UseGpu" had "type" bool'):
            _queue_parameters.get_queue_parameter_definitions(
                farmId=MOCK_FARM_ID, queueId=MOCK_QUEUE_ID
            )
