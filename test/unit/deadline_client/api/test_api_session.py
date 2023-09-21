# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.api functions relating to boto3.Client
"""

from unittest.mock import call, patch, MagicMock, ANY

import boto3  # type: ignore[import]
import pytest
from deadline.client import api, config
from deadline.client.api._session import DeadlineClient


def test_get_boto3_session(fresh_deadline_config):
    """Confirm that api.get_boto3_session gets a session for the configured profile"""
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")

    with patch.object(boto3, "Session", return_value="SomeReturnValue") as boto3_session:
        # Testing this function
        result = api.get_boto3_session()

        # Confirm it returned the mocked value, and was called with the correct args
        assert result == "SomeReturnValue"
        boto3_session.assert_called_once_with(profile_name="SomeRandomProfileName")


def test_get_boto3_session_caching_behavior(fresh_deadline_config):
    """
    Confirm that api.get_boto3_session caches the session, and refreshes if
    the configured profile name changes
    """
    # mock boto3.Session to return a fresh object based on the input profile name
    with patch.object(
        boto3, "Session", side_effect=lambda profile_name: f"session for {profile_name}"
    ) as boto3_session:
        # This is a session with the default profile name
        session0 = api.get_boto3_session()

        assert session0 == "session for None"  # type: ignore

        # This should return the cached object, and not call boto3.Session
        session1 = api.get_boto3_session()

        assert session1 is session0

        # Overriding the config object means to not use the cached object
        session1_override_config = api.get_boto3_session(config=config.config_file.read_config())

        assert session1_override_config is not session0

        # Configuring a new session name should result in a new Session object
        config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
        session2 = api.get_boto3_session()

        assert session2 is not session0
        assert session2 == "session for SomeRandomProfileName"  # type: ignore

        # This should return the cached object, and not call boto3.Session
        session3 = api.get_boto3_session()

        assert session3 is session2

        # boto3.Session should have been called exactly twice, once for each
        # value of AWS profile name that was configured.
        boto3_session.assert_has_calls(
            [
                call(profile_name=None),
                call(profile_name="SomeRandomProfileName"),
            ]
        )


def test_get_check_credentials_status_authenticated(fresh_deadline_config):
    """Confirm that check_credentials_status returns AUTHENTICATED"""
    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        api, "get_boto3_session", new=session_mock
    ):
        config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
        session_mock().client("sts").get_caller_identity.return_value = {}

        assert api.check_credentials_status() == api.AwsCredentialsStatus.AUTHENTICATED


def test_get_check_credentials_status_configuration_error(fresh_deadline_config):
    """Confirm that check_credentials_status returns CONFIGURATION_ERROR"""
    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        api, "get_boto3_session", new=session_mock
    ):
        config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
        session_mock().client("sts").get_caller_identity.side_effect = Exception(
            "some uncaught exception"
        )

        assert api.check_credentials_status() == api.AwsCredentialsStatus.CONFIGURATION_ERROR


def test_get_queue_user_boto3_session_cache(fresh_deadline_config):
    session_mock = MagicMock()
    session_mock.profile_name = "test_profile"
    session_mock.region_name = "us-west-2"
    deadline_mock = MagicMock()
    mock_botocore_session = MagicMock()
    mock_botocore_session.get_config_variable = (
        lambda name: "test_profile" if name == "profile" else None
    )

    with patch.object(api._session, "get_boto3_session", return_value=session_mock), patch(
        "botocore.session.Session", return_value=mock_botocore_session
    ), patch.object(
        api._session, "_get_queue_user_boto3_session"
    ) as _get_queue_user_boto3_session_mock:
        _ = api.get_queue_user_boto3_session(
            deadline_mock, farm_id="farm-1234", queue_id="queue-1234", queue_display_name="queue"
        )
        # Same farm ID and queue ID, returns cached session
        _ = api.get_queue_user_boto3_session(
            deadline_mock, farm_id="farm-1234", queue_id="queue-1234", queue_display_name="queue"
        )
        # Different queue ID, makes a fresh session
        _ = api.get_queue_user_boto3_session(
            deadline_mock, farm_id="farm-1234", queue_id="queue-5678", queue_display_name="queue"
        )
        # Different queue ID, makes a fresh session
        _ = api.get_queue_user_boto3_session(
            deadline_mock, farm_id="farm-5678", queue_id="queue-1234", queue_display_name="queue"
        )
        assert _get_queue_user_boto3_session_mock.call_count == 3


def test_get_queue_user_boto3_session_no_profile(fresh_deadline_config):
    """Make sure that boto3.Session gets called with profile_name=None for the default profile."""
    session_mock = MagicMock()
    # The value returned when no profile was selected is "default"
    session_mock.profile_name = "default"
    session_mock.region_name = "us-west-2"
    deadline_mock = MagicMock()
    mock_botocore_session = MagicMock()
    mock_botocore_session.get_config_variable = (
        lambda name: "default" if name == "profile" else None
    )

    with patch.object(api._session, "get_boto3_session", return_value=session_mock), patch(
        "botocore.session.Session", return_value=mock_botocore_session
    ), patch("boto3.Session") as boto3_session_mock:
        api.get_queue_user_boto3_session(
            deadline_mock, farm_id="farm-1234", queue_id="queue-1234", queue_display_name="queue"
        )
        boto3_session_mock.assert_called_once_with(
            botocore_session=ANY, profile_name=None, region_name="us-west-2"
        )


def test_check_deadline_api_available(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_farms.return_value = {"farms": []}
        session_mock()._session.get_scoped_config().get.return_value = "some-studio-id"

        # Call the function under test
        result = api.check_deadline_api_available()

        assert result is True
        # It should have called list_farms with dry-run to check the API
        session_mock().client("deadline").list_farms.assert_called_once_with(
            maxResults=1, studioId="some-studio-id"
        )


def test_check_deadline_api_available_fails(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_farms.side_effect = Exception()
        session_mock()._session.get_scoped_config().get.return_value = "some-studio-id"

        # Call the function under test
        result = api.check_deadline_api_available()

        assert result is False
        # It should have called list_farms with dry-run to check the API
        session_mock().client("deadline").list_farms.assert_called_once_with(
            maxResults=1, studioId="some-studio-id"
        )


def test_get_boto3_client_deadline(fresh_deadline_config):
    """Confirm that api.get_boto3_client uses the endpoint url for the deadline client"""
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    config.set_setting("settings.deadline_endpoint_url", "some-endpoint-url")

    with patch.object(api._session, "get_boto3_session") as session_mock:
        # Testing this function
        api.get_boto3_client("deadline")

        session_mock().client.assert_called_once_with("deadline", endpoint_url="some-endpoint-url")


def test_get_boto3_client_other(fresh_deadline_config):
    """Confirm that api.get_boto3_client doesn't use the endpoint url for other clients"""
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
    config.set_setting("settings.deadline_endpoint_url", "some-endpoint-url")

    with patch.object(api._session, "get_boto3_session") as session_mock:
        # Testing this function
        api.get_boto3_client("s3")

        session_mock().client.assert_called_once_with("s3")


class FakeClient:
    def fake_deadline_client_has_this(self) -> str:
        return "from fake client"

    def but_not_this(self) -> str:
        return "from fake client"


class FakeDeadlineClient(DeadlineClient):
    def fake_deadline_client_has_this(self) -> str:
        return "from fake deadline client"


def test_deadline_client_pass_through() -> None:
    """
    Confirm that DeadlineClient passes through unknown methods to the underlying client
    but just executes known methods.
    """
    fake_client = FakeClient()
    deadline_client = FakeDeadlineClient(fake_client)

    assert deadline_client.fake_deadline_client_has_this() == "from fake deadline client"
    assert deadline_client.but_not_this() == "from fake client"


def test_get_farm_name_to_displayname() -> None:
    """
    get_farm will be updated such that "name" will be replaced with "displayName".
    Here we make sure that the shim is doing it's job of:
    1. Calling the underlying client method
    2. Replacing the "name" key in the response with "displayName"
    """
    fake_client = MagicMock()
    fake_client.get_farm.return_value = {"name": "farm1"}
    deadline_client = DeadlineClient(fake_client)
    response = deadline_client.get_farm("farmid-somefarm")

    assert "name" not in response
    assert "displayName" in response
    assert response["displayName"] == "farm1"
    fake_client.get_farm.assert_called_once_with("farmid-somefarm")


def test_list_farms_name_to_displayname() -> None:
    """
    list_farms will be updated such that "name" will be replaced with "displayName".
    Here we make sure that the shim is doing its job of:
    1. Calling the underlying client method
    2. Replacing the "name" key in the response with "displayName"
    """
    fake_client = MagicMock()
    fake_client.list_farms.return_value = {"farms": [{"name": "farm1"}]}
    deadline_client = DeadlineClient(fake_client)
    response = deadline_client.list_farms()

    assert "name" not in response["farms"][0]
    assert "displayName" in response["farms"][0]
    assert response["farms"][0]["displayName"] == "farm1"
    fake_client.list_farms.assert_called_once()


def test_get_queue_name_state_to_displayname_status() -> None:
    """
    get_queue will be updated such that "name" will be replaced with "displayName"
    and "state" will be replaced with "status".
    Here we make sure that the shim is doing its job of:
    1. Calling the underlying client method
    2. Replacing the appropriate keys
    """
    fake_client = MagicMock()
    fake_client.get_queue.return_value = {
        "name": "queue1",
        "state": "RUNNING",
    }
    deadline_client = DeadlineClient(fake_client)
    response = deadline_client.get_queue("queueid-somequeue")

    assert "name" not in response
    assert "displayName" in response
    assert "state" not in response
    assert "status" in response
    assert response["displayName"] == "queue1"
    assert response["status"] == "RUNNING"
    fake_client.get_queue.assert_called_once_with("queueid-somequeue")


def test_list_queues_name_to_displayname() -> None:
    """
    list_queues will be updated such that "name" will be replaced with "displayName".
    Here we make sure that the shim is doing its job of:
    1. Calling the underlying client method
    2. Replacing the appropriate keys
    """
    fake_client = MagicMock()
    fake_client.list_queues.return_value = {"queues": [{"name": "queue1"}]}
    deadline_client = DeadlineClient(fake_client)
    response = deadline_client.list_queues()

    assert "name" not in response["queues"][0]
    assert "displayName" in response["queues"][0]
    assert response["queues"][0]["displayName"] == "queue1"
    fake_client.list_queues.assert_called_once()


def test_get_fleet_name_state_to_displayname_status_remove_type() -> None:
    """
    get_fleet will be updated such that "name" will be replaced with "displayName"
    and "state" will be replaced with "status". "type" will be removed.
    Here we make sure that the shim is doing it's job of:
    1. Calling the underlying client method
    2. Replacing the appropriate keys
    """
    fake_client = MagicMock()
    fake_client.get_fleet.return_value = {
        "name": "fleet1",
        "state": "RUNNING",
        "type": "SERVICE_MANAGED",
    }
    deadline_client = DeadlineClient(fake_client)
    response = deadline_client.get_fleet("fleetid-somefleet")

    assert "name" not in response
    assert "displayName" in response
    assert "state" not in response
    assert "status" in response
    assert "type" not in response
    assert response["displayName"] == "fleet1"
    assert response["status"] == "RUNNING"
    fake_client.get_fleet.assert_called_once_with("fleetid-somefleet")


def test_list_fleets_name_to_displayname() -> None:
    """
    list_fleets will be updated such that "name" will be replaced with "displayName".
    Here we make sure that the shim is doing it's job of:
    1. Calling the underlying client method
    2. Replacing the "name" key in the response with "displayName"
    """
    fake_client = MagicMock()
    fake_client.list_fleets.return_value = {"fleets": [{"name": "fleet1"}]}
    deadline_client = DeadlineClient(fake_client)
    response = deadline_client.list_fleets()

    assert "name" not in response["fleets"][0]
    assert "displayName" in response["fleets"][0]
    assert response["fleets"][0]["displayName"] == "fleet1"
    fake_client.list_fleets.assert_called_once()


@pytest.mark.parametrize(
    "kwargs_input, name_in_model, kwargs_output",
    [
        pytest.param(
            {"maxErrorsPerTask": 11},
            "maxErrorsPerTask",
            {"maxErrorsPerTask": 11},
            id="MaxErrorsInSubmissionAndModel",
        ),
        pytest.param(
            {"maxRetriesPerTask": 22},
            "maxErrorsPerTask",
            {"maxErrorsPerTask": 22},
            id="maxRetriesPerTaskInSubmissionNotModel",
        ),
        pytest.param(
            {"maxRetriesPerTask": 33},
            "maxRetriesPerTask",
            {"maxRetriesPerTask": 33},
            id="MaxRetriesInSubmissionAndModel",
        ),
    ],
)
def test_create_job_max_errors_to_max_retries(kwargs_input, name_in_model, kwargs_output) -> None:
    """
    create_job will be updated so that maxErrorsPerTask is renamed to
    maxRetriesPerTask. Here we make sure that the shim is doing its job of:
    1. Calling the underlying client method
    2. Replacing the appropriate key
    """
    kwargs_output["priority"] = 50
    fake_client = MagicMock()
    deadline_client = DeadlineClient(fake_client)
    with patch.object(deadline_client, "_get_deadline_api_input_shape") as input_shape_mock:
        input_shape_mock.return_value = {name_in_model: "testing"}
        deadline_client.create_job(**kwargs_input)
    fake_client.create_job.assert_called_once_with(**kwargs_output)


@pytest.mark.parametrize(
    "kwargs_input, kwargs_output",
    [
        pytest.param(
            {"template": "", "templateType": "", "parameters": ""},
            {"template": "", "templateType": "", "parameters": ""},
            id="jobTemplate_NewAPI",
        ),
        pytest.param(
            {"template": "", "templateType": "", "parameters": ""},
            {
                "jobTemplate": "",
                "jobTemplateType": "",
                "jobParameters": "",
            },
            id="jobTemplate_OldAPI",
        ),
        pytest.param(
            {"template": "", "templateType": "", "parameters": "", "initialState": ""},
            {"jobTemplate": "", "jobTemplateType": "", "jobParameters": "", "initialState": ""},
            id="jobTemplate_StateToState",
        ),
        pytest.param(
            {"template": "", "templateType": "", "parameters": "", "targetTaskRunStatus": ""},
            {
                "jobTemplate": "",
                "jobTemplateType": "",
                "jobParameters": "",
                "initialState": "",
            },
            id="jobTemplate_StatusToState",
        ),
        pytest.param(
            {"template": "", "templateType": "", "parameters": "", "targetTaskRunStatus": ""},
            {
                "jobTemplate": "",
                "jobTemplateType": "",
                "jobParameters": "",
                "targetTaskRunStatus": "",
            },
            id="jobTemplate_StatusToStatus",
        ),
    ],
)
def test_create_job_old_api_compatibility(kwargs_input, kwargs_output) -> None:
    """
    create_job will be updated so that template is renamed to
    jobTemplate. Here we make sure that the shim is doing its job of:
    1. Calling the underlying client method
    2. Replacing the appropriate key

    """
    fake_client = MagicMock()
    kwargs_output["priority"] = 50
    deadline_client = DeadlineClient(fake_client)
    with patch.object(deadline_client, "_get_deadline_api_input_shape") as input_shape_mock:
        input_shape_mock.return_value = kwargs_output
        deadline_client.create_job(**kwargs_input)
    fake_client.create_job.assert_called_once_with(**kwargs_output)


@pytest.mark.parametrize(
    "kwargs_input, kwargs_output",
    [
        pytest.param(
            {"jobTemplate": "", "jobTemplateType": "", "jobParameters": ""},
            {"jobTemplate": "", "jobTemplateType": "", "jobParameters": "", "priority": 50},
            id="jobTemplate_addPriority",
        ),
        pytest.param(
            {"jobTemplate": "", "jobTemplateType": "", "jobParameters": "", "priority": 99},
            {"jobTemplate": "", "jobTemplateType": "", "jobParameters": "", "priority": 99},
            id="jobTemplate_leavePriority",
        ),
    ],
)
def test_create_job_priority_api_compatibility(kwargs_input, kwargs_output) -> None:
    """
    create_job will either leave priority parameter as is or add if missing
    """
    fake_client = MagicMock()
    deadline_client = DeadlineClient(fake_client)
    with patch.object(deadline_client, "_get_deadline_api_input_shape") as input_shape_mock:
        input_shape_mock.return_value = kwargs_output
        deadline_client.create_job(**kwargs_input)
    fake_client.create_job.assert_called_once_with(**kwargs_output)
