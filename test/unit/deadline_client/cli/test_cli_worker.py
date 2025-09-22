# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI worker commands.
"""

from datetime import datetime

from click.testing import CliRunner
from botocore.exceptions import ClientError

from deadline.client.cli import main
from ..shared_constants import (
    MOCK_FARM_ID,
    MOCK_FLEET_ID,
    MOCK_WORKER_ID,
)


def add_mocks_for_worker_list(deadline_mock):
    """
    Adds mock return values to the deadline_mock for sharing across
    the different 'deadline worker list' tests.
    """
    deadline_mock.search_workers.return_value = {
        "totalResults": 2,
        "workers": [
            {
                "workerId": "worker-1234567890abcdef1234567890abcdef",
                "status": "RUNNING",
                "createdAt": datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
                "fleetId": MOCK_FLEET_ID,
                "farmId": MOCK_FARM_ID,
                "hostProperties": {
                    "ipAddresses": {"ipV4Addresses": ["192.168.1.100"], "ipV6Addresses": []},
                    "hostName": "worker-host-001",
                    "ec2InstanceArn": "arn:aws:ec2:us-west-2:123456789012:instance/i-1234567890abcdef0",
                    "ec2InstanceType": "m5.large",
                },
                "log": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": "/aws/deadline/worker",
                        "awslogs-region": "us-west-2",
                    },
                },
                "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
                "updatedAt": datetime.fromisoformat("2024-01-01T12:00:00+00:00"),
                "updatedBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
            },
            {
                "workerId": "worker-abcdef1234567890abcdef1234567890",
                "status": "IDLE",
                "createdAt": datetime.fromisoformat("2024-01-01T11:00:00+00:00"),
                "fleetId": MOCK_FLEET_ID,
                "farmId": MOCK_FARM_ID,
                "hostProperties": {
                    "ipAddresses": {"ipV4Addresses": ["192.168.1.101"], "ipV6Addresses": []},
                    "hostName": "worker-host-002",
                    "ec2InstanceArn": "arn:aws:ec2:us-west-2:123456789012:instance/i-abcdef1234567890a",
                    "ec2InstanceType": "m5.xlarge",
                },
                "log": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": "/aws/deadline/worker",
                        "awslogs-region": "us-west-2",
                    },
                },
                "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
                "updatedAt": datetime.fromisoformat("2024-01-01T13:00:00+00:00"),
                "updatedBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
            },
        ],
    }


def add_mocks_for_worker_get(deadline_mock):
    """
    Adds mock return values to the deadline_mock for sharing across
    the different 'deadline worker get' tests.
    """
    deadline_mock.get_worker.return_value = {
        "workerId": MOCK_WORKER_ID,
        "status": "RUNNING",
        "createdAt": datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
        "updatedAt": datetime.fromisoformat("2024-01-01T12:00:00+00:00"),
        "fleetId": MOCK_FLEET_ID,
        "farmId": MOCK_FARM_ID,
        "hostProperties": {
            "ipAddresses": {"ipV4Addresses": ["192.168.1.100"], "ipV6Addresses": []},
            "hostName": "worker-host-001",
            "ec2InstanceArn": "arn:aws:ec2:us-west-2:123456789012:instance/i-1234567890abcdef0",
            "ec2InstanceType": "m5.large",
        },
        "log": {
            "logDriver": "awslogs",
            "options": {"awslogs-group": "/aws/deadline/worker", "awslogs-region": "us-west-2"},
        },
        "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
        "updatedBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
        "ResponseMetadata": {
            "RequestId": "12345678-1234-1234-1234-123456789012",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "date": "Mon, 01 Jan 2024 12:00:00 GMT",
                "content-type": "application/x-amz-json-1.0",
            },
            "RetryAttempts": 0,
        },
    }


def test_cli_worker_list_success(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker list' successfully lists workers in a fleet.
    """
    add_mocks_for_worker_list(deadline_mock)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "list",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
        ],
    )

    # Verify the CLI output format matches expected YAML structure with unquoted datetime values
    expected_output = """Displaying 2 of 2 workers starting at 0

- workerId: worker-1234567890abcdef1234567890abcdef
  status: RUNNING
  createdAt: 2024-01-01 10:00:00+00:00
- workerId: worker-abcdef1234567890abcdef1234567890
  status: IDLE
  createdAt: 2024-01-01 11:00:00+00:00

"""

    assert result.output == expected_output
    assert result.exit_code == 0

    # Assert correct API call parameters (farmId, fleetIds, itemOffset=0, pageSize=5)
    deadline_mock.search_workers.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetIds=[MOCK_FLEET_ID], itemOffset=0, pageSize=5
    )


def test_cli_worker_list_pagination(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker list' correctly handles custom page-size and item-offset parameters.
    """
    # Mock response for pagination test with custom parameters
    deadline_mock.search_workers.return_value = {
        "totalResults": 50,
        "workers": [
            {
                "workerId": "worker-page2-item1-1234567890abcdef",
                "status": "RUNNING",
                "createdAt": datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
                "fleetId": MOCK_FLEET_ID,
                "farmId": MOCK_FARM_ID,
            },
            {
                "workerId": "worker-page2-item2-abcdef1234567890",
                "status": "IDLE",
                "createdAt": datetime.fromisoformat("2024-01-01T11:00:00+00:00"),
                "fleetId": MOCK_FLEET_ID,
                "farmId": MOCK_FARM_ID,
            },
            {
                "workerId": "worker-page2-item3-fedcba0987654321",
                "status": "STOPPING",
                "createdAt": datetime.fromisoformat("2024-01-01T12:00:00+00:00"),
                "fleetId": MOCK_FLEET_ID,
                "farmId": MOCK_FARM_ID,
            },
        ],
    }

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "list",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            "--page-size",
            "3",
            "--item-offset",
            "10",
        ],
    )

    # Verify pagination information is displayed correctly in CLI output
    expected_output = """Displaying 3 of 50 workers starting at 10

- workerId: worker-page2-item1-1234567890abcdef
  status: RUNNING
  createdAt: 2024-01-01 10:00:00+00:00
- workerId: worker-page2-item2-abcdef1234567890
  status: IDLE
  createdAt: 2024-01-01 11:00:00+00:00
- workerId: worker-page2-item3-fedcba0987654321
  status: STOPPING
  createdAt: 2024-01-01 12:00:00+00:00

"""

    assert result.output == expected_output
    assert result.exit_code == 0

    # Test that API is called with correct pagination parameters
    deadline_mock.search_workers.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetIds=[MOCK_FLEET_ID], itemOffset=10, pageSize=3
    )


def test_cli_worker_list_empty_results(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker list' handles empty worker list scenario appropriately.
    """
    # Mock response for empty results
    deadline_mock.search_workers.return_value = {"totalResults": 0, "workers": []}

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "list",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            "--item-offset",
            "5",
        ],
    )

    # Verify appropriate messaging when no workers are found
    expected_output = """Displaying 0 of 0 workers starting at 5

[]

"""

    assert result.output == expected_output
    assert result.exit_code == 0

    # Verify API call parameters
    deadline_mock.search_workers.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetIds=[MOCK_FLEET_ID], itemOffset=5, pageSize=5
    )


def test_cli_worker_list_missing_fleet_id(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker list' displays usage message and exits with code 2 when required --fleet-id is missing.
    """
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "list",
            "--farm-id",
            MOCK_FARM_ID,
            # Missing --fleet-id parameter
        ],
    )

    # Verify Click displays usage message and exits with code 2
    assert "Error: Missing option '--fleet-id'." in result.output
    assert result.exit_code == 2

    # Verify API was not called since validation failed
    deadline_mock.search_workers.assert_not_called()


def test_cli_worker_list_api_error(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker list' handles AWS ClientError scenarios appropriately.
    """
    # Test incorrect fleet-id scenario
    deadline_mock.search_workers.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "ValidationException",
                "Message": "Fleet ID must match the pattern: ^fleet-[0-9a-f]{32}$",
            }
        },
        operation_name="SearchWorkers",
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "list",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            "incorrect-fleet-id",
        ],
    )

    # Verify DeadlineOperationError message appears in CLI output
    assert "Failed to get Workers from Deadline:" in result.output
    assert "Fleet ID must match the pattern" in result.output
    assert result.exit_code != 0

    # Verify API was called with the incorrect parameter values
    deadline_mock.search_workers.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetIds=["incorrect-fleet-id"], itemOffset=0, pageSize=5
    )


def test_cli_worker_list_api_error_network_timeout(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker list' handles network timeout ClientError scenarios.
    """
    # Test network timeout scenario
    deadline_mock.search_workers.side_effect = ClientError(
        error_response={"Error": {"Code": "RequestTimeout", "Message": "Request timed out"}},
        operation_name="SearchWorkers",
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "list",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
        ],
    )

    # Verify DeadlineOperationError message appears in CLI output
    assert "Failed to get Workers from Deadline:" in result.output
    assert "Request timed out" in result.output
    assert result.exit_code != 0

    # Verify API was called
    deadline_mock.search_workers.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetIds=[MOCK_FLEET_ID], itemOffset=0, pageSize=5
    )


def test_cli_worker_list_api_error_throttling(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker list' handles throttling ClientError scenarios.
    """
    # Test throttling scenario
    deadline_mock.search_workers.side_effect = ClientError(
        error_response={"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
        operation_name="SearchWorkers",
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "list",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
        ],
    )

    # Verify DeadlineOperationError message appears in CLI output
    assert "Failed to get Workers from Deadline:" in result.output
    assert "Rate exceeded" in result.output
    assert result.exit_code != 0

    # Verify API was called
    deadline_mock.search_workers.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetIds=[MOCK_FLEET_ID], itemOffset=0, pageSize=5
    )


def test_cli_worker_get_success(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker get' successfully retrieves and displays complete worker details.
    """
    add_mocks_for_worker_get(deadline_mock)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            "--worker-id",
            MOCK_WORKER_ID,
        ],
    )

    # Verify complete worker details are displayed in YAML format
    # ResponseMetadata should be removed from output as expected
    expected_output = f"""workerId: {MOCK_WORKER_ID}
status: RUNNING
createdAt: 2024-01-01 10:00:00+00:00
updatedAt: 2024-01-01 12:00:00+00:00
fleetId: {MOCK_FLEET_ID}
farmId: {MOCK_FARM_ID}
hostProperties:
  ipAddresses:
    ipV4Addresses:
    - 192.168.1.100
    ipV6Addresses: []
  hostName: worker-host-001
  ec2InstanceArn: arn:aws:ec2:us-west-2:123456789012:instance/i-1234567890abcdef0
  ec2InstanceType: m5.large
log:
  logDriver: awslogs
  options:
    awslogs-group: /aws/deadline/worker
    awslogs-region: us-west-2
createdBy: arn:aws:sts::123456789012:assumed-role/Admin/user
updatedBy: arn:aws:sts::123456789012:assumed-role/Admin/user

"""

    assert result.output == expected_output
    assert result.exit_code == 0

    # Verify correct API call parameters (farmId, fleetId, workerId)
    deadline_mock.get_worker.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID, workerId=MOCK_WORKER_ID
    )

    # Verify that ResponseMetadata is not present in the output
    assert "ResponseMetadata" not in result.output
    assert "RequestId" not in result.output
    assert "HTTPStatusCode" not in result.output

    # Test that datetime fields are displayed as unquoted values
    assert "createdAt: 2024-01-01 10:00:00+00:00" in result.output
    assert "updatedAt: 2024-01-01 12:00:00+00:00" in result.output
    # Ensure they are not quoted
    assert "'2024-01-01 10:00:00+00:00'" not in result.output
    assert '"2024-01-01 10:00:00+00:00"' not in result.output


def test_cli_worker_get_missing_required_options(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker get' displays appropriate error messages and exit codes for missing required parameters.
    """
    runner = CliRunner()

    # Test missing --fleet-id
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            "--worker-id",
            MOCK_WORKER_ID,
            # Missing --fleet-id parameter
        ],
    )

    # Verify appropriate error messages and exit codes for missing parameters
    assert "Error: Missing option '--fleet-id'." in result.output
    assert result.exit_code == 2

    # Verify API was not called since validation failed
    deadline_mock.get_worker.assert_not_called()

    # Test missing --worker-id
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            # Missing --worker-id parameter
        ],
    )

    # Verify appropriate error messages and exit codes for missing parameters
    assert "Error: Missing option '--worker-id'." in result.output
    assert result.exit_code == 2

    # Verify API was not called since validation failed
    deadline_mock.get_worker.assert_not_called()

    # Test missing both --fleet-id and --worker-id
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            # Missing both --fleet-id and --worker-id parameters
        ],
    )

    # Verify appropriate error messages and exit codes for missing parameters
    # Click will report the first missing required option
    assert "Error: Missing option" in result.output
    assert result.exit_code == 2

    # Verify API was not called since validation failed
    deadline_mock.get_worker.assert_not_called()


def test_cli_worker_get_api_error(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker get' handles AWS ClientError scenarios appropriately.
    """
    # Test incorrect worker-id scenario with ResourceNotFoundException
    deadline_mock.get_worker.side_effect = ClientError(
        error_response={
            "Error": {"Code": "ResourceNotFoundException", "Message": "Worker not found"}
        },
        operation_name="GetWorker",
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            "--worker-id",
            "worker-incorrect1234567890abcdef1234567890",
        ],
    )

    # Mock ClientError for ResourceNotFoundException and verify proper error handling
    # The @_handle_error decorator shows the full exception traceback
    assert "The AWS Deadline Cloud CLI encountered the following exception:" in result.output
    assert "ResourceNotFoundException" in result.output
    assert "Worker not found" in result.output
    assert result.exit_code == 1

    # Verify API was called with the incorrect parameter values
    deadline_mock.get_worker.assert_called_once_with(
        farmId=MOCK_FARM_ID,
        fleetId=MOCK_FLEET_ID,
        workerId="worker-incorrect1234567890abcdef1234567890",
    )


def test_cli_worker_get_api_error_validation_exception(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker get' handles ValidationException for incorrect worker-id format.
    """
    # Test incorrect worker-id format scenario
    deadline_mock.get_worker.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "ValidationException",
                "Message": "Worker ID must match the pattern: ^worker-[0-9a-f]{32}$",
            }
        },
        operation_name="GetWorker",
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            "--worker-id",
            "incorrect-worker-id",
        ],
    )

    # Verify ValidationException is handled and displays regex validation error
    # The @_handle_error decorator shows the full exception traceback
    assert "The AWS Deadline Cloud CLI encountered the following exception:" in result.output
    assert "ValidationException" in result.output
    assert "Worker ID must match the pattern" in result.output
    assert result.exit_code == 1

    # Verify API was called with the incorrect parameter values
    deadline_mock.get_worker.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID, workerId="incorrect-worker-id"
    )


def test_cli_worker_get_api_error_network_timeout(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker get' handles network timeout ClientError scenarios.
    """
    # Test network timeout scenario
    deadline_mock.get_worker.side_effect = ClientError(
        error_response={"Error": {"Code": "RequestTimeout", "Message": "Request timed out"}},
        operation_name="GetWorker",
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            "--worker-id",
            MOCK_WORKER_ID,
        ],
    )

    # Test network and service error scenarios
    # The @_handle_error decorator shows the full exception traceback
    assert "The AWS Deadline Cloud CLI encountered the following exception:" in result.output
    assert "RequestTimeout" in result.output
    assert "Request timed out" in result.output
    assert result.exit_code == 1

    # Verify API was called
    deadline_mock.get_worker.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID, workerId=MOCK_WORKER_ID
    )


def test_cli_worker_get_api_error_throttling(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker get' handles throttling ClientError scenarios.
    """
    # Test throttling scenario
    deadline_mock.get_worker.side_effect = ClientError(
        error_response={"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
        operation_name="GetWorker",
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            "--worker-id",
            MOCK_WORKER_ID,
        ],
    )

    # Test network and service error scenarios
    # The @_handle_error decorator shows the full exception traceback
    assert "The AWS Deadline Cloud CLI encountered the following exception:" in result.output
    assert "ThrottlingException" in result.output
    assert "Rate exceeded" in result.output
    assert result.exit_code == 1

    # Verify API was called
    deadline_mock.get_worker.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID, workerId=MOCK_WORKER_ID
    )


def test_cli_worker_get_api_error_service_unavailable(fresh_deadline_config, deadline_mock):
    """
    Tests that 'deadline worker get' handles service unavailable ClientError scenarios.
    """
    # Test service unavailable scenario
    deadline_mock.get_worker.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "ServiceUnavailableException",
                "Message": "Service temporarily unavailable",
            }
        },
        operation_name="GetWorker",
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--farm-id",
            MOCK_FARM_ID,
            "--fleet-id",
            MOCK_FLEET_ID,
            "--worker-id",
            MOCK_WORKER_ID,
        ],
    )

    # Test network and service error scenarios
    # The @_handle_error decorator shows the full exception traceback
    assert "The AWS Deadline Cloud CLI encountered the following exception:" in result.output
    assert "ServiceUnavailableException" in result.output
    assert "Service temporarily unavailable" in result.output
    assert result.exit_code == 1

    # Verify API was called
    deadline_mock.get_worker.assert_called_once_with(
        farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID, workerId=MOCK_WORKER_ID
    )
