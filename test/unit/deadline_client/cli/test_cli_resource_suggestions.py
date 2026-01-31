# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for CLI resource ID error suggestions.
"""

import os

from botocore.exceptions import ClientError
from click.testing import CliRunner

from deadline.client import config
from deadline.client.cli import main

MOCK_FARM_ID = "farm-0123456789abcdef0123456789abcdef"
MOCK_QUEUE_ID = "queue-0123456789abcdef0123456789abcdef"
MOCK_FLEET_ID = "fleet-0123456789abcdef0123456789abcdef"


def test_bundle_submit_wrong_queue_suggests_queues(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Test that when GetQueue fails with AccessDeniedException, the CLI suggests available queues.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", "queue-wrongid12345678901234567890")

    # GetQueue fails with AccessDeniedException
    deadline_mock.get_queue.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform: deadline:GetQueue",
            }
        },
        "GetQueue",
    )

    # ListQueues succeeds and returns available queues
    deadline_mock.list_queues.return_value = {
        "queues": [
            {"queueId": MOCK_QUEUE_ID, "displayName": "My Queue"},
            {"queueId": "queue-anotherid1234567890123456", "displayName": "Another Queue"},
        ]
    }

    # Write a minimal JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write('{"specificationVersion": "jobtemplate-2023-09", "name": "Test", "steps": []}')

    runner = CliRunner()
    result = runner.invoke(main, ["bundle", "submit", temp_job_bundle_dir])

    assert result.exit_code != 0
    assert "AccessDeniedException" in result.output
    assert f"Available queues in farm {MOCK_FARM_ID}" in result.output
    assert MOCK_QUEUE_ID in result.output
    assert "My Queue" in result.output


def test_bundle_submit_wrong_farm_suggests_farms(
    fresh_deadline_config, deadline_mock, temp_job_bundle_dir
):
    """
    Test that when both GetQueue and ListQueues fail, the CLI suggests available farms.
    """
    config.set_setting("defaults.farm_id", "farm-wrongid12345678901234567890")
    config.set_setting("defaults.queue_id", "queue-wrongid12345678901234567890")

    # GetQueue fails with AccessDeniedException
    deadline_mock.get_queue.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform: deadline:GetQueue",
            }
        },
        "GetQueue",
    )

    # ListQueues also fails
    deadline_mock.list_queues.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform: deadline:ListQueues",
            }
        },
        "ListQueues",
    )

    # ListFarms succeeds
    deadline_mock.list_farms.return_value = {
        "farms": [
            {"farmId": MOCK_FARM_ID, "displayName": "Correct Farm"},
            {"farmId": "farm-anotherid1234567890123456", "displayName": "Another Farm"},
        ]
    }

    # Write a minimal JSON template
    with open(os.path.join(temp_job_bundle_dir, "template.json"), "w", encoding="utf8") as f:
        f.write('{"specificationVersion": "jobtemplate-2023-09", "name": "Test", "steps": []}')

    runner = CliRunner()
    result = runner.invoke(main, ["bundle", "submit", temp_job_bundle_dir])

    assert result.exit_code != 0
    assert "AccessDeniedException" in result.output
    assert "may be incorrect. Available farms:" in result.output
    assert MOCK_FARM_ID in result.output
    assert "Correct Farm" in result.output


def test_queue_list_wrong_farm_suggests_farms(fresh_deadline_config, deadline_mock):
    """
    Test that when ListQueues fails, the CLI suggests available farms.
    """
    config.set_setting("defaults.farm_id", "farm-wrongid12345678901234567890")

    # ListQueues fails
    deadline_mock.list_queues.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform: deadline:ListQueues",
            }
        },
        "ListQueues",
    )

    # ListFarms succeeds
    deadline_mock.list_farms.return_value = {
        "farms": [
            {"farmId": MOCK_FARM_ID, "displayName": "Correct Farm"},
        ]
    }

    runner = CliRunner()
    result = runner.invoke(main, ["queue", "list"])

    assert result.exit_code != 0
    assert "Failed to get Queues from Deadline" in result.output
    assert "may be incorrect. Available farms:" in result.output
    assert MOCK_FARM_ID in result.output


def test_worker_list_wrong_fleet_suggests_fleets(fresh_deadline_config, deadline_mock):
    """
    Test that when SearchWorkers fails, the CLI suggests available fleets.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    # SearchWorkers fails
    deadline_mock.search_workers.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource of type fleet with id fleet-00000000000000000000000000000000 does not exist.",
            }
        },
        "SearchWorkers",
    )

    # ListFleets succeeds
    deadline_mock.list_fleets.return_value = {
        "fleets": [
            {"fleetId": MOCK_FLEET_ID, "displayName": "GPU Fleet"},
            {"fleetId": "fleet-anotherid1234567890123456", "displayName": "CPU Fleet"},
        ]
    }

    runner = CliRunner()
    result = runner.invoke(
        main, ["worker", "list", "--fleet-id", "fleet-00000000000000000000000000000000"]
    )

    assert result.exit_code != 0
    assert "Failed to get Workers from Deadline" in result.output
    assert "may be incorrect. Available fleets:" in result.output
    assert MOCK_FLEET_ID in result.output
    assert "GPU Fleet" in result.output


def test_worker_list_suggestion_uses_correct_api_params(fresh_deadline_config, deadline_mock):
    """
    Test that when suggesting workers, the API call is made correctly.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    # First SearchWorkers call fails, second one (for suggestions) succeeds
    deadline_mock.search_workers.side_effect = [
        ClientError(
            {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Resource of type fleet with id fleet-wrongid does not exist.",
                }
            },
            "SearchWorkers",
        ),
        # Second call for suggestions via api.search_workers
        {
            "workers": [{"workerId": "worker-abc123", "status": "RUNNING"}],
            "totalResults": 1,
        },
    ]

    runner = CliRunner()
    runner.invoke(main, ["worker", "list", "--fleet-id", "fleet-wrongid00000000000000000000"])

    # Verify the suggestion call was made
    assert deadline_mock.search_workers.call_count == 2


def test_suggestion_truncates_long_lists(fresh_deadline_config, deadline_mock):
    """
    Test that suggestions are truncated when there are more than 10 results.
    """
    config.set_setting("defaults.farm_id", "farm-wrongid12345678901234567890")

    # ListQueues fails
    deadline_mock.list_queues.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform: deadline:ListQueues",
            }
        },
        "ListQueues",
    )

    # ListFarms returns many farms
    deadline_mock.list_farms.return_value = {
        "farms": [{"farmId": f"farm-{i:032d}", "displayName": f"Farm {i}"} for i in range(15)]
    }

    runner = CliRunner()
    result = runner.invoke(main, ["queue", "list"])

    assert result.exit_code != 0
    assert "... and 5 more" in result.output


def test_no_suggestion_on_other_errors(fresh_deadline_config, deadline_mock):
    """
    Test that non-access errors don't trigger suggestions.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    # ListQueues fails with a different error type
    deadline_mock.list_queues.side_effect = ClientError(
        {
            "Error": {
                "Code": "ServiceUnavailable",
                "Message": "Service is temporarily unavailable",
            }
        },
        "ListQueues",
    )

    runner = CliRunner()
    result = runner.invoke(main, ["queue", "list"])

    assert result.exit_code != 0
    assert "Failed to get Queues from Deadline" in result.output
    assert "Available" not in result.output


def test_suggestion_failure_silent(fresh_deadline_config, deadline_mock):
    """
    Test that if List API also fails, we show a hint about permissions.
    """
    config.set_setting("defaults.farm_id", "farm-wrongid12345678901234567890")

    # ListQueues fails
    deadline_mock.list_queues.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform: deadline:ListQueues",
            }
        },
        "ListQueues",
    )

    # ListFarms also fails
    deadline_mock.list_farms.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform: deadline:ListFarms",
            }
        },
        "ListFarms",
    )

    runner = CliRunner()
    result = runner.invoke(main, ["queue", "list"])

    assert result.exit_code != 0
    assert "Failed to get Queues from Deadline" in result.output
    assert "Could not list available resources" in result.output
    assert "IAM policy is missing List permissions" in result.output


def test_farm_get_wrong_farm_suggests_farms(fresh_deadline_config, deadline_mock):
    """
    Test that when GetFarm fails, the CLI suggests available farms.
    """
    config.set_setting("defaults.farm_id", "farm-wrongid12345678901234567890")

    deadline_mock.get_farm.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource of type farm does not exist.",
            }
        },
        "GetFarm",
    )

    deadline_mock.list_farms.return_value = {
        "farms": [
            {"farmId": MOCK_FARM_ID, "displayName": "Correct Farm"},
        ]
    }

    runner = CliRunner()
    result = runner.invoke(main, ["farm", "get"])

    assert result.exit_code != 0
    assert "Failed to get Farm from Deadline" in result.output
    assert "Available farms:" in result.output
    assert MOCK_FARM_ID in result.output


def test_fleet_get_wrong_fleet_suggests_fleets(fresh_deadline_config, deadline_mock):
    """
    Test that when GetFleet fails, the CLI suggests available fleets.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    deadline_mock.get_fleet.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource of type fleet does not exist.",
            }
        },
        "GetFleet",
    )

    deadline_mock.list_fleets.return_value = {
        "fleets": [
            {"fleetId": MOCK_FLEET_ID, "displayName": "GPU Fleet"},
        ]
    }

    runner = CliRunner()
    result = runner.invoke(
        main, ["fleet", "get", "--fleet-id", "fleet-wrongid12345678901234567890"]
    )

    assert result.exit_code != 0
    assert "Failed to get Fleet from Deadline" in result.output
    assert f"Available fleets in farm {MOCK_FARM_ID}" in result.output
    assert MOCK_FLEET_ID in result.output


def test_queue_get_wrong_queue_suggests_queues(fresh_deadline_config, deadline_mock):
    """
    Test that when GetQueue fails, the CLI suggests available queues.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", "queue-wrongid12345678901234567890")

    deadline_mock.get_queue.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource of type queue does not exist.",
            }
        },
        "GetQueue",
    )

    deadline_mock.list_queues.return_value = {
        "queues": [
            {"queueId": MOCK_QUEUE_ID, "displayName": "My Queue"},
        ]
    }

    runner = CliRunner()
    result = runner.invoke(main, ["queue", "get"])

    assert result.exit_code != 0
    assert "Failed to get Queue from Deadline" in result.output
    assert f"Available queues in farm {MOCK_FARM_ID}" in result.output
    assert MOCK_QUEUE_ID in result.output


def test_job_get_wrong_job_suggests_jobs(fresh_deadline_config, deadline_mock):
    """
    Test that when GetJob fails, the CLI suggests recent jobs.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    deadline_mock.get_job.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource of type job does not exist.",
            }
        },
        "GetJob",
    )

    deadline_mock.list_jobs.return_value = {
        "jobs": [
            {"jobId": "job-0123456789abcdef0123456789abcdef", "name": "Recent Job"},
        ]
    }

    runner = CliRunner()
    result = runner.invoke(main, ["job", "get", "--job-id", "job-wrongid12345678901234567890"])

    assert result.exit_code != 0
    assert "Failed to get Job from Deadline" in result.output
    assert f"Recent jobs in queue {MOCK_QUEUE_ID}" in result.output
    assert "job-0123456789abcdef0123456789abcdef" in result.output


def test_worker_get_wrong_worker_suggests_workers(fresh_deadline_config, deadline_mock):
    """
    Test that when GetWorker fails, the CLI suggests available workers.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    deadline_mock.get_worker.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource of type worker does not exist.",
            }
        },
        "GetWorker",
    )

    deadline_mock.search_workers.return_value = {
        "workers": [
            {"workerId": "worker-0123456789abcdef0123456789ab", "status": "RUNNING"},
        ],
        "totalResults": 1,
    }

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "worker",
            "get",
            "--fleet-id",
            MOCK_FLEET_ID,
            "--worker-id",
            "worker-wrongid12345678901234567890",
        ],
    )

    assert result.exit_code != 0
    assert "Failed to get Worker from Deadline" in result.output
    assert f"Available workers in fleet {MOCK_FLEET_ID}" in result.output
    assert "worker-0123456789abcdef0123456789ab" in result.output


def test_farm_list_wrong_farm_suggests_farms(fresh_deadline_config, deadline_mock):
    """
    Test that when ListFarms fails with AccessDeniedException, the CLI suggests available farms.
    """
    # First call fails, second (for suggestions) succeeds
    deadline_mock.list_farms.side_effect = [
        ClientError(
            {
                "Error": {
                    "Code": "AccessDeniedException",
                    "Message": "User is not authorized to perform: deadline:ListFarms",
                }
            },
            "ListFarms",
        ),
        {"farms": [{"farmId": MOCK_FARM_ID, "displayName": "Available Farm"}]},
    ]

    runner = CliRunner()
    result = runner.invoke(main, ["farm", "list"])

    assert result.exit_code != 0
    assert "Failed to get Farms from Deadline" in result.output
    assert "Available farms:" in result.output
    assert MOCK_FARM_ID in result.output


def test_fleet_list_wrong_farm_suggests_fleets(fresh_deadline_config, deadline_mock):
    """
    Test that when ListFleets fails, the CLI suggests available fleets.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    # First call fails, second (for suggestions) succeeds
    deadline_mock.list_fleets.side_effect = [
        ClientError(
            {
                "Error": {
                    "Code": "AccessDeniedException",
                    "Message": "User is not authorized to perform: deadline:ListFleets",
                }
            },
            "ListFleets",
        ),
        {"fleets": [{"fleetId": MOCK_FLEET_ID, "displayName": "GPU Fleet"}]},
    ]

    runner = CliRunner()
    result = runner.invoke(main, ["fleet", "list"])

    assert result.exit_code != 0
    assert "Failed to get Fleets from Deadline" in result.output
    assert f"Available fleets in farm {MOCK_FARM_ID}" in result.output
    assert MOCK_FLEET_ID in result.output


def test_job_list_wrong_queue_suggests_jobs(fresh_deadline_config, deadline_mock):
    """
    Test that when SearchJobs fails, the CLI suggests recent jobs.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    # SearchJobs fails
    deadline_mock.search_jobs.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform: deadline:SearchJobs",
            }
        },
        "SearchJobs",
    )

    # ListJobs for suggestions succeeds
    deadline_mock.list_jobs.return_value = {
        "jobs": [{"jobId": "job-0123456789abcdef0123456789abcdef", "name": "Recent Job"}]
    }

    runner = CliRunner()
    result = runner.invoke(main, ["job", "list"])

    assert result.exit_code != 0
    assert "Failed to get Jobs from Deadline" in result.output
    assert f"Recent jobs in queue {MOCK_QUEUE_ID}" in result.output


def test_job_cancel_wrong_job_suggests_jobs(fresh_deadline_config, deadline_mock):
    """
    Test that when GetJob fails in job cancel, the CLI suggests recent jobs.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", MOCK_QUEUE_ID)

    deadline_mock.get_job.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource of type job does not exist.",
            }
        },
        "GetJob",
    )

    deadline_mock.list_jobs.return_value = {
        "jobs": [{"jobId": "job-0123456789abcdef0123456789abcdef", "name": "Recent Job"}]
    }

    runner = CliRunner()
    result = runner.invoke(main, ["job", "cancel", "--job-id", "job-wrongid12345678901234567890"])

    assert result.exit_code != 0
    assert "Failed to get Job from Deadline" in result.output
    assert f"Recent jobs in queue {MOCK_QUEUE_ID}" in result.output


def test_queue_paramdefs_wrong_queue_suggests_queues(fresh_deadline_config, deadline_mock):
    """
    Test that when list_queue_environments fails, the CLI suggests available queues.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)
    config.set_setting("defaults.queue_id", "queue-wrongid12345678901234567890")

    # get_queue_parameter_definitions calls list_queue_environments internally
    deadline_mock.list_queue_environments.side_effect = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource of type queue does not exist.",
            }
        },
        "ListQueueEnvironments",
    )

    deadline_mock.list_queues.return_value = {
        "queues": [{"queueId": MOCK_QUEUE_ID, "displayName": "My Queue"}]
    }

    runner = CliRunner()
    result = runner.invoke(main, ["queue", "paramdefs"])

    assert result.exit_code != 0
    assert "Failed to get Queue Parameter Definitions from Deadline" in result.output
    assert f"Available queues in farm {MOCK_FARM_ID}" in result.output
    assert MOCK_QUEUE_ID in result.output
