# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from .test_utils import DeadlineCliTest
from deadline.client.cli import main
import datetime

from click.testing import CliRunner
import json
import boto3


def test_queue_get(deadline_cli_test: DeadlineCliTest) -> None:
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "queue",
            "get",
            "--queue-id",
            deadline_cli_test.queue_id,
            "--farm-id",
            deadline_cli_test.farm_id,
        ],
    )

    assert result.exit_code == 0

    assert f"queueId: {deadline_cli_test.queue_id}" in result.output
    # The following vary from queue to queue, so just make sure the general layout is there.
    # Unit tests are able to test the output more throughly. We'll only look for the required fields.
    assert "displayName:" in result.output
    assert f"farmId: {deadline_cli_test.farm_id}" in result.output
    assert "status" in result.output
    assert "defaultBudgetAction" in result.output


def test_queue_list(deadline_cli_test: DeadlineCliTest) -> None:
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["queue", "list", "--farm-id", deadline_cli_test.farm_id],
    )

    assert result.exit_code == 0

    assert f"- queueId: {deadline_cli_test.queue_id}" in result.output
    # The following vary from queue to queue, so just make sure the general layout is there.
    # Unit tests are able to test the output more throughly. We'll only look for the required fields.
    assert "displayName:" in result.output


def test_queue_export_credentials(deadline_cli_test: DeadlineCliTest) -> None:
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "queue",
            "export-credentials",
            "--farm-id",
            deadline_cli_test.farm_id,
            "--queue-id",
            deadline_cli_test.queue_id,
        ],
    )

    try:
        assert result.exit_code == 0

        creds = json.loads(result.output)
        # Transform credential_process output back into what boto expects

        # We want to make sure the date is in an expected format, but we're not going to further validate this date
        _ = datetime.datetime.fromisoformat(creds["Expiration"])
        assert creds["Version"] == 1

        session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )

        # Use the session
        sts = session.client("sts")

        # If this works it means our creds are good, so we're happy
        sts.get_caller_identity()
    except json.JSONDecodeError:
        # If JSON parsing fails, don't include the raw output in the error as this could print credentials
        assert False, "Failed to parse JSON output from export-credentials"
    except Exception as e:
        # For any other exception, make sure we don't include credentials in the error, so we need to be careful with what we print.
        assert False, f"Test failed: {type(e).__name__}"
