# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from .test_utils import DeadlineCliTest
from deadline.client.cli import main

from click.testing import CliRunner


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
