# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from .test_utils import DeadlineCliTest
from deadline.client.cli import main

from click.testing import CliRunner


def test_farm_get(deadline_cli_test: DeadlineCliTest) -> None:

    runner = CliRunner()

    result = runner.invoke(
        main,
        ["farm", "get", "--farm-id", deadline_cli_test.farm_id],
    )

    assert result.exit_code == 0

    assert f"farmId: {deadline_cli_test.farm_id}" in result.output
    # The following vary from farm to farm, so just make sure the general layout is there.
    # Unit tests are able to test the output more throughly. We'll only look for the required fields.
    assert "displayName:" in result.output
    assert "description:" in result.output


def test_farm_list(deadline_cli_test: DeadlineCliTest) -> None:

    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "farm",
            "list",
        ],
    )

    assert result.exit_code == 0

    assert f"- farmId: {deadline_cli_test.farm_id}" in result.output
    # The following vary from farm to farm, so just make sure the general layout is there.
    # Unit tests are able to test the output more throughly. We'll only look for the required fields.
    assert "displayName:" in result.output
