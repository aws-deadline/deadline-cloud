# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import pytest
from typing import Optional
from deadline.client.job_bundle.timeouts import TimeoutSettings, add_timeouts_to_job_template
import json
from pathlib import Path


def test_timeout_settings_default_values():
    settings = TimeoutSettings()
    assert settings.is_activated
    assert settings.on_enter_timeout_seconds == 86400  # 1 day in seconds
    assert settings.on_exit_timeout_seconds == 86400
    assert settings.on_run_timeout_seconds == 432000  # 5 days in seconds


def test_timeout_settings_custom_values():
    settings = TimeoutSettings(
        is_activated=False,
        on_enter_timeout_seconds=3600,
        on_exit_timeout_seconds=7200,
        on_run_timeout_seconds=10800,
    )
    assert not settings.is_activated
    assert settings.on_enter_timeout_seconds == 3600
    assert settings.on_exit_timeout_seconds == 7200
    assert settings.on_run_timeout_seconds == 10800


def test_timeout_settings_negative_values():
    with pytest.raises(ValueError):
        TimeoutSettings(on_enter_timeout_seconds=-1)

    with pytest.raises(ValueError):
        TimeoutSettings(on_exit_timeout_seconds=-1)

    with pytest.raises(ValueError):
        TimeoutSettings(on_run_timeout_seconds=-1)


def test_timeout_settings_zero_values():
    with pytest.raises(ValueError):
        TimeoutSettings(on_enter_timeout_seconds=0)

    with pytest.raises(ValueError):
        TimeoutSettings(on_exit_timeout_seconds=0)

    with pytest.raises(ValueError):
        TimeoutSettings(on_run_timeout_seconds=0)


def load_json_data(file_name: str) -> dict:
    # Get the current file's directory and construct path to data
    current_dir = Path(__file__).parent
    data_dir = current_dir / "data"
    file_path = data_dir / f"{file_name}.json"

    with open(file_path, "r") as file:
        return json.load(file)


@pytest.mark.parametrize(
    "template_filename, timeout_settings, expected_template_filename",
    [
        pytest.param(
            "sample_job_template",
            TimeoutSettings(),
            "sample_job_template_with_default_timeouts",
            id="Add default timeouts to sample job template",
        ),
        pytest.param(
            "sample_job_template",
            None,
            "sample_job_template_with_default_timeouts",
            id="Add default timeouts if time settings is None",
        ),
        pytest.param(
            "sample_job_template",
            TimeoutSettings(
                is_activated=False,
                on_enter_timeout_seconds=3600,
                on_exit_timeout_seconds=3600,
                on_run_timeout_seconds=7200,
            ),
            "sample_job_template",
            id="If deactivated, then timeouts are not added",
        ),
        pytest.param(
            "sample_job_template",
            TimeoutSettings(
                on_enter_timeout_seconds=3600,
                on_exit_timeout_seconds=3600,
                on_run_timeout_seconds=7200,
            ),
            "sample_job_template_with_custom_timeouts",
            id="Add custom timeouts to sample job template",
        ),
        pytest.param(
            "sample_job_template_with_multiple_job_envs_step_envs",
            TimeoutSettings(
                on_enter_timeout_seconds=360,
                on_exit_timeout_seconds=360,
                on_run_timeout_seconds=720,
            ),
            "sample_job_template_with_multiple_job_envs_step_envs_with_custom_timeouts",
            id="Add timeouts for multi environments/steps job.",
        ),
    ],
)
def test_add_timeouts_to_job_template(
    template_filename: str,
    timeout_settings: Optional[TimeoutSettings],
    expected_template_filename: str,
):
    template = load_json_data(template_filename)
    expected_template = load_json_data(expected_template_filename)

    add_timeouts_to_job_template(template, timeout_settings)
    assert template == expected_template
