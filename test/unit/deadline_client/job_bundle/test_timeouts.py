# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import pytest
from typing import Any, Optional, Dict
from deadline.client.job_bundle.timeouts import TimeoutSettings, add_timeouts_to_job_template
from ..testing_utilities import create_sample_job_template


def test_timeout_settings_default_values():
    settings = TimeoutSettings()
    assert settings.is_activated
    assert settings.on_enter_timeout_seconds == 86400  # 1 day in seconds
    assert settings.on_exit_timeout_seconds == 86400
    assert settings.on_run_timeout_seconds == 432000  # 5 days in seconds


def test_timeout_settings_custom_values():
    settings = TimeoutSettings(
        is_activated=True,
        on_enter_timeout_seconds=3600,
        on_exit_timeout_seconds=7200,
        on_run_timeout_seconds=10800,
    )
    assert settings.is_activated
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


SAMPLE_JOB_TEMPLATE = create_sample_job_template(
    num_of_job_env=1, num_of_steps=1, num_of_step_env_per_step=1, num_of_tasks_per_step=1
)
SAMPLE_JOB_TEMPLATE_WITH_DEFAULT_TIMEOUTS = create_sample_job_template(
    num_of_job_env=1,
    num_of_step_env_per_step=1,
    num_of_steps=1,
    num_of_tasks_per_step=1,
    on_task_run_timeout=432000,
    on_exit_timeout=86400,
    on_enter_timeout=86400,
)
SAMPLE_JOB_TEMPLATE_WITH_CUSTOM_TIMEOUTS = create_sample_job_template(
    num_of_job_env=1,
    num_of_step_env_per_step=1,
    num_of_steps=1,
    num_of_tasks_per_step=1,
    on_task_run_timeout=7200,
    on_exit_timeout=3600,
    on_enter_timeout=3600,
)
SAMPLE_JOB_WITH_MULTIPLE_JOB_ENV_STEP_ENVS = create_sample_job_template(
    num_of_job_env=3, num_of_step_env_per_step=3, num_of_steps=3, num_of_tasks_per_step=3
)
SAMPLE_JOB_WITH_MULTIPLE_JOB_ENV_STEP_ENVS_WITH_CUSTOM_TIMEOUTS = create_sample_job_template(
    num_of_job_env=3,
    num_of_step_env_per_step=3,
    num_of_steps=3,
    num_of_tasks_per_step=3,
    on_task_run_timeout=720,
    on_exit_timeout=360,
    on_enter_timeout=360,
)


@pytest.mark.parametrize(
    "template, timeout_settings, expected_template",
    [
        pytest.param(
            SAMPLE_JOB_TEMPLATE,
            TimeoutSettings(),
            SAMPLE_JOB_TEMPLATE_WITH_DEFAULT_TIMEOUTS,
            id="Add default timeouts to sample job template",
        ),
        pytest.param(
            SAMPLE_JOB_TEMPLATE,
            None,
            SAMPLE_JOB_TEMPLATE_WITH_DEFAULT_TIMEOUTS,
            id="Add default timeouts if time settings is None",
        ),
        pytest.param(
            SAMPLE_JOB_TEMPLATE,
            TimeoutSettings(
                is_activated=False,
                on_enter_timeout_seconds=3600,
                on_exit_timeout_seconds=3600,
                on_run_timeout_seconds=7200,
            ),
            SAMPLE_JOB_TEMPLATE,
            id="If deactivated, then timeouts are not added",
        ),
        pytest.param(
            SAMPLE_JOB_TEMPLATE,
            TimeoutSettings(
                on_enter_timeout_seconds=3600,
                on_exit_timeout_seconds=3600,
                on_run_timeout_seconds=7200,
            ),
            SAMPLE_JOB_TEMPLATE_WITH_CUSTOM_TIMEOUTS,
            id="Add custom timeouts to sample job template",
        ),
        pytest.param(
            SAMPLE_JOB_WITH_MULTIPLE_JOB_ENV_STEP_ENVS,
            TimeoutSettings(
                on_enter_timeout_seconds=360,
                on_exit_timeout_seconds=360,
                on_run_timeout_seconds=720,
            ),
            SAMPLE_JOB_WITH_MULTIPLE_JOB_ENV_STEP_ENVS_WITH_CUSTOM_TIMEOUTS,
            id="Add timeouts for multi environments/steps job.",
        ),
    ],
)
def test_add_timeouts_to_job_template(
    template: Dict[str, Any],
    timeout_settings: Optional[TimeoutSettings],
    expected_template: Dict[str, Any],
):
    """Test adding timeouts to job templates."""
    add_timeouts_to_job_template(template, timeout_settings)
    assert template == expected_template
