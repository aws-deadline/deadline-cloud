# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests the job bundle folders created for the user's history of jobs.
"""

import os
import tempfile
import pytest

from freezegun import freeze_time

from deadline.client import config, job_bundle


def test_create_job_bundle_dir(fresh_deadline_config):
    # Use a temporary directory for the job history
    with tempfile.TemporaryDirectory() as tmpdir:
        config.set_setting("settings.job_history_dir", tmpdir)
        EXPECTED_DIRS = [
            "2023-01-15-01-cli_job-Test CLI Job Name",
            "2023-01-15-02-maya-Maya  Job with  Characters",
            "2023-01-15-03-cli_job-",
            "2023-04-15-01-maya-my_scene_filemb",
        ]
        EXPECTED_FULL_PATHS = [os.path.join(tmpdir, reldir[:7], reldir) for reldir in EXPECTED_DIRS]

        # Create a bunch of job bundle directories in order, and check that the expected dir is
        # there in each case.
        with freeze_time("2023-01-15T03:05"):
            assert (
                job_bundle.create_job_history_bundle_dir("cli_job", "Test CLI Job Name")
                == EXPECTED_FULL_PATHS[0]
            )
        assert os.path.isdir(EXPECTED_FULL_PATHS[0])
        with freeze_time("2023-01-15T12:12"):
            assert (
                job_bundle.create_job_history_bundle_dir("maya", "Maya : Job with %~\\/ Characters")
                == EXPECTED_FULL_PATHS[1]
            )
        assert os.path.isdir(EXPECTED_FULL_PATHS[1])
        with freeze_time("2023-01-15T07:10"):
            assert job_bundle.create_job_history_bundle_dir("cli_job", "") == EXPECTED_FULL_PATHS[2]
        assert os.path.isdir(EXPECTED_FULL_PATHS[2])
        with freeze_time("2023-04-15T19:59"):
            assert (
                job_bundle.create_job_history_bundle_dir("maya", "my_scene_file.mb")
                == EXPECTED_FULL_PATHS[3]
            )
        assert os.path.isdir(EXPECTED_FULL_PATHS[3])

        # Confirm the full set of expected directories
        assert sorted(os.listdir(tmpdir)) == ["2023-01", "2023-04"]
        assert sorted(os.listdir(os.path.join(tmpdir, "2023-01"))) == EXPECTED_DIRS[:3]
        assert sorted(os.listdir(os.path.join(tmpdir, "2023-04"))) == EXPECTED_DIRS[3:]


@pytest.mark.parametrize(
    "submitter_name, job_name, freeze_date, expected_output_path",
    [
        pytest.param(
            "SubmitterOne",
            "JobOne",
            "2023-09-25",
            os.path.join("2023-09", "2023-09-25-01-SubmitterOne-JobOne"),
            id="NoInvalidCharacters",
        ),
        pytest.param(
            "Submitter...Two",
            "Job@#$%^?Two",
            "2023-09-25",
            os.path.join("2023-09", "2023-09-25-01-SubmitterTwo-JobTwo"),
            id="InvalidCharactersInNames",
        ),
        pytest.param(
            "\\..\\..\\..\\SubmitterThree",
            "./../../Job/Three",
            "2023-09-25",
            os.path.join("2023-09", "2023-09-25-01-SubmitterThree-JobThree"),
            id="PathsInNames",
        ),
    ],
)
def test_create_job_bundle_dir_sanitization(
    submitter_name: str,
    job_name: str,
    freeze_date: str,
    expected_output_path: str,
    fresh_deadline_config,
):
    # Use a temporary directory for the job history
    with tempfile.TemporaryDirectory() as tmpdir, freeze_time(freeze_date):
        config.set_setting("settings.job_history_dir", tmpdir)
        assert job_bundle.create_job_history_bundle_dir(submitter_name, job_name) == os.path.join(
            tmpdir, expected_output_path
        )
        assert os.path.isdir(os.path.join(tmpdir, expected_output_path))
