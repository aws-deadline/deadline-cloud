# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
This module provides functions to work with [job bundles] locally. For example, you can
call [read_job_bundle_parameters][deadline.client.job_bundle.read_job_bundle_parameters]
to get information like the names, types, and default values of a job bundle's parameters.

Use the [api.create_job_from_job_bundle][deadline.client.api.create_job_from_job_bundle] function to submit a job
bundle to a queue.

[job bundles]: https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-bundle.html
"""

__all__ = [
    "apply_job_parameters",
    "create_job_history_bundle_dir",
    "deadline_yaml_dump",
    "read_job_bundle_parameters",
]

import datetime
import glob
import os

from ..config import get_setting
from ._yaml import deadline_yaml_dump
from .parameters import apply_job_parameters, read_job_bundle_parameters


def create_job_history_bundle_dir(submitter_name: str, job_name: str) -> str:
    """
    Creates a new directory in the configured directory
    settings.job_history_dir, in which to place a new
    job bundle for submission.

    The directory will look like
      `<job_history_dir>/YYYY-mm/YYYY-mm-ddTHH-##-<submitter_name>-<job_name>`
    """
    job_history_dir = str(get_setting("settings.job_history_dir"))
    job_history_dir = os.path.expanduser(job_history_dir)

    # Clean the submitter_name's characters
    submitter_name_cleaned = "".join(
        char for char in submitter_name if char.isalnum() or char in " -_"
    )

    # Clean the job_name's characters and truncate for the filename
    job_name_cleaned = "".join(char for char in job_name if char.isalnum() or char in " -_")
    job_name_cleaned = job_name_cleaned[:128]

    timestamp = datetime.datetime.now()
    month_tag = timestamp.strftime("%Y-%m")
    date_tag = timestamp.strftime("%Y-%m-%d")

    month_dir = os.path.join(job_history_dir, month_tag)
    if not os.path.isdir(month_dir):
        os.makedirs(month_dir)

    # Index the files so they sort in order of submission
    number = 1
    existing_dirs = glob.glob(os.path.join(month_dir, f"{date_tag}-*"))
    if existing_dirs:
        for dir_path in existing_dirs:
            try:
                dir_number = int(os.path.basename(dir_path)[len(date_tag) + 1 :].split("-", 1)[0])
                number = max(number, dir_number + 1)
            except ValueError:
                # Skip if this dir has no number
                pass

    result = os.path.join(
        month_dir, f"{date_tag}-{number:02}-{submitter_name_cleaned}-{job_name_cleaned}"
    )
    os.makedirs(result)
    return result
