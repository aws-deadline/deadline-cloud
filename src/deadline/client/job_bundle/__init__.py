# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = [
    "adaptors",
    "create_job_history_bundle_dir",
    "read_job_bundle_parameters",
    "apply_job_parameters",
    "deadline_yaml_dump",
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
      <job_history_dir>/YYYY-mm/YYYY-mm-ddTHH-##-<submitter_name>-<job_name>
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
    existing_dirs = sorted(glob.glob(os.path.join(month_dir, f"{date_tag}-*")))
    if existing_dirs:
        latest_dir = existing_dirs[-1]
        number = int(os.path.basename(latest_dir)[len(date_tag) + 1 :].split("-", 1)[0]) + 1

    result = os.path.join(
        month_dir, f"{date_tag}-{number:02}-{submitter_name_cleaned}-{job_name_cleaned}"
    )
    os.makedirs(result)
    return result
