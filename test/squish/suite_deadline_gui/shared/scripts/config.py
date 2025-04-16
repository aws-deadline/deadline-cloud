# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import os

# get user's home directory
home_dir = str(Path.home())

# get python version for Deadline executable file path
python_version = f"Python{sys.version_info.major}{sys.version_info.minor}"
# Deadline executable file path on Windows
windows_deadline = f"{home_dir}/AppData/Local/Programs/Python/{python_version}/Scripts/deadline.exe"
# WINDOWS_DEADLINE_PATH environment variable
windows_deadline_path_envvar = str(
    Path(os.environ.get("WINDOWS_DEADLINE_PATH", f"{windows_deadline}"))
)


# tst_verify_settings_dialogue test suite:
profile_name = "(default)"

default_job_hist_dir_os_path = Path("~/.deadline/job_history/(default)")
custom_job_hist_dir_os_path = Path("~/.deadline/job_history/(default)/squish_test")
default_job_hist_dir = str(default_job_hist_dir_os_path)
custom_job_hist_dir = str(custom_job_hist_dir_os_path)

farm_name = "Deadline Cloud Squish Farm"
farm_desc = "Squish Automation Test Framework"
queue_name = "Squish Automation Queue"

storage_profile_linux = "Linux Storage Profile"
storage_profile_windows = "Windows Storage Profile"
storage_profile_macos = "macOS Storage Profile"

job_attachments = "COPIED"
tooltip_text_copied = (
    "When selected, the worker downloads all job attachments to disk before rendering begins."
)
tooltip_text_lightbulb = "This setting determines how job attachments are loaded on the worker instance. 'COPIED' may be faster if every task needs all attachments, while 'VIRTUAL' may perform better if tasks only require a subset of attachments."
conflict_res_option = "NOT\\_SELECTED"
conflict_res_option_expected_text = conflict_res_option.replace("\\_", "_")
logging_level = "WARNING"


# tst_verify_gui_submitter_bundles test suite:
# Simple UI with Job Attachments (simple_ui_with_ja)
simple_ui_with_ja = (
    f"{home_dir}/deadline-cloud/test/squish/deadline_gui_test_samples/simple_ui_with_ja"
)
simple_ui_with_ja_name = "Simple UI with Job Attachments"
# Simple UI - No Job Attachments (simple_ui_no_ja)
simple_ui_no_ja = f"{home_dir}/deadline-cloud/test/squish/deadline_gui_test_samples/simple_ui_no_ja"
simple_ui_no_ja_name = "Simple UI - No Job Attachments"
