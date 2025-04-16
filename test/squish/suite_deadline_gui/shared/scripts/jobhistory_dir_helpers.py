# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

import workstation_config_locators
import squish
import test
import platform
import os
import shutil


def open_job_hist_directory():
    # hit '...' button to open Choose Job history directory file browser
    squish.clickButton(squish.waitForObject(workstation_config_locators.open_job_hist_dir_button))
    test.log("Opened job history directory dialogue.")
    # verify job history directory dialogue is open
    test.compare(
        str(
            squish.waitForObjectExists(
                workstation_config_locators.choosejobhistdir_filebrowser
            ).windowTitle
        ),
        "Choose Job history directory",
        "Expect Choose Job history directory dialogue window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(
            workstation_config_locators.choosejobhistdir_filebrowser
        ).visible,
        True,
        "Expect Choose Job history directory dialogue to be open.",
    )
    # hit 'choose' button to set job history directory and close file browser
    squish.clickButton(
        squish.waitForObject(workstation_config_locators.choosejobhistdir_choose_button)
    )
    test.log("Set job history and closed job history directory dialogue.")


def enter_job_hist_dir_input(job_history_directory_path: str):
    # clear job history directory input field
    squish.waitForObject(workstation_config_locators.job_hist_dir_input)
    job_hist_text_input = squish.findObject(workstation_config_locators.job_hist_dir_input)
    job_hist_text_input.clear()
    # enter custom file path in job history text input
    squish.type(workstation_config_locators.job_hist_dir_input, job_history_directory_path)
    test.log("Entered job history directory file path in text input.")


def verify_job_hist_dir_text_input(job_history_directory_path: str):
    # verify job history directory text input displays correct file path based on OS platform being tested
    if platform.system() == "Linux":
        test.log("Detected test running on Linux OS")
        test.compare(
            str(
                squish.waitForObjectExists(
                    workstation_config_locators.job_hist_dir_input
                ).displayText
            ),
            job_history_directory_path,
            "Expect job history directory file path to be set correctly on Linux.",
        )
    elif platform.system() == "Windows":
        test.log("Detected test running on Windows OS")
        test.compare(
            str(
                squish.waitForObjectExists(
                    workstation_config_locators.job_hist_dir_input
                ).displayText
            ),
            job_history_directory_path,
            "Expect job history directory file path to be set correctly on Windows.",
        )
    elif platform.system() == "Darwin":
        test.log("Detected test running on macOS")
        test.compare(
            str(
                squish.waitForObjectExists(
                    workstation_config_locators.job_hist_dir_input
                ).displayText
            ),
            job_history_directory_path,
            "Expect job history directory file path to be set correctly on macOS.",
        )


def check_directory_exists(job_history_directory_path: str):
    return os.path.exists(job_history_directory_path) and os.path.isdir(job_history_directory_path)


def verify_directory_exists(job_history_directory_path: str):
    if platform.system() == "Linux":
        test.log("Detected test running on Linux OS")
        # verify job history directory exists on user's system
        if check_directory_exists(os.path.expanduser(job_history_directory_path)):
            test.passes(
                f"Job history directory '{job_history_directory_path}' exists on user's system."
            )
        else:
            test.fail("Job history directory not found on user's system")
    elif platform.system() == "Windows":
        test.log("Detected test running on Windows OS")
        # verify job history directory exists on user's system
        if check_directory_exists(os.path.normcase(os.path.expanduser(job_history_directory_path))):
            test.passes(
                f"Job history directory '{job_history_directory_path}' exists on user's system."
            )
        else:
            test.fail("Job history directory not found on user's system")
    elif platform.system() == "Darwin":
        test.log("Detected test running on macOS")
        # verify job history directory exists on user's system
        if check_directory_exists(os.path.expanduser(job_history_directory_path)):
            test.passes(
                f"Job history directory '{job_history_directory_path}' exists on user's system."
            )
        else:
            test.fail("Job history directory not found on user's system")


def delete_directory_if_exists(job_history_directory_path: str):
    if platform.system() == "Linux":
        test.log("Detected test running on Linux OS")
        # if folder exists, delete folder
        if check_directory_exists(os.path.expanduser(job_history_directory_path)):
            shutil.rmtree(os.path.expanduser(job_history_directory_path))
            test.log("Deleted job history directory folder from user's system for test cleanup")
        else:
            test.log("Job history directory not found on user's system")
    elif platform.system() == "Windows":
        test.log("Detected test running on Windows OS")
        # if folder exists, delete folder
        if check_directory_exists(os.path.normcase(os.path.expanduser(job_history_directory_path))):
            shutil.rmtree(os.path.normcase(os.path.expanduser(job_history_directory_path)))
            test.log("Deleted job history directory folder from user's system for test cleanup")
        else:
            test.log("Job history directory not found on user's system")
    elif platform.system() == "Darwin":
        test.log("Detected test running on macOS")
        # if folder exists, delete folder
        if check_directory_exists(os.path.expanduser(job_history_directory_path)):
            shutil.rmtree(os.path.expanduser(job_history_directory_path))
            test.log("Deleted job history directory folder from user's system for test cleanup")
        else:
            test.log("Job history directory not found on user's system")
