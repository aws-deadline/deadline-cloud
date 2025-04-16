# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import choose_jobbundledir_locators
import gui_submitter_helpers
import gui_submitter_locators
import config
import squish
import test
import platform


# launch Choose Job Bundle GUI Submitter on linux and macOS
def launch_jobbundle_dir():
    squish.startApplication("deadline bundle gui-submit --browse")
    test.log("Launched Choose Job Bundle Directory.")


# launch Choose Job Bundle GUI Submitter on windows
def launch_jobbundle_dir_windows_only():
    squish.startApplication(
        f"python {config.windows_deadline_path_envvar} bundle gui-submit --browse"
    )
    test.log("Launched Choose Job Bundle Directory on Windows.")


# launch Choose Job Bundle GUI Submitter based on OS platform being tested
def detect_platform_and_launch_jobbundle_guisubmitter():
    if platform.system() == "Linux":
        test.log("Detected test running on Linux OS")
        test.log("Launching Choose Job Bundle GUI Submitter on Linux")
        launch_jobbundle_dir()
    elif platform.system() == "Windows":
        test.log("Detected test running on Windows OS")
        test.log("Launching Choose Job Bundle GUI Submitter on Windows")
        launch_jobbundle_dir_windows_only()
    elif platform.system() == "Darwin":
        test.log("Detected test running on macOS")
        test.log("Launching Choose Job Bundle GUI Submitter on macOS")
        launch_jobbundle_dir()
    else:
        test.log("Unknown operating system")


def select_jobbundle(filepath: str):
    # enter job bundle directory file path in directory text input
    squish.type(
        squish.waitForObject(choose_jobbundledir_locators.jobbundle_filepath_input), filepath
    )
    test.log("Entered job bundle file path in Choose Job Bundle Directory.")
    # verify text input appears
    test.compare(
        str(
            squish.waitForObjectExists(
                choose_jobbundledir_locators.jobbundle_filepath_input
            ).displayText
        ),
        filepath,
        "Expect job bundle file path to be input in dialogue.",
    )
    # hit 'choose' button
    test.log("Hitting 'Choose' button to open Submitter dialogue for selected job bundle.")
    squish.clickButton(
        squish.waitForObject(choose_jobbundledir_locators.choose_jobbundledir_button)
    )


def load_different_job_bundle():
    # click on job specific settings tab to navigate and ensure tests are on correct tab
    gui_submitter_helpers.navigate_job_specific_settings()
    # verify load different job bundle button exists and contains correct button text
    test.compare(
        str(
            squish.waitForObjectExists(gui_submitter_locators.load_different_job_bundle_button).text
        ),
        "Load a different job bundle",
        "Expect Load a different job bundle button to contain correct text.",
    )
    # verify load a different job bundle button is enabled
    test.compare(
        squish.waitForObjectExists(gui_submitter_locators.load_different_job_bundle_button).enabled,
        True,
        "Expect Load a different job bundle button to be enabled.",
    )
    # click on load a different job bundle button
    test.log("Hitting `Load a different job bundle` button.")
    squish.clickButton(
        squish.waitForObject(gui_submitter_locators.load_different_job_bundle_button)
    )
    # verify Choose Job Bundle directory is open
    test.compare(
        str(
            squish.waitForObjectExists(
                choose_jobbundledir_locators.choose_job_bundle_dir
            ).windowTitle
        ),
        "Choose job bundle directory",
        "Expect Choose job bundle directory window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(choose_jobbundledir_locators.choose_job_bundle_dir).visible,
        True,
        "Expect Choose job bundle directory to be open.",
    )
