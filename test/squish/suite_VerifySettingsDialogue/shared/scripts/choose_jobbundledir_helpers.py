# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import choose_jobbundledir_locators
import aws_submitter_helpers
import aws_submitter_locators
import squish
import test


def launch_jobbundle_dir_and_select_jobbundle(filepath: str):
    squish.startApplication("deadline bundle gui-submit --browse")
    test.log("Launched Choose Job Bundle Directory.")
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


def verify_load_different_jobbundle_dir():
    # click on job specific settings tab to navigate and ensure tests are on correct tab
    aws_submitter_helpers.navigate_job_specificsettings_tab()
    # verify load different job bundle button exists and contains correct button text
    test.compare(
        str(
            squish.waitForObjectExists(aws_submitter_locators.load_different_job_bundle_button).text
        ),
        "Load a different job bundle",
        "Expect Load a different job bundle button to contain correct text.",
    )
    # verify load a different job bundle button is enabled
    test.compare(
        squish.waitForObjectExists(aws_submitter_locators.load_different_job_bundle_button).enabled,
        True,
        "Expect Load a different job bundle button to be enabled.",
    )
    # click on load a different job bundle button
    test.log("Hitting `Load a different job bundle` button.")
    squish.clickButton(
        squish.waitForObject(aws_submitter_locators.load_different_job_bundle_button)
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
    # hit cancel button to close out of choose job bundle directory
    test.log("Hitting `Cancel` button to close Choose job bundle directory.")
    squish.clickButton(
        squish.waitForObject(choose_jobbundledir_locators.jobbundledir_cancel_button)
    )
