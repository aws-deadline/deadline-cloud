# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import choose_jobbundledir_locators
import squish
import test


def launch_choose_jobbundle_directory():
    squish.startApplication("deadline bundle gui-submit --browse")
    test.log("Launched Choose Job Bundle Directory.")
    # verify Choose Job Bundle directory is open.
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


def enter_jobbundle_directory(filepath: str):
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
