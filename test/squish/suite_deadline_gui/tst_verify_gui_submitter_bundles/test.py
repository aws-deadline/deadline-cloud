# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
# mypy: disable-error-code="attr-defined"

import config
import choose_jobbundledir_helpers
import choose_jobbundledir_locators
import gui_submitter_helpers
import gui_submitter_locators
import squish

import test


def init():
    # launch Choose job bundle directory using deadline bundle gui-submit --browse command
    choose_jobbundledir_helpers.launch_jobbundle_dir()
    # verify Choose job bundle directory is open
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


def main():
    # select job bundle one
    choose_jobbundledir_helpers.select_jobbundle(config.jobbundle_one)
    # verify GUI Submitter dialogue opens
    test.compare(
        str(squish.waitForObjectExists(gui_submitter_locators.aws_submitter_dialogue).windowTitle),
        "Submit to AWS Deadline Cloud",
        "Expect AWS Deadline Cloud Submitter window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(gui_submitter_locators.aws_submitter_dialogue).visible,
        True,
        "Expect AWS Deadline Cloud Submitter to be open.",
    )
    # verify shared job settings tab for job bundle one
    test.log("Start verifying Shared Job Settings tab for Job Bundle One")
    gui_submitter_helpers.verify_shared_job_settings(
        config.jobbundle_one_name,
    )
    # verify load different job bundle flow
    test.log("Navigate to Job-Specific Settings tab and verify Load a different job bundle flow")
    choose_jobbundledir_helpers.load_different_job_bundle()
    # select job bundle two
    choose_jobbundledir_helpers.select_jobbundle(config.jobbundle_two)
    # verify shared job settings tab for job bundle two
    test.log("Start verifying Shared Job Settings tab for Job Bundle Two")
    gui_submitter_helpers.verify_shared_job_settings(
        config.jobbundle_two_name,
    )


def cleanup():
    test.log("Closing AWS Submitter dialogue by sending QCloseEvent to 'x' button.")
    squish.sendEvent(
        "QCloseEvent", squish.waitForObject(gui_submitter_locators.aws_submitter_dialogue)
    )
