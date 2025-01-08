# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import gui_submitter_locators
import squish
import test


def navigate_shared_job_settings():
    # click on Shared job settings tab
    test.log("Navigate to Shared job settings tab.")
    squish.clickTab(
        squish.waitForObject(gui_submitter_locators.shared_jobsettings_tab), "Shared job settings"
    )
    # verify on shared job settings tab
    test.compare(
        squish.waitForObjectExists(
            gui_submitter_locators.shared_jobsettings_properties_box
        ).visible,
        True,
        "Expect user to be on Shared job settings tab.",
    )


def navigate_job_specific_settings():
    # click on Job-specific settings tab
    test.log("Navigate to Job-specific settings tab.")
    squish.clickTab(
        squish.waitForObject(gui_submitter_locators.job_specificsettings_tab),
        "Job-specific settings",
    )
    # verify on job specific settings tab
    test.compare(
        squish.waitForObjectExists(gui_submitter_locators.job_specificsettings_properties).visible,
        True,
        "Expect user to be on Job-specific settings tab.",
    )


def verify_shared_job_settings(
    job_name: str,
):
    # click on shared job settings tab to navigate and ensure tests are on correct tab
    navigate_shared_job_settings()
    # verify job name is set correctly
    test.compare(
        str(
            squish.waitForObjectExists(gui_submitter_locators.job_properties_name_input).displayText
        ),
        job_name,
        "Expect correct job bundle job name to be displayed by default.",
    )
