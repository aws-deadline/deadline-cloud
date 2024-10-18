# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import aws_submitter_locators
import gui_locators
import gui_helpers
import loginout_helpers
import squish
import test
import config


def open_settings_dialogue():
    test.log("Hitting `Settings` button to open Deadline Settings dialogue.")
    # click on Settings button to open Deadline Settings dialogue from Submitter
    squish.clickButton(squish.waitForObject(aws_submitter_locators.settings_button))
    test.log("Opened Settings dialogue.")
    # verify Settings dialogue is opened
    test.compare(
        squish.waitForObjectExists(gui_locators.deadline_config_dialog).visible,
        True,
        "Expect the Deadline Settings dialogue to be open.",
    )


def close_settings_dialogue():
    # click on 'OK' button to close Deadline Settings dialogue
    gui_helpers.close_deadline_config_gui()
    test.log("Closed Settings dialogue.")


def authenticate_submitter_settings_dialogue():
    # hit Settings button to open Deadline Settings dialogue
    open_settings_dialogue()
    # check for refresh error dialogues and close if present
    test.log(
        "Checking for refresh error dialogues prior to setting aws profile name for test setup."
    )
    loginout_helpers.check_and_close_refresh_error_dialogues()
    # authenticate in settings dialogue (using default aws profile) if not authenticated before running blender tests
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    # close Settings dialogue
    gui_helpers.close_deadline_config_gui()


def navigate_shared_jobsettings_tab():
    # click on Shared job settings tab
    test.log("Navigate to Shared job settings tab.")
    squish.clickTab(
        squish.waitForObject(aws_submitter_locators.shared_jobsettings_tab), "Shared job settings"
    )
    # verify on shared job settings tab
    test.compare(
        squish.waitForObjectExists(
            aws_submitter_locators.shared_jobsettings_properties_box
        ).visible,
        True,
        "Expect user to be on Shared job settings tab.",
    )


def navigate_job_specificsettings_tab():
    # click on Job-specific settings tab
    test.log("Navigate to Job-specific settings tab.")
    squish.clickTab(
        squish.waitForObject(aws_submitter_locators.job_specificsettings_tab),
        "Job-specific settings",
    )
    # verify on job specific settings tab
    test.compare(
        squish.waitForObjectExists(aws_submitter_locators.job_specificsettings_properties).visible,
        True,
        "Expect user to be on Job-specific settings tab.",
    )


def verify_shared_job_settings_tab(
    job_name: str,
    default_desc: str,
    farm_name: str,
    farm_desc: str,
    queue_name: str,
    conda_packages: str,
    conda_channels: str,
):
    # click on shared job settings tab to navigate and ensure tests are on correct tab
    navigate_shared_jobsettings_tab()
    # verify default job name is set to correct name
    test.compare(
        str(
            squish.waitForObjectExists(aws_submitter_locators.job_properties_name_input).displayText
        ),
        job_name,
        "Expect correct job bundle job name to be displayed by default.",
    )
    # verify default description contains no text
    test.compare(
        str(
            squish.waitForObjectExists(aws_submitter_locators.job_properties_desc_input).displayText
        ),
        default_desc,
        "Expect empty job bundle description to be displayed by default.",
    )
    # verify correct farm name is displayed
    test.compare(
        str(
            squish.waitForObjectExists(
                aws_submitter_locators.deadlinecloud_farmname_locator(farm_name)
            ).text
        ),
        farm_name,
        "Expect correct farm name to be displayed.",
    )
    # verify farm name tooltip contains correct farm description
    test.compare(
        str(
            squish.waitForObjectExists(
                aws_submitter_locators.deadlinecloud_farmname_locator(farm_name)
            ).toolTip
        ),
        farm_desc,
        "Expect correct farm description to be displayed.",
    )
    # verify correct queue name is displayed
    test.compare(
        str(
            squish.waitForObjectExists(
                aws_submitter_locators.deadlinecloud_queuename_locator(queue_name)
            ).text
        ),
        queue_name,
        "Expect correct queue name to be displayed.",
    )
    # verify Conda Packages contains correct Conda Package name
    test.compare(
        str(
            squish.waitForObjectExists(aws_submitter_locators.conda_packages_text_input).displayText
        ),
        conda_packages,
        "Expect correct DCC Conda Package to be displayed.",
    )
    # verify Conda Channels contains correct Conda Channel name
    test.compare(
        str(
            squish.waitForObjectExists(aws_submitter_locators.conda_channels_text_input).displayText
        ),
        conda_channels,
        "Expect correct Conda Channel to be displayed.",
    )


def verify_job_specific_settings_tab(
    frames: str, output_dir: str, output_pattern: str, output_format: str, conda_packages: str
):
    # click on job specific settings tab to test navigate and ensure tests are on correct tab
    navigate_job_specificsettings_tab()
    # verify correct frame range is displayed
    test.compare(
        str(
            squish.waitForObjectExists(
                aws_submitter_locators.render_parameters_frames_text_input
            ).displayText
        ),
        frames,
        "Expect correct frames to be input in dialogue from job bundle.",
    )
    # verify correct output directory file path is displayed
    test.compare(
        str(
            squish.waitForObjectExists(
                aws_submitter_locators.render_parameters_output_dir_filepath
            ).displayText
        ),
        output_dir,
        "Expect correct output directory file path to be input in dialogue from job bundle.",
    )
    # verify correct output file pattern is displayed
    test.compare(
        str(
            squish.waitForObjectExists(
                aws_submitter_locators.render_parameters_output_file_pattern_text_input
            ).displayText
        ),
        output_pattern,
        "Expect correct output file pattern to be input in dialogue from job bundle.",
    )
    # verify correct output file format is displayed
    test.compare(
        str(
            squish.waitForObjectExists(
                aws_submitter_locators.render_parameters_output_file_format_dropdown
            ).currentText
        ),
        output_format,
        "Expect correct output file format to be input in dialogue from job bundle.",
    )
    # verify correct Conda Packages is displayed
    test.compare(
        str(
            squish.waitForObjectExists(
                aws_submitter_locators.software_environment_condapackages_text_input
            ).displayText
        ),
        conda_packages,
        "Expect correct Conda Packages to be displayed under Software Environment.",
    )


def verify_conda_tooltip_texts():
    # click on shared job settings tab to test navigate and ensure tests are on correct tab
    navigate_shared_jobsettings_tab()
    # verify Conda Packages contains correct tooltip text
    test.compare(
        str(squish.waitForObjectExists(aws_submitter_locators.conda_packages_text_label).toolTip),
        config.tooltip_text_conda_packages,
        "Expect Conda Packages to contain correct tooltip text.",
    )
    # verify Conda Channels contains correct tooltip text
    test.compare(
        str(squish.waitForObjectExists(aws_submitter_locators.conda_channels_text_label).toolTip),
        config.tooltip_text_conda_channels,
        "Expect Conda Channels to contain correct tooltip text.",
    )
