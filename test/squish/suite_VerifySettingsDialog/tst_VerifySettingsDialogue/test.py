# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
# mypy: disable-error-code="attr-defined"

import gui_helpers
import gui_locators
import loginout_helpers
import squish
import test

profileName = "(default)"
jobHistDir = "~/.deadline/job_history/(default)"
farmName = "Deadline Cloud Squish Farm"
queueName = "Squish Automation Queue"
storageProfile = "Squish Storage Profile"
jobAttachments = "COPIED"
tooltipTextCOPIED = (
    "When selected, the worker downloads all job attachments to disk before rendering begins."
)
tooltipTextLightbulb = "This setting determines how job attachments are loaded on the worker instance. 'COPIED' may be faster if every task needs all attachments, while 'VIRTUAL' may perform better if tasks only require a subset of attachments."
conflictResOption = "NOT\\_SELECTED"
conflictResOptionExpectedText = conflictResOption.replace("\\_", "_")
loggingLevel = "WARNING"


def init():
    # launch deadline config gui
    gui_helpers.launchDeadlineConfigGUI()
    # check for refresh error dialogues and close if present
    loginout_helpers.checkAndCloseRefreshErrorDialogues()
    # using aws credential/non-DCM profile, set aws profile name to `(default)`
    gui_helpers.setAWSProfileNameAndVerifyAuthentication(profileName)
    # verify correct aws profile name is set
    test.compare(
        squish.waitForObjectExists(gui_locators.global_settings_AWS_profile_QComboBox).currentText,
        profileName,
        "Expect selected AWS profile name to be set.",
    )


def main():
    # verify default job history directory file path is correct
    test.compare(
        squish.waitForObjectExists(gui_locators.profile_settings_QLineEdit).displayText,
        jobHistDir,
        "Expect correct job history directory file path to be displayed by default.",
    )
    # open and close job history directory file browser
    gui_helpers.openAndCloseJobHistDirectory()
    # verify selected job history directory path is set
    test.compare(
        str(squish.waitForObjectExists(gui_locators.profile_settings_QLineEdit).displayText),
        jobHistDir,
        "Expect selected job history directory file path to be set.",
    )
    # set farm name
    gui_helpers.setFarmName(farmName)
    # verify correct farm name is set
    test.compare(
        str(squish.waitForObjectExists(gui_locators.profile_settings_QComboBox).currentText),
        farmName,
        "Expect selected farm name to be set.",
    )
    # set queue name
    gui_helpers.setQueueName(queueName)
    # verify correct queue name is set
    test.compare(
        str(squish.waitForObjectExists(gui_locators.farm_settings_QComboBox).currentText),
        queueName,
        "Expect selected queue name to be set.",
    )
    # set storage profile
    gui_helpers.setStorageProfile(storageProfile)
    # verify correct storage profile name is set
    test.compare(
        str(squish.waitForObjectExists(gui_locators.farm_settings_QComboBox_2).currentText),
        storageProfile,
        "Expect selected storage profile to be set.",
    )
    # set job attachments filesystem options
    gui_helpers.setJobAttachmentsFilesystemOptions(jobAttachments)
    # verify job attachments filesystem options is set to 'COPIED'
    test.compare(
        str(squish.waitForObjectExists(gui_locators.farm_settings_QComboBox_3).currentText),
        jobAttachments,
        "Expect selected job attachment filesystem option to be set.",
    )
    # verify 'COPIED' contains correct tooltip text
    test.compare(
        str(squish.waitForObjectExists(gui_locators.farm_settings_QComboBox_3).toolTip),
        tooltipTextCOPIED,
        "Expect COPIED to contain correct tooltip text.",
    )
    # verify job attachments filesystem options lightbulb icon contains correct tooltip text
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.farm_settings_Job_attachments_filesystem_options_QLabel_2
            ).toolTip
        ),
        tooltipTextLightbulb,
        "Expect job attachments filesystem options lightbulb icon to contain correct tooltip text.",
    )
    # verify auto accept prompt defaults checkbox is checkable
    test.compare(
        squish.waitForObjectExists(
            gui_locators.general_settings_Auto_accept_prompt_defaults_QCheckBox
        ).checkable,
        True,
        "Expect auto accept prompt defaults checkbox to be checkable.",
    )
    # verify telemetry opt out checkbox is checkable
    test.compare(
        squish.waitForObjectExists(
            gui_locators.general_settings_Telemetry_opt_out_QCheckBox
        ).checkable,
        True,
        "Expect telemetry opt out checkbox to be checkable.",
    )
    # set conflict resolution option
    gui_helpers.setConflictResolutionOption(conflictResOption)
    # verify conflict resolution option is set to 'NOT_SELECTED'
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.general_settings_Conflict_resolution_option_QComboBox
            ).currentText
        ),
        conflictResOptionExpectedText,
        "Expect selected conflict resolution option to be set.",
    )
    # set current logging level option
    gui_helpers.setCurrentLoggingLevel(loggingLevel)
    # verify current logging level option is set to 'WARNING'
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.general_settings_Current_logging_level_QComboBox
            ).currentText
        ),
        loggingLevel,
        "Expect selected current logging level to be set.",
    )
    # disabling due to https://sim.amazon.com/issues/Bea-28289
    # gui_helpers.hitApplyButton()
    test.log("All deadline config GUI settings have been set.")


def cleanup():
    # reset aws profile name to `(default)`
    gui_helpers.setAWSProfileNameAndVerifyAuthentication(profileName)
    test.log("Reset aws profile name to `(default)` for test cleanup.")
    # close deadline config gui
    gui_helpers.closeDeadlineConfigGUI()
