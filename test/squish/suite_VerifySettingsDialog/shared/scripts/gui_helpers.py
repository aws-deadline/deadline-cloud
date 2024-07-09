# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import gui_locators
import squish
import test

snoozeTimeout = 3  # seconds


def launchDeadlineConfigGUI():
    squish.startApplication("deadline config gui")
    test.log("Launching Deadline Config GUI")
    test.log(
        "Sleep for " + str(snoozeTimeout) + " seconds to allow GUI authentication to fully load."
    )
    squish.snooze(snoozeTimeout)
    test.compare(
        squish.waitForObjectExists(
            gui_locators.aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog
        ).visible,
        True,
        "Expect the Deadline Config GUI to be open.",
    )


def setGlobalSettings(profileName: str):
    # open AWS profile drop down menu
    squish.mouseClick(
        squish.waitForObjectExists(gui_locators.global_settings_AWS_profile_QComboBox),
    )
    test.compare(
        squish.waitForObjectExists(gui_locators.aWS_profile_deadlinecloud_squish_QModelIndex).text,
        profileName,
        "Expect AWS profile name to be present in drop down.",
    )
    # select AWS profile: `deadlinecloud_squish`
    squish.mouseClick(gui_locators.aWS_profile_deadlinecloud_squish_QModelIndex)
    test.log("Set Global settings")


def setProfileSettings(jobHistDir: str, farmName: str):
    test.compare(
        str(squish.waitForObjectExists(gui_locators.profile_settings_QLineEdit).displayText),
        jobHistDir,
        "Expect Job history directory default path to be equal.",
    )
    # open Default farm drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.profile_settings_QComboBox))
    test.compare(
        squish.waitForObjectExists(gui_locators.deadline_Cloud_Squish_Farm_QModelIndex).text,
        farmName,
        "Expect Farm name to be present in drop down.",
    )
    # select Default farm: `Deadline Cloud Squish Farm`
    squish.mouseClick(squish.waitForObjectItem(gui_locators.profile_settings_QComboBox, farmName))
    test.log("Set Profile settings")


def openAndSetDefaultJobHistDirectory():
    # hit '...' button to open Choose Job history directory file browser
    squish.clickButton(squish.waitForObject(gui_locators.profile_settings_QPushButton))
    test.compare(
        str(squish.waitForObjectExists(gui_locators.qFileDialog_QFileDialog).windowTitle),
        "Choose Job history directory",
        "Expect Choose Job history directory dialogue to be open.",
    )
    test.compare(
        squish.waitForObjectExists(gui_locators.qFileDialog_QFileDialog).visible,
        True,
        "Expect Choose Job history directory dialogue to be open.",
    )
    # hit 'choose' button to set default and close file browser
    squish.clickButton(squish.waitForObject(gui_locators.profile_settings_Choose_QPushButton))
    test.log("Open and set default Job history directory")


def setFarmSettings(queueName: str, storageProfile: str, jobAttachments: str):
    # open Default queue drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.farm_settings_QComboBox))
    test.compare(
        squish.waitForObjectExists(gui_locators.squish_Automation_Queue_QModelIndex).text,
        queueName,
        "Expect Queue name to be present in drop down.",
    )
    # select Default queue: `Squish Automation Queue`
    squish.mouseClick(squish.waitForObjectItem(gui_locators.farm_settings_QComboBox, queueName))
    # open Default storage profile drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.farm_settings_QComboBox_2))
    test.compare(
        squish.waitForObjectExists(gui_locators.squish_Storage_Profile_QModelIndex).text,
        storageProfile,
        "Expect Storage profile name to be present in drop down.",
    )
    # select Default storage profile: `Squish Storage Profile`
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.farm_settings_QComboBox_2, storageProfile)
    )
    # open Job attachments filesystem options drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.farm_settings_QComboBox_3))
    # select Job attachments filesystem options: COPIED
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.farm_settings_QComboBox_3, jobAttachments)
    )
    test.log("Set Farm settings")


def setGeneralSettings(conflictResOption: str, loggingLevel: str):
    # verify Auto accept prompt defaults check box can be checked
    test.compare(
        squish.waitForObjectExists(
            gui_locators.general_settings_Auto_accept_prompt_defaults_QCheckBox
        ).checkable,
        True,
        "Expect `Auto accept prompt defaults` check box to be enabled.",
    )
    # verify Auto accept prompt defaults check box is unchecked (default setting)
    test.compare(
        squish.waitForObjectExists(
            gui_locators.general_settings_Auto_accept_prompt_defaults_QCheckBox
        ).checked,
        False,
        "Expect `Auto accept prompt defaults` check box to be unchecked by default.",
    )
    # verify Telemetry opt out check box can be checked
    test.compare(
        squish.waitForObjectExists(
            gui_locators.general_settings_Telemetry_opt_out_QCheckBox
        ).checkable,
        True,
        "Expect `Telemetry opt out` check box to be enabled.",
    )
    # verify Telemetry opt out check box is checked (default setting)
    test.compare(
        squish.waitForObjectExists(
            gui_locators.general_settings_Telemetry_opt_out_QCheckBox
        ).checked,
        True,
        "Expect `Telemetry opt out` check box to be checked by default.",
    )
    # open Conflict resolution option drop down menu
    squish.mouseClick(
        squish.waitForObject(gui_locators.general_settings_Conflict_resolution_option_QComboBox),
    )
    # select Conflict resolution option: NOT_SELECTED
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.general_settings_Conflict_resolution_option_QComboBox, conflictResOption
        ),
    )
    # open Current logging level drop down menu
    squish.mouseClick(
        squish.waitForObject(gui_locators.general_settings_Current_logging_level_QComboBox),
    )
    # select Current logging level: WARNING
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.general_settings_Current_logging_level_QComboBox, loggingLevel
        ),
    )
    test.log("Set General settings")


def hitApplyButton():
    # hit 'Apply' button
    squish.clickButton(
        squish.waitForObject(
            gui_locators.aWS_Deadline_Cloud_workstation_configuration_Apply_QPushButton
        )
    )


def closeDeadlineConfigGUI():
    test.log("Hit `OK` button to close GUI")
    # hit 'OK' button to close Deadline Config GUI
    squish.clickButton(
        squish.waitForObject(
            gui_locators.aWS_Deadline_Cloud_workstation_configuration_OK_QPushButton
        )
    )
