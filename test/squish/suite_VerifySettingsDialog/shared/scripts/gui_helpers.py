# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import gui_locators
import squish
import test

snoozeTimeout = 1  # seconds


def launchDeadlineConfigGUI():
    squish.startApplication("deadline config gui")
    test.log("Launched Deadline Config GUI.")
    test.log(
        "Sleep for " + str(snoozeTimeout) + " second(s) to allow GUI authentication to fully load."
    )
    squish.snooze(snoozeTimeout)
    test.compare(
        squish.waitForObjectExists(
            gui_locators.aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog
        ).visible,
        True,
        "Expect the Deadline Config GUI to be open.",
    )


def closeDeadlineConfigGUI():
    test.log("Hitting `OK` button to close Deadline Config GUI.")
    # hit 'OK' button to close Deadline Config GUI
    squish.clickButton(
        squish.waitForObject(
            gui_locators.aWS_Deadline_Cloud_workstation_configuration_OK_QPushButton
        )
    )


# apply button bug: https://sim.amazon.com/issues/Bea-28289
def hitApplyButton():
    test.log("Hitting `Apply` button to apply selected settings.")
    # hit 'Apply' button
    squish.clickButton(
        squish.waitForObject(
            gui_locators.aWS_Deadline_Cloud_workstation_configuration_Apply_QPushButton
        )
    )
    test.log("Settings have been applied.")


def setAWSProfileNameAndVerifyAuthentication(profileName: str):
    # open AWS profile drop down menu
    squish.mouseClick(
        squish.waitForObjectExists(gui_locators.global_settings_AWS_profile_QComboBox),
    )
    test.log("Opened AWS profile drop down menu.")
    test.compare(
        squish.waitForObjectExists(gui_locators.profileNameLocator(profileName)).text,
        profileName,
        "Expect AWS profile name to be present in drop down.",
    )
    # select AWS profile
    squish.mouseClick(gui_locators.profileNameLocator(profileName))
    test.log("Selected AWS profile name.")
    # verify user is authenticated - confirm statuses appear and text is correct
    test.log("Verifying user is authenticated...")
    test.compare(
        squish.waitForObjectExists(
            gui_locators.credential_source_b_style_color_green_HOST_PROVIDED_b_QLabel
        ).visible,
        True,
        "Expect `Credential source: HOST_PROVIDED` to be visible when selected aws profile.",
    )
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.credential_source_b_style_color_green_HOST_PROVIDED_b_QLabel
            ).text
        ),
        "<b style='color:green;'>HOST_PROVIDED</b>",
        "Expect `Credential source: HOST_PROVIDED` text to be correct when selected aws profile.",
    )
    test.compare(
        squish.waitForObjectExists(
            gui_locators.authentication_status_b_style_color_green_AUTHENTICATED_b_QLabel
        ).visible,
        True,
        "Expect `Authentication status: AUTHENTICATED` to be visible.",
    )
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.authentication_status_b_style_color_green_AUTHENTICATED_b_QLabel
            ).text
        ),
        "<b style='color:green;'>AUTHENTICATED</b>",
        "Expect `Authentication status: AUTHENTICATED` text to be correct.",
    )
    test.compare(
        squish.waitForObjectExists(
            gui_locators.aWS_Deadline_Cloud_API_b_style_color_green_AUTHORIZED_b_QLabel
        ).visible,
        True,
        "Expect `AWS Deadline Cloud API: AUTHORIZED` to be visible.",
    )
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.aWS_Deadline_Cloud_API_b_style_color_green_AUTHORIZED_b_QLabel
            ).text
        ),
        "<b style='color:green;'>AUTHORIZED</b>",
        "Expect `AWS Deadline Cloud API: AUTHORIZED` text to be correct.",
    )


def setFarmName(farmName: str):
    # open Default farm drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.profile_settings_QComboBox))
    test.log("Opened farm name drop down menu.")
    test.compare(
        squish.waitForObjectExists(gui_locators.deadline_Cloud_Squish_Farm_QModelIndex).text,
        farmName,
        "Expect farm name to be present in drop down.",
    )
    # select Default farm
    squish.mouseClick(squish.waitForObjectItem(gui_locators.profile_settings_QComboBox, farmName))
    test.log("Selected farm name.")


def openAndCloseJobHistDirectory():
    # hit '...' button to open Choose Job history directory file browser
    squish.clickButton(squish.waitForObject(gui_locators.profile_settings_QPushButton))
    test.log("Opened job history directory dialogue.")
    # verify job history directory dialogue is open
    test.compare(
        str(squish.waitForObjectExists(gui_locators.qFileDialog_QFileDialog).windowTitle),
        "Choose Job history directory",
        "Expect Choose Job history directory dialogue window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(gui_locators.qFileDialog_QFileDialog).visible,
        True,
        "Expect Choose Job history directory dialogue to be open.",
    )
    # hit 'choose' button to set default and close file browser
    squish.clickButton(squish.waitForObject(gui_locators.profile_settings_Choose_QPushButton))
    test.log("Closed job history directory dialogue.")


def setQueueName(queueName: str):
    # open Default queue drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.farm_settings_QComboBox))
    test.log("Opened queue name drop down menu.")
    test.compare(
        squish.waitForObjectExists(gui_locators.squish_Automation_Queue_QModelIndex).text,
        queueName,
        "Expect queue name to be present in drop down.",
    )
    # select Default queue
    squish.mouseClick(squish.waitForObjectItem(gui_locators.farm_settings_QComboBox, queueName))
    test.log("Selected queue name.")


def setStorageProfile(storageProfile: str):
    # open Default storage profile drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.farm_settings_QComboBox_2))
    test.log("Opened storage profile drop down menu.")
    test.compare(
        squish.waitForObjectExists(gui_locators.squish_Storage_Profile_QModelIndex).text,
        storageProfile,
        "Expect storage profile to be present in drop down.",
    )
    # select Default storage profile
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.farm_settings_QComboBox_2, storageProfile)
    )
    test.log("Selected storage profile.")


def setJobAttachmentsFilesystemOptions(jobAttachments: str):
    # open Job attachments filesystem options drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.farm_settings_QComboBox_3))
    test.log("Opened job attachments filesystem options drop down menu.")
    # select Job attachments filesystem options
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.farm_settings_QComboBox_3, jobAttachments)
    )
    test.log("Selected job attachments filesystem option.")


def setConflictResolutionOption(conflictResOption: str):
    # open Conflict resolution option drop down menu
    squish.mouseClick(
        squish.waitForObject(gui_locators.general_settings_Conflict_resolution_option_QComboBox),
    )
    test.log("Opened conflict resolution option drop down menu.")
    # select Conflict resolution option
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.general_settings_Conflict_resolution_option_QComboBox, conflictResOption
        ),
    )
    test.log("Selected conflict resolution option.")


def setCurrentLoggingLevel(loggingLevel: str):
    # open Current logging level drop down menu
    squish.mouseClick(
        squish.waitForObject(gui_locators.general_settings_Current_logging_level_QComboBox),
    )
    test.log("Opened current logging level drop down menu.")
    # select Current logging level
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.general_settings_Current_logging_level_QComboBox, loggingLevel
        ),
    )
    test.log("Selected current logging level.")
