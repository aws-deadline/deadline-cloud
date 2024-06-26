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


def setGlobalSettings(profileName: str, jobHistDir: str):
    test.compare(
        str(squish.waitForObjectExists(gui_locators.profile_settings_QLineEdit).displayText),
        jobHistDir,
        "Expect job history directory default path to be equal.",
    )
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.global_settings_AWS_profile_QComboBox, profileName),
        484,
        9,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.global_settings_AWS_profile_QComboBox, profileName),
        225,
        6,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    test.log("Set Global settings")


def setProfileSettings(farmName: str):
    squish.mouseClick(
        squish.waitForObject(gui_locators.profile_settings_QComboBox),
        407,
        13,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.profile_settings_QComboBox, farmName),
        189,
        10,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    test.log("Set Profile settings")


def setFarmSettings(queueName: str, storageProfile: str, jobAttachments: str):
    squish.mouseClick(
        squish.waitForObject(gui_locators.farm_settings_QComboBox),
        392,
        10,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.farm_settings_QComboBox, queueName),
        200,
        9,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObject(gui_locators.farm_settings_QComboBox_2),
        389,
        12,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.farm_settings_QComboBox_2, storageProfile),
        185,
        11,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObject(gui_locators.farm_settings_QComboBox_3),
        323,
        8,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.farm_settings_QComboBox_3, jobAttachments),
        183,
        12,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    test.log("Set Farm settings")


def setGeneralSettings(conflictResOption: str, loggingLevel: str):
    squish.mouseClick(
        squish.waitForObject(gui_locators.general_settings_Conflict_resolution_option_QComboBox),
        382,
        10,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.general_settings_Conflict_resolution_option_QComboBox, conflictResOption
        ),
        202,
        6,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObject(gui_locators.general_settings_Current_logging_level_QComboBox),
        382,
        14,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.general_settings_Current_logging_level_QComboBox, loggingLevel
        ),
        204,
        10,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    squish.mouseClick(
        squish.waitForObject(
            gui_locators.aWS_Deadline_Cloud_workstation_configuration_Apply_QPushButton
        ),
        30,
        8,
        squish.Qt.NoModifier,
        squish.Qt.LeftButton,
    )
    test.log("Set General settings")


def closeDeadlineConfigGUI():
    test.log("Hit `OK` button to close GUI")
    squish.clickButton(
        squish.waitForObject(
            gui_locators.aWS_Deadline_Cloud_workstation_configuration_OK_QPushButton
        )
    )
