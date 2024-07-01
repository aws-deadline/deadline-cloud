# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
# mypy: disable-error-code="attr-defined"

import gui_helpers
import test

profileName = "deadlinecloud\\_squish"
jobHistDir = "~/.deadline/job_history/deadlinecloud_squish"
farmName = "Deadline Cloud Squish Farm"
queueName = "Squish Automation Queue"
jobAttachments = "COPIED"
storageProfile = "Squish Storage Profile"
conflictResOption = "NOT\\_SELECTED"
loggingLevel = "WARNING"


def main():
    gui_helpers.launchDeadlineConfigGUI()
    gui_helpers.setGlobalSettings(profileName, jobHistDir)
    gui_helpers.setProfileSettings(farmName)
    gui_helpers.setFarmSettings(queueName, storageProfile, jobAttachments)
    gui_helpers.setGeneralSettings(conflictResOption, loggingLevel)
    test.log("All settings config have been applied.")
    gui_helpers.closeDeadlineConfigGUI()
