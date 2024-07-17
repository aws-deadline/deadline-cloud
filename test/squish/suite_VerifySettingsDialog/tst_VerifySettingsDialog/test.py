# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
# mypy: disable-error-code="attr-defined"

import gui_PageObjects
import test

profileName = "deadlinecloud_squish"
jobHistDir = "~/.deadline/job_history/deadlinecloud_squish"
farmName = "Deadline Cloud Squish Farm"
queueName = "Squish Automation Queue"
jobAttachments = "COPIED"
storageProfile = "Squish Storage Profile"
conflictResOption = "NOT\\_SELECTED"
loggingLevel = "WARNING"


def main():
    gui_PageObjects.launchDeadlineConfigGUI()
    gui_PageObjects.setGlobalSettings(profileName)
    gui_PageObjects.setProfileSettings(jobHistDir, farmName)
    gui_PageObjects.openAndSetDefaultJobHistDirectory()
    gui_PageObjects.setFarmSettings(queueName, storageProfile, jobAttachments)
    gui_PageObjects.setGeneralSettings(conflictResOption, loggingLevel)
    # disabling due to https://sim.amazon.com/issues/Bea-28289
    # gui_PageObjects.hitApplyButton()
    test.log("All settings config have been applied.")
    gui_PageObjects.closeDeadlineConfigGUI()
    
    