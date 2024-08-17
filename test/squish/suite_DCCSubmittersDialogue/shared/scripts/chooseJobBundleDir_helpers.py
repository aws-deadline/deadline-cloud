# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import chooseJobBundleDir_locators
import squish
import test

blenderJobBundleFilePath = "/home/rocky/deadline-cloud-fork/deadline-cloud/test/squish/jobbundle_testfiles/chicken_job"

snoozeTimeoutLow = 3 # seconds
snoozeTimeoutMed = 6 # seconds

def launchChooseJobBundleDirectory():
    squish.startApplication("deadline bundle gui-submit --browse")
    test.log("Launching Choose Job Bundle Directory")
    test.log(
        "Sleep for " + str(snoozeTimeoutLow) + " seconds to allow Choose Job Bundle Directory to fully load."
    )
    squish.snooze(snoozeTimeoutLow)
    test.compare(
        str(squish.waitForObjectExists(chooseJobBundleDir_locators.qFileDialog_QFileDialog).windowTitle), 
        "Choose job bundle directory", 
        "Expecting the Choose Job history directory dialogue Window title to be appear.",
    )
    test.compare(
        squish.waitForObjectExists(chooseJobBundleDir_locators.qFileDialog_QFileDialog).visible,
        True,
        "Expecting the Choose Job history directory dialogue box to be open.",
    )
    
def selectBlenderJobBundleAndLoadSubmitter():
    squish.mouseClick(squish.waitForObject(chooseJobBundleDir_locators.fileNameEdit_QLineEdit))
    squish.type(squish.waitForObject(chooseJobBundleDir_locators.fileNameEdit_QLineEdit), blenderJobBundleFilePath)
    squish.clickButton(squish.waitForObject(chooseJobBundleDir_locators.qFileDialog_Choose_QPushButton))
    squish.snooze(snoozeTimeoutMed)
    
    