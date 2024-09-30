# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import choose_jobbundledir_locators
import gui_helpers
import gui_locators
import squish
import test
import config
import names


snooze_timeout_low = 1 # seconds
snooze_timeout_med = 3 # seconds
snooze_timeout_high = 5 # seconds

def launch_choosejobbundle_directory():
    squish.startApplication("deadline bundle gui-submit --browse")
    test.log("Launched Choose Job Bundle Directory.")
    # verify Choose Job Bundle directory is open.
    test.compare(
        str(squish.waitForObjectExists(choose_jobbundledir_locators.qFileDialog_QFileDialog).windowTitle), 
        "Choose job bundle directory", 
        "Expect Choose job bundle directory window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(choose_jobbundledir_locators.qFileDialog_QFileDialog).visible,
        True,
        "Expect Choose job bundle directory to be open.",
    )
    
def enter_jobbundle_directory(filepath: str):
    # enter job bundle directory file path in directory text input
    squish.type(squish.waitForObject(names.fileNameEdit_QLineEdit), filepath)
    test.log("Entered job bundle file path in Choose Job Bundle Directory text input.")
    # verify text input appears
    test.compare(str(squish.waitForObjectExists(names.fileNameEdit_QLineEdit).displayText), filepath, "Expect job bundle directory file path to be input.")
    # hit 'choose' button
    test.log("Hitting 'Choose' button to open Submitter dialogue for selected job bundle.")
    squish.clickButton(squish.waitForObject(names.qFileDialog_Choose_QPushButton))
    # verify AWS Deadline Cloud Submitter dialogue is open
    test.compare(str(squish.waitForObjectExists(names.submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog).windowTitle), 
        "Submit to AWS Deadline Cloud", 
        "Expect AWS Deadline Cloud Submitter window title to be present.")
    test.compare(
        squish.waitForObjectExists(names.submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog).visible,
        True,
        "Expect AWS Deadline Cloud Submitter to be open.",
    )

def open_close_settings_dialogue():
    # click on Settings button to open Settings dialogue from Submitter
    squish.clickButton(squish.waitForObject(names.submit_to_AWS_Deadline_Cloud_Settings_QPushButton))
    # verify Settings dialogue is opened
    test.compare(
        squish.waitForObjectExists(gui_locators.deadline_config_dialog).visible,
        True,
        "Expect the Submitter Settings dialogue to be open.",
    )
    # click on 'OK' button to close Settings dialogue
    gui_helpers.close_deadline_config_gui()
    # verify Submitter dialogue remains open
    test.compare(
        str(squish.waitForObjectExists(choose_jobbundledir_locators.qFileDialog_QFileDialog).windowTitle), 
        "Choose job bundle directory", 
        "Expect Choose job bundle directory window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(choose_jobbundledir_locators.qFileDialog_QFileDialog).visible,
        True,
        "Expect Choose job bundle directory dialogue to be open.",
    )
    
def submit_blender_job_bundle():
    
    

    
    
    
    
    
    