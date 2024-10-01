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

def launch_choose_jobbundle_directory():
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

def open_settings_dialogue():
    # click on Settings button to open Deadline Settings dialogue from Submitter
    squish.clickButton(squish.waitForObject(names.submit_to_AWS_Deadline_Cloud_Settings_QPushButton))
    # verify Settings dialogue is opened
    test.compare(
        squish.waitForObjectExists(gui_locators.deadline_config_dialog).visible,
        True,
        "Expect the Deadline Settings dialogue to be open.",
    )
    
def close_settings_dialogue():
    # click on 'OK' button to close Deadline Settings dialogue
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
   
def verify_shared_job_settings():
    # verify shared job settings widget is visible when on shared job settings tab
    test.compare(waitForObjectExists(names.o_SharedJobSettingsWidget).visible, True)
    # verify default job name is set
    test.compare(str(squish.waitForObjectExists(names.name_QLineEdit).displayText), config.blender_job_bundle_folder)
    # verify default description contains no text
    test.compare(str(waitForObjectExists(names.job_Properties_Description_QLineEdit).displayText), "")
    # verify correct farm name is displayed
    test.compare(str(waitForObjectExists(names.deadline_Cloud_settings_Deadline_Cloud_Squish_Farm_QLabel).text), "Deadline Cloud Squish Farm")
    # verify farm name tooltip contains correct farm description
    test.compare(str(waitForObjectExists(names.deadline_Cloud_settings_Deadline_Cloud_Squish_Farm_QLabel).toolTip), "Squish Automation Test Framework")
    # verify correct queue name is displayed
    test.compare(str(waitForObjectExists(names.deadline_Cloud_settings_Squish_Automation_Queue_QLabel).text), "Squish Automation Queue")
    # verify Conda Packages contains correct tooltip text
    test.compare(str(waitForObjectExists(names.queue_Environment_Conda_Conda_Packages_QLabel).toolTip), "This is a space-separated list of Conda package match specifications to install for the job. E.g. \"blender=3.6\" for a job that renders frames in Blender 3.6.\nSee https://docs.conda.io/projects/conda/en/latest/user-guide/concepts/pkg-specs.html#package-match-specifications\n")
    # verify Conda Channels contains correct tooltip text
    test.compare(str(waitForObjectExists(names.queue_Environment_Conda_Conda_Channels_QLabel).toolTip), "This is a space-separated list of Conda channels from which to install packages. Deadline Cloud SMF packages are installed from the \"deadline-cloud\" channel that is configured by Deadline Cloud.\nAdd \"conda-forge\" to get packages from the https://conda-forge.org/ community, and \"defaults\" to get packages from Anaconda Inc (make sure your usage complies with https://www.anaconda.com/terms-of-use).\n")
    # verify authentication status widget is present when on shared job settings tab
    test.compare(waitForObjectExists(names.submit_to_AWS_Deadline_Cloud_DeadlineAuthenticationStatusWidget).visible, True)
    
# def submit_blender_job_bundle():
    
    

    
    
    
    
    
    