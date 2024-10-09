# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import aws_submitter_locators
import gui_locators
import gui_helpers
import squish
import test
import config
import names

def open_settings_dialogue():
    test.log("Hitting `Settings` button to open Deadline Settings dialogue.")
    # click on Settings button to open Deadline Settings dialogue from Submitter
    squish.clickButton(squish.waitForObject(names.submit_to_AWS_Deadline_Cloud_Settings_QPushButton))
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
    
def navigate_job_specificsettings_tab():
    # click on Job-specific settings tab
    test.log("Navigate to Job-specific settings tab.")
    squish.clickTab(squish.waitForObject(names.submit_to_AWS_Deadline_Cloud_QTabWidget), "Job-specific settings")
    # verify on job specific settings tab
    test.compare(squish.waitForObjectExists(names.o_JobBundleSettingsWidget).visible, True, "Expect user to be on Job-specific settings tab.")
    
def navigate_shared_jobsettings_tab():
    # click on Shared job settings tab
    test.log("Navigate to Shared job settings tab.")
    squish.clickTab(squish.waitForObject(names.submit_to_AWS_Deadline_Cloud_QTabWidget), "Shared job settings")
    # verify on shared job settings tab
    test.compare(squish.waitForObjectExists(names.o_SharedJobSettingsWidget).visible, True, "Expect user to be on Shared job settings tab.")
    
def verify_job_properties(job_name:str, default_desc:str):
    # verify default job name is set to correct name
    test.compare(str(squish.waitForObjectExists(names.name_QLineEdit).displayText), job_name, "Expect correct job bundle job name to be displayed by default.")
    # verify default description contains no text
    test.compare(str(squish.waitForObjectExists(names.job_Properties_Description_QLineEdit).displayText), default_desc, "Expect empty job bundle description to be displayed by default.")
    
def verify_deadline_cloud_settings(farm_name:str, farm_desc:str, queue_name:str):
    # verify correct farm name is displayed
    test.compare(str(squish.waitForObjectExists(names.deadline_Cloud_settings_Deadline_Cloud_Squish_Farm_QLabel).text), farm_name, "Expect correct farm name to be displayed.")
    # verify farm name tooltip contains correct farm description
    test.compare(str(squish.waitForObjectExists(names.deadline_Cloud_settings_Deadline_Cloud_Squish_Farm_QLabel).toolTip), farm_desc, "Expect correct farm description to be displayed.")
    # verify correct queue name is displayed
    test.compare(str(squish.waitForObjectExists(names.deadline_Cloud_settings_Squish_Automation_Queue_QLabel).text), queue_name, "Expect correct queue name to be displayed.")
    
def verify_queue_environment(conda_packages:str, conda_channels:str):     
    # verify Conda Packages contains correct Conda Package name
    test.compare(str(squish.waitForObjectExists(names.queue_Environment_Conda_Conda_Packages_QLineEdit).displayText), conda_packages, "Expect correct DCC Conda Package to be displayed.")
    # verify Conda Channels contains correct Conda Channel name
    test.compare(str(squish.waitForObjectExists(names.queue_Environment_Conda_Conda_Channels_QLineEdit).displayText), conda_channels, "Expect correct Conda Channel to be displayed.")

def verify_conda_tooltip_texts():
    # verify Conda Packages contains correct tooltip text
    test.compare(str(squish.waitForObjectExists(names.queue_Environment_Conda_Conda_Packages_QLabel).toolTip), config.tooltip_text_conda_packages, "Expect Conda Packages to contain correct tooltip text.")
    # verify Conda Channels contains correct tooltip text
    test.compare(str(squish.waitForObjectExists(names.queue_Environment_Conda_Conda_Channels_QLabel).toolTip), config.tooltip_text_conda_channels, "Expect Conda Channels to contain correct tooltip text.")

def verify_render_parameters(frames:str, output_dir:str, output_pattern:str, output_format:str):
    # verify correct frame range is displayed
    test.compare(str(squish.waitForObjectExists(names.render_Parameters_Frames_QLineEdit).displayText), frames, "Expect correct frames to be input in dialogue from job bundle.")
    # verify correct output directory file path is displayed
    test.compare(str(squish.waitForObjectExists(names.render_Parameters_QLineEdit).displayText), output_dir, "Expect correct output directory file path to be input in dialogue from job bundle.")
    # verify correct output file pattern is displayed
    test.compare(str(squish.waitForObjectExists(names.render_Parameters_Output_File_Pattern_QLineEdit).displayText), output_pattern, "Expect correct output file pattern to be input in dialogue from job bundle.")
    # verify correct output file format is displayed
    test.compare(str(squish.waitForObjectExists(names.render_Parameters_Output_File_Format_QComboBox).currentText), output_format, "Expect correct output file format to be input in dialogue from job bundle.")
    
def verify_software_env(conda_packages:str):
    # verify correct Conda Packages is displayed
    test.compare(str(squish.waitForObjectExists(names.software_Environment_Conda_Packages_QLineEdit).displayText), conda_packages, "Expect correct Conda Packages to be displayed under Software Environment.")
    