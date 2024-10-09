# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
# mypy: disable-error-code="attr-defined"

import config
import choose_jobbundledir_helpers
import aws_submitter_helpers
import gui_helpers
import loginout_helpers
import squish
import test
import names


def init():
    # launch deadline bundle gui-submit --browse
    choose_jobbundledir_helpers.launch_choose_jobbundle_directory()
    # enter blender job bundle file path and hit 'choose'
    choose_jobbundledir_helpers.enter_jobbundle_directory(config.blender_filepath)
    # verify AWS Deadline Cloud Submitter dialogue is open
    test.compare(str(squish.waitForObjectExists(names.submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog).windowTitle),
        "Submit to AWS Deadline Cloud", 
        "Expect AWS Deadline Cloud Submitter window title to be present.")
    test.compare(
        squish.waitForObjectExists(names.submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog).visible,
        True,
        "Expect AWS Deadline Cloud Submitter to be open.",
    )
    # hit Settings button to open Deadline Settings dialogue
    test.log("Opening Settings dialogue to set aws profile name for test setup.")
    aws_submitter_helpers.open_settings_dialogue()
    # check for refresh error dialogues and close if present
    test.log("Checking for refresh error dialogues prior to setting aws profile name for test setup.")
    loginout_helpers.check_and_close_refresh_error_dialogues()
    # authenticate in settings dialogue (using default aws profile) if not authenticated before running blender tests
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    # close Settings dialogue
    gui_helpers.close_deadline_config_gui()
    # verify AWS Deadline Cloud Submitter dialogue remains open
    test.compare(str(squish.waitForObjectExists(names.submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog).windowTitle), 
        "Submit to AWS Deadline Cloud", 
        "Expect AWS Deadline Cloud Submitter window title to be present after closing Settings dialogue.")
    test.compare(
        squish.waitForObjectExists(names.submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog).visible,
        True,
        "Expect AWS Deadline Cloud Submitter to be open after closing Settings dialogue.",
    )
    # verify on shared job settings tab
    test.compare(squish.waitForObjectExists(names.o_SharedJobSettingsWidget).visible, True, "Expect User to be on Shared job settings tab.")

def main():
    # verify shared job settings tab contains correct job properties based on selected blender job bundle
    aws_submitter_helpers.verify_job_properties(config.blender_job_name, config.empty_desc)
    # verify shared job settings tab contains correct Deadline Cloud settings
    aws_submitter_helpers.verify_deadline_cloud_settings(config.farm_name, config.farm_desc, config.queue_name)
    # verify shared job settings tab contains correct Queue Environment
    aws_submitter_helpers.verify_queue_environment(config.blender_conda_package, config.conda_channel)
    # verify shared job settings tab contain correct Conda tooltip texts
    aws_submitter_helpers.verify_conda_tooltip_texts()
    # navigate to job-specific settings tab 
    aws_submitter_helpers.navigate_job_specificsettings_tab()
    # verify job-specific settings tab contains correct render parameters based on selected blender job bundle
    aws_submitter_helpers.verify_render_parameters(config.blender_frames, config.blender_output_dir, config.blender_output_pattern, config.blender_output_format)
    # verify job-specific settings tab contains correct software environment based on selected blender job bundle
    aws_submitter_helpers.verify_software_env(config.blender_conda_package)
    # navigate back to shared job settings tab
    aws_submitter_helpers.navigate_shared_jobsettings_tab()
    
def cleanup():
    # reset aws profile name to `(default)`
    aws_submitter_helpers.open_settings_dialogue()
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    test.log("Reset aws profile name to `default` for test cleanup.")
    gui_helpers.close_deadline_config_gui()
    # close AWS Submitter dialogue by sending QCloseEvent to 'x'
    test.log("Closing AWS Submitter dialogue by sending QCloseEvent to 'x' button.")
    sendEvent("QCloseEvent", waitForObject(names.submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog))

