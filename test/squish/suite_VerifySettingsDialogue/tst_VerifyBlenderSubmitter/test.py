# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
# mypy: disable-error-code="attr-defined"

import config
import choose_jobbundledir_helpers
import blender_submitter_helpers
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
    # hit Settings button to open Deadline Settings dialogue
    choose_jobbundledir_helpers.open_settings_dialogue()
    # check for refresh error dialogues and close if present
    loginout_helpers.check_and_close_refresh_error_dialogues()
    # authenticate in settings dialogue (using default aws profile) if not authenticated before running blender tests
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    # close Settings dialogue
    choose_jobbundledir_helpers.close_settings_dialogue()

def main():
    # verify shared job settings tab contains correct defaults based on selected blender job bundle
    choose_jobbundledir_helpers.verify_shared_job_settings()
    # verify job-specific settings tab contains correct defaults based on selected blender job bundle
    
    # verify job attachments tab contains correct defaults based on selected blender job bundle
    # verify settings can be opened and closed from Submitter dialogue
    # verify export bundle button works
    # verify job bundle can be successfully submitted to deadline cloud farm
    
    
    
def cleanup():
    # reset aws profile name to `(default)`
    choose_jobbundledir_helpers.open_settings_dialogue()
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    choose_jobbundledir_helpers.close_settings_dialogue()
    # close AWS Submitter dialogue by hitting 'x'
    sendEvent("QCloseEvent", waitForObject(names.submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog))










