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
    choose_jobbundledir_helpers.launch_choosejobbundle_directory()
    # enter blender job bundle file path and hit 'choose'
    choose_jobbundledir_helpers.enter_jobbundle_directory(config.blender_filepath)
    # authenticate in settings dialogue (using default aws profile) if not authenticated before running blender tests


def main():
    # verify shared job settings tab contains correct defaults based on selected blender job bundle
    test.compare(str(squish.waitForObjectExists(names.name_QLineEdit).displayText), config.blender_job_bundle_folder)
    # verify job-specific settings tab contains correct defaults based on selected blender job bundle
    # verify job attachments tab contains correct defaults based on selected blender job bundle
    # verify settings can be opened and closed from Submitter dialogue
    choose_jobbundledir_helpers.open_close_settings_dialogue()
    # verify export bundle button works
    # verify job bundle can be successfully submitted to deadline cloud farm
    
    
    
def cleanup():
    # reset aws profile name to `(default)`
    # close Blender Submitter dialogue










