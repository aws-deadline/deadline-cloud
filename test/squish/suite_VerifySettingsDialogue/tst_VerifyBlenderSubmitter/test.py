# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
# mypy: disable-error-code="attr-defined"

import config
import choose_jobbundledir_helpers
import aws_submitter_helpers
import aws_submitter_locators
import squish
import test


def init():
    # launch deadline bundle gui-submit --browse and select Blender job bundle
    choose_jobbundledir_helpers.launch_jobbundle_dir_and_select_jobbundle(config.blender_filepath)
    # verify AWS Deadline Cloud Submitter dialogue is open
    test.compare(
        str(squish.waitForObjectExists(aws_submitter_locators.aws_submitter_dialogue).windowTitle),
        "Submit to AWS Deadline Cloud",
        "Expect AWS Deadline Cloud Submitter window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(aws_submitter_locators.aws_submitter_dialogue).visible,
        True,
        "Expect AWS Deadline Cloud Submitter to be open.",
    )
    # open settings dialogue and authenticate using aws default profile
    aws_submitter_helpers.authenticate_submitter_settings_dialogue()


def main():
    # verify shared job settings tab contains correct job properties defaults based on selected blender job bundle
    test.log("Start verifying Shared Job Settings tests...")
    aws_submitter_helpers.verify_shared_job_settings_tab(
        config.blender_job_name,
        config.empty_desc,
        config.farm_name,
        config.farm_desc,
        config.queue_name,
        config.blender_conda_package,
        config.conda_channel,
    )
    # verify job-specific settings tab contains correct defaults based on selected blender job bundle
    test.log("Start verifying Job-Specific Settings tests...")
    aws_submitter_helpers.verify_job_specific_settings_tab(
        config.blender_frames,
        config.blender_output_dir,
        config.blender_output_pattern,
        config.blender_output_format,
        config.blender_conda_package,
    )
    # verify AWS Submitter dialogue contains correct tool tip texts (on shared job settings tab)
    test.log("Start verifying correct tooltip texts...")
    aws_submitter_helpers.verify_conda_tooltip_texts()
    # verify load different job bundle directory
    test.log("Start verifying Load a different job bundle tests...")
    choose_jobbundledir_helpers.verify_load_different_jobbundle_dir()


def cleanup():
    test.log("Start test cleanup...")
    # reset aws profile name to `(default)`
    aws_submitter_helpers.authenticate_submitter_settings_dialogue()
    test.log("Reset aws profile name to `default` for test cleanup.")
    # close AWS Submitter dialogue by sending QCloseEvent to 'x' button
    test.log("Closing AWS Submitter dialogue by sending QCloseEvent to 'x' button.")
    squish.sendEvent(
        "QCloseEvent", squish.waitForObject(aws_submitter_locators.aws_submitter_dialogue)
    )
