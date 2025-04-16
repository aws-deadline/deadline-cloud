# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

import config
import workstation_config_helpers
import workstation_config_locators
import loginout_helpers
import jobhistory_dir_helpers
import squish
import test


def init():
    # launch Deadline Workstation Config dialog based on OS platform being tested
    workstation_config_helpers.detect_platform_and_launch_deadline_config()
    # clear and reset job history directory text input
    jobhistory_dir_helpers.enter_job_hist_dir_input(config.default_job_hist_dir)
    # verify text input displays correct default job history directory path to be tested
    jobhistory_dir_helpers.verify_job_hist_dir_text_input(config.default_job_hist_dir)
    # delete/clean-up job history directory folder in user's system
    jobhistory_dir_helpers.delete_directory_if_exists(config.custom_job_hist_dir)
    test.log("Reset job history directory for test setup.")
    # using aws credential/non-DCM profile, set aws profile name to `(default)`
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    # verify correct aws profile name is set
    test.compare(
        squish.waitForObjectExists(
            workstation_config_locators.globalsettings_awsprofile_dropdown
        ).currentText,
        config.profile_name,
        "Expect selected AWS profile name to be set.",
    )


def main():
    # enter custom job history directory file path
    jobhistory_dir_helpers.enter_job_hist_dir_input(config.custom_job_hist_dir)
    # open job history directory to set custom path and to create new directory
    jobhistory_dir_helpers.open_job_hist_directory()
    # verify text input displays correct custom job history directory path
    jobhistory_dir_helpers.verify_job_hist_dir_text_input(config.custom_job_hist_dir)
    # verify custom job history folder is created/exists in user's system
    jobhistory_dir_helpers.verify_directory_exists(config.custom_job_hist_dir)
    # set farm name
    workstation_config_helpers.set_farm_name(config.farm_name)
    # verify correct farm name is set
    test.compare(
        str(
            squish.waitForObjectExists(
                workstation_config_locators.profilesettings_defaultfarm_dropdown
            ).currentText
        ),
        config.farm_name,
        "Expect selected farm name to be set.",
    )
    # set queue name
    workstation_config_helpers.set_queue_name(config.queue_name)
    # verify correct queue name is set
    test.compare(
        str(
            squish.waitForObjectExists(
                workstation_config_locators.farmsettings_defaultqueue_dropdown
            ).currentText
        ),
        config.queue_name,
        "Expect selected queue name to be set.",
    )
    # set and verify storage profile based on OS platform being tested
    workstation_config_helpers.set_and_verify_os_storage_profile(
        config.storage_profile_linux, config.storage_profile_windows, config.storage_profile_macos
    )
    # set job attachments filesystem options
    workstation_config_helpers.set_job_attachments_filesystem_options(config.job_attachments)
    # verify job attachments filesystem options is set to 'COPIED'
    test.compare(
        str(
            squish.waitForObjectExists(
                workstation_config_locators.farmsettings_jobattachmentsoptions_dropdown
            ).currentText
        ),
        config.job_attachments,
        "Expect selected job attachment filesystem option to be set.",
    )
    # verify 'COPIED' contains correct tooltip text
    test.compare(
        str(
            squish.waitForObjectExists(
                workstation_config_locators.farmsettings_jobattachmentsoptions_dropdown
            ).toolTip
        ),
        config.tooltip_text_copied,
        "Expect COPIED to contain correct tooltip text.",
    )
    # verify job attachments filesystem options lightbulb icon contains correct tooltip text
    test.compare(
        str(
            squish.waitForObjectExists(
                workstation_config_locators.jobattachments_filesystemoptions_lightbulb_icon
            ).toolTip
        ),
        config.tooltip_text_lightbulb,
        "Expect job attachments filesystem options lightbulb icon to contain correct tooltip text.",
    )
    # verify auto accept prompt defaults checkbox is checkable
    test.compare(
        squish.waitForObjectExists(
            workstation_config_locators.autoaccept_promptdefaults_checkbox
        ).checkable,
        True,
        "Expect auto accept prompt defaults checkbox to be checkable.",
    )
    # verify telemetry opt out checkbox is checkable
    test.compare(
        squish.waitForObjectExists(workstation_config_locators.telemetry_optout_checkbox).checkable,
        True,
        "Expect telemetry opt out checkbox to be checkable.",
    )
    # set conflict resolution option
    workstation_config_helpers.set_conflict_resolution_option(config.conflict_res_option)
    # verify conflict resolution option is set to 'NOT_SELECTED'
    test.compare(
        str(
            squish.waitForObjectExists(
                workstation_config_locators.conflictresolution_option_dropdown
            ).currentText
        ),
        config.conflict_res_option_expected_text,
        "Expect selected conflict resolution option to be set.",
    )
    # set current logging level option
    workstation_config_helpers.set_current_logging_level(config.logging_level)
    # verify current logging level option is set to 'WARNING'
    test.compare(
        str(
            squish.waitForObjectExists(
                workstation_config_locators.currentlogging_level_dropdown
            ).currentText
        ),
        config.logging_level,
        "Expect selected current logging level to be set.",
    )
    test.log("All Deadline Workstation Config settings have been set.")


def cleanup():
    # reset aws profile name to `(default)`
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    test.log("Reset aws profile name to `(default)` for test cleanup.")
    # reset job history directory setting
    jobhistory_dir_helpers.enter_job_hist_dir_input(config.default_job_hist_dir)
    # verify text input displays correct default job history directory path to be tested
    jobhistory_dir_helpers.verify_job_hist_dir_text_input(config.default_job_hist_dir)
    # delete/clean-up custom job history directory folder in user's system
    jobhistory_dir_helpers.delete_directory_if_exists(config.custom_job_hist_dir)
    test.log("Reset job history directory back to default for test cleanup.")
    # close Deadline Workstation Config
    workstation_config_helpers.close_deadline_config_gui()
