# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

import config
import gui_helpers
import gui_locators
import loginout_helpers
import squish

import test


def init():
    # launch deadline config gui
    gui_helpers.launch_deadline_config_gui()
    # using aws credential/non-DCM profile, set aws profile name to `(default)`
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    # verify correct aws profile name is set
    test.compare(
        squish.waitForObjectExists(gui_locators.globalsettings_awsprofile_dropdown).currentText,
        config.profile_name,
        "Expect selected AWS profile name to be set.",
    )


def main():
    # verify default job history directory file path is correct
    test.compare(
        squish.waitForObjectExists(gui_locators.job_hist_dir_input).displayText,
        config.job_hist_dir,
        "Expect correct job history directory file path to be displayed by default.",
    )
    # open and close job history directory file browser
    gui_helpers.open_close_job_hist_directory()
    # verify selected job history directory path is set
    test.compare(
        str(squish.waitForObjectExists(gui_locators.job_hist_dir_input).displayText),
        config.job_hist_dir,
        "Expect selected job history directory file path to be set.",
    )
    # set farm name
    gui_helpers.set_farm_name(config.farm_name)
    # verify correct farm name is set
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.profilesettings_defaultfarm_dropdown
            ).currentText
        ),
        config.farm_name,
        "Expect selected farm name to be set.",
    )
    # set queue name
    gui_helpers.set_queue_name(config.queue_name)
    # verify correct queue name is set
    test.compare(
        str(
            squish.waitForObjectExists(gui_locators.farmsettings_defaultqueue_dropdown).currentText
        ),
        config.queue_name,
        "Expect selected queue name to be set.",
    )
    # set storage profile
    gui_helpers.set_storage_profile(config.storage_profile)
    # verify correct storage profile name is set
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.farmsettings_defaultstorageprofile_dropdown
            ).currentText
        ),
        config.storage_profile,
        "Expect selected storage profile to be set.",
    )
    # set job attachments filesystem options
    gui_helpers.set_job_attachments_filesystem_options(config.job_attachments)
    # verify job attachments filesystem options is set to 'COPIED'
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.farmsettings_jobattachmentsoptions_dropdown
            ).currentText
        ),
        config.job_attachments,
        "Expect selected job attachment filesystem option to be set.",
    )
    # verify 'COPIED' contains correct tooltip text
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.farmsettings_jobattachmentsoptions_dropdown
            ).toolTip
        ),
        config.tooltip_text_copied,
        "Expect COPIED to contain correct tooltip text.",
    )
    # verify job attachments filesystem options lightbulb icon contains correct tooltip text
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_locators.jobattachments_filesystemoptions_lightbulb_icon
            ).toolTip
        ),
        config.tooltip_text_lightbulb,
        "Expect job attachments filesystem options lightbulb icon to contain correct tooltip text.",
    )
    # verify auto accept prompt defaults checkbox is checkable
    test.compare(
        squish.waitForObjectExists(gui_locators.autoaccept_promptdefaults_checkbox).checkable,
        True,
        "Expect auto accept prompt defaults checkbox to be checkable.",
    )
    # verify telemetry opt out checkbox is checkable
    test.compare(
        squish.waitForObjectExists(gui_locators.telemetry_optout_checkbox).checkable,
        True,
        "Expect telemetry opt out checkbox to be checkable.",
    )
    # set conflict resolution option
    gui_helpers.set_conflict_resolution_option(config.conflict_res_option)
    # verify conflict resolution option is set to 'NOT_SELECTED'
    test.compare(
        str(
            squish.waitForObjectExists(gui_locators.conflictresolution_option_dropdown).currentText
        ),
        config.conflict_res_option_expected_text,
        "Expect selected conflict resolution option to be set.",
    )
    # set current logging level option
    gui_helpers.set_current_logging_level(config.logging_level)
    # verify current logging level option is set to 'WARNING'
    test.compare(
        str(squish.waitForObjectExists(gui_locators.currentlogging_level_dropdown).currentText),
        config.logging_level,
        "Expect selected current logging level to be set.",
    )
    test.log("All deadline config GUI settings have been set.")


def cleanup():
    # reset aws profile name to `(default)`
    loginout_helpers.set_aws_profile_name_and_verify_auth(config.profile_name)
    test.log("Reset aws profile name to `(default)` for test cleanup.")
    # close deadline config gui
    gui_helpers.close_deadline_config_gui()
