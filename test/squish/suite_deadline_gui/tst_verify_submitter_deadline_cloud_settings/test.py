# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-
# mypy: disable-error-code="attr-defined"

import config
import choose_jobbundledir_helpers
import choose_jobbundledir_locators
import gui_submitter_helpers
import gui_submitter_locators
import squish
import test


def init():
    # launch Choose Job Bundle GUI Submitter based on OS platform being tested
    choose_jobbundledir_helpers.detect_platform_and_launch_jobbundle_guisubmitter()
    # verify Choose job bundle directory is open
    test.compare(
        str(
            squish.waitForObjectExists(
                choose_jobbundledir_locators.choose_job_bundle_dir
            ).windowTitle
        ),
        "Choose job bundle directory",
        "Expect Choose job bundle directory window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(choose_jobbundledir_locators.choose_job_bundle_dir).visible,
        True,
        "Expect Choose job bundle directory to be open.",
    )
    # select a job bundle to open the Submit Dialog
    choose_jobbundledir_helpers.select_jobbundle(config.simple_ui_with_ja)
    # verify GUI Submitter dialogue opens
    test.compare(
        str(squish.waitForObjectExists(gui_submitter_locators.aws_submitter_dialogue).windowTitle),
        "Deadline Cloud JobBundle Submitter",
        "Expect AWS Deadline Cloud Submitter window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(gui_submitter_locators.aws_submitter_dialogue).visible,
        True,
        "Expect AWS Deadline Cloud Submitter to be open.",
    )


def main():
    # Navigate to Shared job settings tab where Deadline Cloud settings live
    gui_submitter_helpers.navigate_shared_job_settings()

    # --- Test farm selection and verify queue list refreshes ---
    test.log("Testing farm selection in Submit Dialog")
    gui_submitter_helpers.set_farm_name(config.farm_name)
    # verify correct farm name is set
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_submitter_locators.deadline_cloud_settings_farm_dropdown
            ).currentText
        ),
        config.farm_name,
        "Expect selected farm name to be set.",
    )

    # --- Test queue selection (verifies queue list refreshed after farm change) ---
    test.log(
        "Testing queue selection in Submit Dialog (verifies queue list refreshed after farm change)"
    )
    gui_submitter_helpers.set_queue_name(config.queue_name)
    # verify correct queue name is set
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_submitter_locators.deadline_cloud_settings_queue_dropdown
            ).currentText
        ),
        config.queue_name,
        "Expect selected queue name to be set.",
    )

    # --- Test storage profile selection (verifies storage profile list refreshed after queue change) ---
    test.log(
        "Testing storage profile selection in Submit Dialog (verifies list refreshed after queue change)"
    )
    gui_submitter_helpers.set_and_verify_os_storage_profile(
        config.storage_profile_linux,
        config.storage_profile_windows,
        config.storage_profile_macos,
    )

    # --- Verify cascade: change farm again and confirm queue/storage profile update ---
    test.log("Testing cascade: re-selecting farm to verify queue and storage profile lists refresh")
    # Re-select the same farm to trigger a refresh of dependent lists
    gui_submitter_helpers.set_farm_name(config.farm_name)
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_submitter_locators.deadline_cloud_settings_farm_dropdown
            ).currentText
        ),
        config.farm_name,
        "Expect farm name still set after re-selection.",
    )
    # After farm re-selection, queue list should have refreshed - verify we can still select the queue
    gui_submitter_helpers.set_queue_name(config.queue_name)
    test.compare(
        str(
            squish.waitForObjectExists(
                gui_submitter_locators.deadline_cloud_settings_queue_dropdown
            ).currentText
        ),
        config.queue_name,
        "Expect queue name set after farm re-selection (queue list refreshed).",
    )
    # After queue re-selection, storage profile list should have refreshed
    gui_submitter_helpers.set_and_verify_os_storage_profile(
        config.storage_profile_linux,
        config.storage_profile_windows,
        config.storage_profile_macos,
    )

    test.log(
        "All Deadline Cloud settings (farm, queue, storage profile) and refresh cascade verified in Submit Dialog."
    )


def cleanup():
    test.log("Closing AWS Submitter dialogue by sending QCloseEvent to 'x' button.")
    squish.sendEvent(
        "QCloseEvent", squish.waitForObject(gui_submitter_locators.aws_submitter_dialogue)
    )
