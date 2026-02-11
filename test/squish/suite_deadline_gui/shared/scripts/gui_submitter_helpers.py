# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import gui_submitter_locators
import platform
import squish
import test


def navigate_shared_job_settings():
    # click on Shared job settings tab
    test.log("Navigate to Shared job settings tab.")
    squish.clickTab(
        squish.waitForObject(gui_submitter_locators.shared_jobsettings_tab), "Shared job settings"
    )
    # verify on shared job settings tab
    test.compare(
        squish.waitForObjectExists(
            gui_submitter_locators.shared_jobsettings_properties_box
        ).visible,
        True,
        "Expect user to be on Shared job settings tab.",
    )


def navigate_job_specific_settings():
    # click on Job-specific settings tab
    test.log("Navigate to Job-specific settings tab.")
    squish.clickTab(
        squish.waitForObject(gui_submitter_locators.job_specificsettings_tab),
        "Job-specific settings",
    )
    # verify on job specific settings tab
    test.compare(
        squish.waitForObjectExists(gui_submitter_locators.job_specificsettings_properties).visible,
        True,
        "Expect user to be on Job-specific settings tab.",
    )


def verify_shared_job_settings(
    job_name: str,
):
    # click on shared job settings tab to navigate and ensure tests are on correct tab
    navigate_shared_job_settings()
    # verify job name is set correctly
    test.compare(
        str(
            squish.waitForObjectExists(gui_submitter_locators.job_properties_name_input).displayText
        ),
        job_name,
        "Expect correct job bundle job name to be displayed by default.",
    )


def _wait_for_combo_box_value(locator, expected_value: str, timeout_ms: int = 5000):
    """Wait for a combo box to show the expected value (handles async refresh)."""
    import time

    start_time = time.time()
    timeout_sec = timeout_ms / 1000.0
    while time.time() - start_time < timeout_sec:
        try:
            current_text = str(squish.waitForObjectExists(locator).currentText)
            if current_text == expected_value:
                return True
            if current_text != "<refreshing>":
                # Value stabilized but doesn't match - fail fast
                return False
        except Exception:
            pass
        time.sleep(0.1)
    return False


def set_farm_name(farm_name: str):
    """Set the farm in the Submit Dialog's Deadline Cloud settings."""
    # Ensure we're on the Shared job settings tab
    navigate_shared_job_settings()
    # Open farm dropdown menu
    squish.mouseClick(
        squish.waitForObject(gui_submitter_locators.deadline_cloud_settings_farm_dropdown)
    )
    test.log("Opened farm name drop down menu in Submit Dialog.")
    test.compare(
        squish.waitForObjectExists(
            gui_submitter_locators.farm_name_dropdown_locator(farm_name)
        ).text,
        farm_name,
        "Expect farm name to be present in drop down.",
    )
    # Select the farm
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_submitter_locators.deadline_cloud_settings_farm_dropdown, farm_name
        )
    )
    test.log(f"Selected farm name: {farm_name}")
    # Wait for async refresh to complete
    test.verify(
        _wait_for_combo_box_value(
            gui_submitter_locators.deadline_cloud_settings_farm_dropdown, farm_name
        ),
        f"Farm combo box should show '{farm_name}' after async refresh",
    )


def set_queue_name(queue_name: str):
    """Set the queue in the Submit Dialog's Deadline Cloud settings."""
    # Ensure we're on the Shared job settings tab
    navigate_shared_job_settings()
    # Open queue dropdown menu
    squish.mouseClick(
        squish.waitForObject(gui_submitter_locators.deadline_cloud_settings_queue_dropdown)
    )
    test.log("Opened queue name drop down menu in Submit Dialog.")
    test.compare(
        squish.waitForObjectExists(
            gui_submitter_locators.queue_name_dropdown_locator(queue_name)
        ).text,
        queue_name,
        "Expect queue name to be present in drop down.",
    )
    # Select the queue
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_submitter_locators.deadline_cloud_settings_queue_dropdown, queue_name
        )
    )
    test.log(f"Selected queue name: {queue_name}")
    # Wait for async refresh to complete
    test.verify(
        _wait_for_combo_box_value(
            gui_submitter_locators.deadline_cloud_settings_queue_dropdown, queue_name
        ),
        f"Queue combo box should show '{queue_name}' after async refresh",
    )


def set_storage_profile(storage_profile: str):
    """Set the storage profile in the Submit Dialog's Deadline Cloud settings."""
    # Ensure we're on the Shared job settings tab
    navigate_shared_job_settings()
    # Open storage profile dropdown menu
    squish.mouseClick(
        squish.waitForObject(
            gui_submitter_locators.deadline_cloud_settings_storage_profile_dropdown
        )
    )
    test.log("Opened storage profile drop down menu in Submit Dialog.")
    test.compare(
        squish.waitForObjectExists(
            gui_submitter_locators.storage_profile_dropdown_locator(storage_profile)
        ).text,
        storage_profile,
        "Expect storage profile to be present in drop down.",
    )
    # Select the storage profile
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_submitter_locators.deadline_cloud_settings_storage_profile_dropdown,
            storage_profile,
        )
    )
    test.log(f"Selected storage profile: {storage_profile}")
    # Wait for async refresh to complete
    test.verify(
        _wait_for_combo_box_value(
            gui_submitter_locators.deadline_cloud_settings_storage_profile_dropdown, storage_profile
        ),
        f"Storage profile combo box should show '{storage_profile}' after async refresh",
    )


def set_and_verify_os_storage_profile(
    linux_storage_profile: str, windows_storage_profile: str, macos_storage_profile: str
):
    """Set and verify storage profile based on OS platform being tested."""
    if platform.system() == "Linux":
        test.log("Detected test running on Linux OS")
        set_storage_profile(linux_storage_profile)
        test.compare(
            str(
                squish.waitForObjectExists(
                    gui_submitter_locators.deadline_cloud_settings_storage_profile_dropdown
                ).currentText
            ),
            linux_storage_profile,
            "Expect selected storage profile to be set.",
        )
    elif platform.system() == "Windows":
        test.log("Detected test running on Windows OS")
        set_storage_profile(windows_storage_profile)
        test.compare(
            str(
                squish.waitForObjectExists(
                    gui_submitter_locators.deadline_cloud_settings_storage_profile_dropdown
                ).currentText
            ),
            windows_storage_profile,
            "Expect selected storage profile to be set.",
        )
    elif platform.system() == "Darwin":
        test.log("Detected test running on macOS")
        set_storage_profile(macos_storage_profile)
        test.compare(
            str(
                squish.waitForObjectExists(
                    gui_submitter_locators.deadline_cloud_settings_storage_profile_dropdown
                ).currentText
            ),
            macos_storage_profile,
            "Expect selected storage profile to be set.",
        )
    test.log("Selected and verified storage profile based on OS platform being tested.")
