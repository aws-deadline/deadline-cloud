# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

import workstation_config_locators
import gui_submitter_locators
import config
import squish
import test
import platform

snooze_timeout = 1  # seconds


# launch Deadline Workstation Config on linux and macOS
def launch_deadline_config_gui():
    squish.startApplication("deadline config gui")
    test.log("Launched Deadline Workstation Config GUI.")
    test.log(
        "Sleep for " + str(snooze_timeout) + " second(s) to allow authentication to fully load."
    )
    squish.snooze(snooze_timeout)
    test.compare(
        squish.waitForObjectExists(workstation_config_locators.deadline_config_dialog).visible,
        True,
        "Expect the Deadline Workstation Config GUI to be open.",
    )


# launch Deadline Workstation Config on windows
def launch_deadline_config_gui_windows_only():
    squish.startApplication(f"python {config.windows_deadline_path_envvar} config gui")
    test.log("Launched Deadline Workstation Config GUI on Windows.")
    test.log(
        "Sleep for " + str(snooze_timeout) + " second(s) to allow authentication to fully load."
    )
    squish.snooze(snooze_timeout)
    test.compare(
        squish.waitForObjectExists(workstation_config_locators.deadline_config_dialog).visible,
        True,
        "Expect the Deadline Workstation Config GUI to be open.",
    )


# launch Deadline Workstation Config based on OS platform being tested
def detect_platform_and_launch_deadline_config():
    if platform.system() == "Linux":
        test.log("Detected test running on Linux OS")
        test.log("Launching Deadline Workstation Config on Linux")
        launch_deadline_config_gui()
    elif platform.system() == "Windows":
        test.log("Detected test running on Windows OS")
        test.log("Launching Deadline Workstation Config on Windows")
        launch_deadline_config_gui_windows_only()
    elif platform.system() == "Darwin":
        test.log("Detected test running on macOS")
        test.log("Launching Deadline Workstation Config on macOS")
        launch_deadline_config_gui()
    else:
        test.log("Unknown operating system")


def close_deadline_config_gui():
    test.log("Hitting `OK` button to close Deadline Settings.")
    # hit 'OK' button to close Deadline Config GUI
    squish.clickButton(squish.waitForObject(workstation_config_locators.deadlinedialog_ok_button))


def open_settings_dialogue():
    test.log("Hitting `Settings` button to open Deadline Settings dialogue.")
    # click on Settings button to open Deadline Settings dialogue from Submitter
    squish.clickButton(squish.waitForObject(gui_submitter_locators.settings_button))
    test.log("Opened Deadline Workstation Settings dialogue.")
    # verify Settings dialogue is opened
    test.compare(
        squish.waitForObjectExists(workstation_config_locators.deadline_config_dialog).visible,
        True,
        "Expect the Deadline Settings dialogue to be open.",
    )


def hit_apply_button():
    test.log("Hitting `Apply` button to apply selected settings.")
    # hit 'Apply' button
    squish.clickButton(
        squish.waitForObject(workstation_config_locators.deadlinedialog_apply_button)
    )
    test.log("Settings have been applied.")


def set_farm_name(farm_name: str):
    # open Default farm drop down menu
    squish.mouseClick(
        squish.waitForObject(workstation_config_locators.profilesettings_defaultfarm_dropdown)
    )
    test.log("Opened farm name drop down menu.")
    test.compare(
        squish.waitForObjectExists(workstation_config_locators.farm_name_locator(farm_name)).text,
        farm_name,
        "Expect farm name to be present in drop down.",
    )
    # select Default farm
    squish.mouseClick(
        squish.waitForObjectItem(
            workstation_config_locators.profilesettings_defaultfarm_dropdown, farm_name
        )
    )
    test.log("Selected farm name.")


def set_queue_name(queue_name: str):
    # open Default queue drop down menu
    squish.mouseClick(
        squish.waitForObject(workstation_config_locators.farmsettings_defaultqueue_dropdown)
    )
    test.log("Opened queue name drop down menu.")
    test.compare(
        squish.waitForObjectExists(workstation_config_locators.queue_name_locator(queue_name)).text,
        queue_name,
        "Expect queue name to be present in drop down.",
    )
    # select Default queue
    squish.mouseClick(
        squish.waitForObjectItem(
            workstation_config_locators.farmsettings_defaultqueue_dropdown, queue_name
        )
    )
    test.log("Selected queue name.")


def set_and_verify_os_storage_profile(
    linux_storage_profile: str, windows_storage_profile: str, macos_storage_profile: str
):
    # open Default storage profile drop down menu
    squish.mouseClick(
        squish.waitForObject(
            workstation_config_locators.farmsettings_defaultstorageprofile_dropdown
        )
    )
    test.log("Opened storage profile drop down menu.")
    # select storage profile based on OS platform being tested
    if platform.system() == "Linux":
        test.log("Detected test running on Linux OS")
        test.compare(
            squish.waitForObjectExists(
                workstation_config_locators.storage_profile_locator(linux_storage_profile)
            ).text,
            linux_storage_profile,
            "Expect Linux Storage Profile to be present in drop down.",
        )
        # select Linux Storage Profile
        squish.mouseClick(
            squish.waitForObjectItem(
                workstation_config_locators.farmsettings_defaultstorageprofile_dropdown,
                linux_storage_profile,
            )
        )
        # verify correct storage profile name is set
        test.compare(
            str(
                squish.waitForObjectExists(
                    workstation_config_locators.farmsettings_defaultstorageprofile_dropdown
                ).currentText
            ),
            linux_storage_profile,
            "Expect selected storage profile to be set.",
        )
    elif platform.system() == "Windows":
        test.log("Detected test running on Windows OS")
        test.compare(
            squish.waitForObjectExists(
                workstation_config_locators.storage_profile_locator(windows_storage_profile)
            ).text,
            windows_storage_profile,
            "Expect Windows Storage Profile to be present in drop down.",
        )
        # select Windows Storage Profile
        squish.mouseClick(
            squish.waitForObjectItem(
                workstation_config_locators.farmsettings_defaultstorageprofile_dropdown,
                windows_storage_profile,
            )
        )
        # verify correct storage profile name is set
        test.compare(
            str(
                squish.waitForObjectExists(
                    workstation_config_locators.farmsettings_defaultstorageprofile_dropdown
                ).currentText
            ),
            windows_storage_profile,
            "Expect selected storage profile to be set.",
        )
    elif platform.system() == "Darwin":
        test.log("Detected test running on macOS")
        test.compare(
            squish.waitForObjectExists(
                workstation_config_locators.storage_profile_locator(macos_storage_profile)
            ).text,
            macos_storage_profile,
            "Expect macOS Storage Profile to be present in drop down.",
        )
        # select macOS Storage Profile
        squish.mouseClick(
            squish.waitForObjectItem(
                workstation_config_locators.farmsettings_defaultstorageprofile_dropdown,
                macos_storage_profile,
            )
        )
        # verify correct storage profile name is set
        test.compare(
            str(
                squish.waitForObjectExists(
                    workstation_config_locators.farmsettings_defaultstorageprofile_dropdown
                ).currentText
            ),
            macos_storage_profile,
            "Expect selected storage profile to be set.",
        )
    test.log("Selected storage profile based on OS platform being tested.")


def set_job_attachments_filesystem_options(job_attachments: str):
    # open Job attachments filesystem options drop down menu
    squish.mouseClick(
        squish.waitForObject(
            workstation_config_locators.farmsettings_jobattachmentsoptions_dropdown
        )
    )
    test.log("Opened job attachments filesystem options drop down menu.")
    # select Job attachments filesystem options
    squish.mouseClick(
        squish.waitForObjectItem(
            workstation_config_locators.farmsettings_jobattachmentsoptions_dropdown, job_attachments
        )
    )
    test.log("Selected job attachments filesystem option.")


def set_conflict_resolution_option(conflict_res_option: str):
    # open Conflict resolution option drop down menu
    squish.mouseClick(
        squish.waitForObject(workstation_config_locators.conflictresolution_option_dropdown),
    )
    test.log("Opened conflict resolution option drop down menu.")
    # select Conflict resolution option
    squish.mouseClick(
        squish.waitForObjectItem(
            workstation_config_locators.conflictresolution_option_dropdown, conflict_res_option
        ),
    )
    test.log("Selected conflict resolution option.")


def set_current_logging_level(logging_level: str):
    # open Current logging level drop down menu
    squish.mouseClick(
        squish.waitForObject(workstation_config_locators.currentlogging_level_dropdown),
    )
    test.log("Opened current logging level drop down menu.")
    # select Current logging level
    squish.mouseClick(
        squish.waitForObjectItem(
            workstation_config_locators.currentlogging_level_dropdown, logging_level
        ),
    )
    test.log("Selected current logging level.")
