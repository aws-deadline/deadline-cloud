# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

import gui_locators
import squish

import test

snooze_timeout = 1  # seconds


def launch_deadline_config_gui():
    squish.startApplication("deadline config gui")
    test.log("Launched Deadline Config GUI.")
    test.log(
        "Sleep for " + str(snooze_timeout) + " second(s) to allow GUI authentication to fully load."
    )
    squish.snooze(snooze_timeout)
    test.compare(
        squish.waitForObjectExists(gui_locators.deadline_config_dialog).visible,
        True,
        "Expect the Deadline Config GUI to be open.",
    )


def close_deadline_config_gui():
    test.log("Hitting `OK` button to close Deadline Config GUI.")
    # hit 'OK' button to close Deadline Config GUI
    squish.clickButton(squish.waitForObject(gui_locators.deadlinedialog_ok_button))


def hit_apply_button():
    test.log("Hitting `Apply` button to apply selected settings.")
    # hit 'Apply' button
    squish.clickButton(squish.waitForObject(gui_locators.deadlinedialog_apply_button))
    test.log("Settings have been applied.")


def set_farm_name(farm_name: str):
    # open Default farm drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.profilesettings_defaultfarm_dropdown))
    test.log("Opened farm name drop down menu.")
    test.compare(
        squish.waitForObjectExists(gui_locators.farm_name_locator(farm_name)).text,
        farm_name,
        "Expect farm name to be present in drop down.",
    )
    # select Default farm
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.profilesettings_defaultfarm_dropdown, farm_name)
    )
    test.log("Selected farm name.")


def open_close_job_hist_directory():
    # hit '...' button to open Choose Job history directory file browser
    squish.clickButton(squish.waitForObject(gui_locators.open_job_hist_dir_button))
    test.log("Opened job history directory dialogue.")
    # verify job history directory dialogue is open
    test.compare(
        str(squish.waitForObjectExists(gui_locators.choosejobhistdir_filebrowser).windowTitle),
        "Choose Job history directory",
        "Expect Choose Job history directory dialogue window title to be present.",
    )
    test.compare(
        squish.waitForObjectExists(gui_locators.choosejobhistdir_filebrowser).visible,
        True,
        "Expect Choose Job history directory dialogue to be open.",
    )
    # hit 'choose' button to set default and close file browser
    squish.clickButton(squish.waitForObject(gui_locators.choosejobhistdir_choose_button))
    test.log("Closed job history directory dialogue.")


def set_queue_name(queue_name: str):
    # open Default queue drop down menu
    squish.mouseClick(squish.waitForObject(gui_locators.farmsettings_defaultqueue_dropdown))
    test.log("Opened queue name drop down menu.")
    test.compare(
        squish.waitForObjectExists(gui_locators.queue_name_locator(queue_name)).text,
        queue_name,
        "Expect queue name to be present in drop down.",
    )
    # select Default queue
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.farmsettings_defaultqueue_dropdown, queue_name)
    )
    test.log("Selected queue name.")


def set_storage_profile(storage_profile: str):
    # open Default storage profile drop down menu
    squish.mouseClick(
        squish.waitForObject(gui_locators.farmsettings_defaultstorageprofile_dropdown)
    )
    test.log("Opened storage profile drop down menu.")
    test.compare(
        squish.waitForObjectExists(gui_locators.storage_profile_locator(storage_profile)).text,
        storage_profile,
        "Expect storage profile to be present in drop down.",
    )
    # select Default storage profile
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.farmsettings_defaultstorageprofile_dropdown, storage_profile
        )
    )
    test.log("Selected storage profile.")


def set_job_attachments_filesystem_options(job_attachments: str):
    # open Job attachments filesystem options drop down menu
    squish.mouseClick(
        squish.waitForObject(gui_locators.farmsettings_jobattachmentsoptions_dropdown)
    )
    test.log("Opened job attachments filesystem options drop down menu.")
    # select Job attachments filesystem options
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.farmsettings_jobattachmentsoptions_dropdown, job_attachments
        )
    )
    test.log("Selected job attachments filesystem option.")


def set_conflict_resolution_option(conflict_res_option: str):
    # open Conflict resolution option drop down menu
    squish.mouseClick(
        squish.waitForObject(gui_locators.conflictresolution_option_dropdown),
    )
    test.log("Opened conflict resolution option drop down menu.")
    # select Conflict resolution option
    squish.mouseClick(
        squish.waitForObjectItem(
            gui_locators.conflictresolution_option_dropdown, conflict_res_option
        ),
    )
    test.log("Selected conflict resolution option.")


def set_current_logging_level(logging_level: str):
    # open Current logging level drop down menu
    squish.mouseClick(
        squish.waitForObject(gui_locators.currentlogging_level_dropdown),
    )
    test.log("Opened current logging level drop down menu.")
    # select Current logging level
    squish.mouseClick(
        squish.waitForObjectItem(gui_locators.currentlogging_level_dropdown, logging_level),
    )
    test.log("Selected current logging level.")
