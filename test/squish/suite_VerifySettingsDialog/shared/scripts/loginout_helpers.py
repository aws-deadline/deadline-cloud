# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*
# mypy: disable-error-code="attr-defined"

import loginout_locators
import squish
import test


def checkAndCloseRefreshErrorDialogues():
    # when launching deadline config gui/settings dialogue, up to three refresh error dialogues can appear, and they appear in any order.
    # these dialogues must be closed prior to being able to select the correct profile for authentication, or logging in.
    # note that these dialogues only appear when a DCM-created profile is preset (vs aws profile).
    dialogue_names = {
        "Farms Refresh Error": (
            loginout_locators.refresh_Farms_list_QMessageBox,
            loginout_locators.refresh_Farms_list_OK_QPushButton,
        ),
        "Queues Refresh Error": (
            loginout_locators.refresh_Queues_list_QMessageBox,
            loginout_locators.refresh_Queues_list_OK_QPushButton,
        ),
        "Storage Profiles Refresh Error": (
            loginout_locators.refresh_Storage_profiles_list_QMessageBox,
            loginout_locators.refresh_Storage_profiles_list_OK_QPushButton,
        ),
    }
    timeout = 300  # milliseconds

    for _ in range(3):  # three attempts are needed as the three dialogues may or may not appear
        for name, (errorDialogue, okButton) in dialogue_names.items():
            if errorDialogue:  # only check if the dialogue hasn't been handled yet
                try:
                    squish.waitForObject(errorDialogue, timeout)
                    squish.mouseClick(okButton)
                    test.log(f"{name} appeared and was closed.")
                    dialogue_names[name] = (None, None)  # mark as handled by setting to None
                except LookupError:
                    test.log(f"{name} was not present.")
        if all(d[0] is None for d in dialogue_names.values()):
            break
