# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

import loginout_locators
import workstation_config_locators
import squish
import test


def check_and_close_refresh_error_dialogues():
    """
    When launching deadline config gui/settings dialogue, up to three refresh error dialogues can appear, and they appear in any order.
    These dialogues must be closed prior to being able to select the correct profile for authentication, or logging in.
    A scenario in which these dialogues appear is when a DCM-created profile is pre-selected (vs an aws profile) and the user has not logged in.
    """
    dialogue_names = {
        "Farms Refresh Error": (
            loginout_locators.refreshfarmslist_error_dialog,
            loginout_locators.refreshfarmslist_ok_button,
        ),
        "Queues Refresh Error": (
            loginout_locators.refreshqueueslist_error_dialog,
            loginout_locators.refreshqueueslist_ok_button,
        ),
        "Storage Profiles Refresh Error": (
            loginout_locators.refreshstorageprofiles_error_dialog,
            loginout_locators.refreshstorageprofiles_ok_button,
        ),
    }
    timeout = 300  # milliseconds

    for _ in range(3):  # three attempts are needed as the three dialogues may or may not appear
        for name, (error_dialogue, ok_button) in dialogue_names.items():
            if error_dialogue:  # only check if the dialogue hasn't been handled yet
                try:
                    squish.waitForObject(error_dialogue, timeout)
                    squish.mouseClick(ok_button)
                    test.log(f"{name} appeared and was closed.")
                    dialogue_names[name] = (None, None)  # mark as handled by setting to None
                except LookupError:
                    test.log(f"{name} was not present.")
        if all(d[0] is None for d in dialogue_names.values()):
            break


def set_aws_profile_name_and_verify_auth(profile_name: str):
    # open AWS profile drop down menu
    squish.mouseClick(
        squish.waitForObjectExists(workstation_config_locators.globalsettings_awsprofile_dropdown),
    )
    test.log("Opened AWS profile drop down menu.")
    test.compare(
        squish.waitForObjectExists(
            workstation_config_locators.profile_name_locator(profile_name)
        ).text,
        profile_name,
        "Expect AWS profile name to be present in drop down.",
    )
    # select AWS profile
    squish.mouseClick(workstation_config_locators.profile_name_locator(profile_name))
    test.log("Selected AWS profile name.")

    # verify user is authenticated using the new DeadlineAuthenticationStatusWidget
    # The widget shows authentication status via:
    # 1. The widget being visible
    # 2. The login button being hidden (not visible when authenticated)
    # 3. The profile name displayed on the profile button
    test.log("Verifying user is authenticated...")

    # Wait for the authentication status widget to be visible
    test.compare(
        squish.waitForObjectExists(loginout_locators.authentication_status_widget).visible,
        True,
        "Expect authentication status widget to be visible.",
    )

    # When authenticated, the login button should be hidden (visible=False)
    # Use the hidden locator to find the button even when not visible
    login_button = squish.waitForObjectExists(loginout_locators.authentication_login_button_hidden)
    test.compare(
        login_button.visible,
        False,
        "Expect login button to be hidden when authenticated.",
    )

    # Verify the profile button shows the expected profile name
    profile_button = squish.waitForObjectExists(loginout_locators.authentication_profile_button)
    test.verify(
        profile_name in str(profile_button.text),
        f"Expect profile button to contain profile name '{profile_name}'.",
    )
