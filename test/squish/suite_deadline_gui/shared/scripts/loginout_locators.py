# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# encoding: UTF-8

import workstation_config_locators

# authentication status widget (replaces the old AuthenticationStatusGroup widgets)
# The new DeadlineAuthenticationStatusWidget is a single widget that shows:
# - A status icon (green checkmark when authenticated)
# - The profile name
# - Action buttons (login, switch profile, etc.)
authentication_status_widget = {
    "type": "DeadlineAuthenticationStatusWidget",
    "unnamed": 1,
    "visible": 1,
    "window": workstation_config_locators.deadline_config_dialog,
}

# Login button - exists but hidden when user is authenticated
# Note: visible is set to 0 to find the button even when hidden
authentication_login_button_hidden = {
    "container": authentication_status_widget,
    "text": "Log in",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 0,
}

# Profile button - shows the profile name and has dropdown menu
authentication_profile_button = {
    "container": authentication_status_widget,
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
}

# refresh farms list error dialogue
refreshfarmslist_error_dialog = {
    "type": "QMessageBox",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Refresh Farms list",
}
refreshfarmslist_ok_button = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": refreshfarmslist_error_dialog,
}

# refresh queues list error dialogue
refreshqueueslist_error_dialog = {
    "type": "QMessageBox",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Refresh Queues list",
}
refreshqueueslist_ok_button = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": refreshqueueslist_error_dialog,
}

# refresh storage profiles list error dialogue
refreshstorageprofiles_error_dialog = {
    "type": "QMessageBox",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Refresh Storage profiles list",
}
refreshstorageprofiles_ok_button = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": refreshstorageprofiles_error_dialog,
}
