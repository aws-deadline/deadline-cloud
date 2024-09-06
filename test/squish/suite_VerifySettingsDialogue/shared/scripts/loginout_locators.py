# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# encoding: UTF-8

import gui_locators

# authentication status
credential_source_auth_group = {
    "title": "Credential source",
    "type": "AuthenticationStatusGroup",
    "unnamed": 1,
    "visible": 1,
    "window": gui_locators.deadline_config_dialog,
}
authentication_status_auth_group = {
    "title": "Authentication status",
    "type": "AuthenticationStatusGroup",
    "unnamed": 1,
    "visible": 1,
    "window": gui_locators.deadline_config_dialog,
}
deadlinecloud_api_auth_group = {
    "title": "AWS Deadline Cloud API",
    "type": "AuthenticationStatusGroup",
    "unnamed": 1,
    "visible": 1,
    "window": gui_locators.deadline_config_dialog,
}
credential_source_hostprovided_label = {
    "container": credential_source_auth_group,
    "text": "<b style='color:green;'>HOST_PROVIDED</b>",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
credential_source_dcmlogin_label = {
    "container": credential_source_auth_group,
    "text": "<b style='color:green;'>DEADLINE_CLOUD_MONITOR_LOGIN</b>",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
authentication_status_authenticated_label = {
    "container": authentication_status_auth_group,
    "text": "<b style='color:green;'>AUTHENTICATED</b>",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
deadlinecloud_api_authorized_label = {
    "container": deadlinecloud_api_auth_group,
    "text": "<b style='color:green;'>AUTHORIZED</b>",
    "type": "QLabel",
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
