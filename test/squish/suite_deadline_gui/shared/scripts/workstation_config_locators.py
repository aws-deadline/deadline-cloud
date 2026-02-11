# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# AWS Deadline Cloud workstation configuration dialogue
deadline_config_dialog = {
    "type": "DeadlineConfigDialog",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "AWS Deadline Cloud workstation configuration",
}
# OK button
deadlinedialog_ok_button = {
    "text": "Ok",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": deadline_config_dialog,
}
# Apply button
deadlinedialog_apply_button = {
    "text": "Apply",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": deadline_config_dialog,
}

# global settings box
deadlinedialog_globalsettings_box = {
    "title": "Global settings",
    "type": "QGroupBox",
    "unnamed": 1,
    "visible": 1,
    "window": deadline_config_dialog,
}
globalsettings_awsprofile_label = {
    "container": deadlinedialog_globalsettings_box,
    "text": "AWS profile",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
globalsettings_awsprofile_dropdown = {
    "container": deadlinedialog_globalsettings_box,
    "leftWidget": globalsettings_awsprofile_label,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
# `(default)` aws profile element
default_awsprofile_index = {
    "container": globalsettings_awsprofile_dropdown,
    "text": "(default)",
    "type": "QModelIndex",
}

# profile settings box
deadlinedialog_profilesettings_box = {
    "title": "Profile settings",
    "type": "QGroupBox",
    "unnamed": 1,
    "visible": 1,
    "window": deadline_config_dialog,
}
profilesettings_jobhistdir_label = {
    "container": deadlinedialog_profilesettings_box,
    "text": "Job history directory",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
job_hist_dir_input = {
    "container": deadlinedialog_profilesettings_box,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}

# choose job history directory file browser
open_job_hist_dir_button = {
    "container": deadlinedialog_profilesettings_box,
    "text": "...",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
}
choosejobhistdir_filebrowser = {"name": "QFileDialog", "type": "QFileDialog", "visible": 1}
choosejobhistdir_choose_button = {
    "container": deadlinedialog_profilesettings_box,
    "text": "Choose",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
}

# farm settings box
deadlinedialog_farmsettings_box = {
    "title": "Farm settings",
    "type": "QGroupBox",
    "unnamed": 1,
    "visible": 1,
    "window": deadline_config_dialog,
}
farmsettings_jobattachmentsoptions_dropdown = {
    "container": deadlinedialog_farmsettings_box,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
jobattachments_filesystemoptions_text_label = {
    "container": deadlinedialog_farmsettings_box,
    "text": "Job attachments filesystem options",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
jobattachments_filesystemoptions_lightbulb_icon = {
    "container": deadlinedialog_farmsettings_box,
    "leftWidget": jobattachments_filesystemoptions_text_label,
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}


# general settings box
deadlinedialog_generalsettings_box = {
    "title": "General settings",
    "type": "QGroupBox",
    "unnamed": 1,
    "visible": 1,
    "window": deadline_config_dialog,
}
autoaccept_promptdefaults_text_label = {
    "container": deadlinedialog_generalsettings_box,
    "text": "Auto accept prompt defaults",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
autoaccept_promptdefaults_checkbox = {
    "container": deadlinedialog_generalsettings_box,
    "leftWidget": autoaccept_promptdefaults_text_label,
    "type": "QCheckBox",
    "unnamed": 1,
    "visible": 1,
}
telemetry_optout_textlabel = {
    "container": deadlinedialog_generalsettings_box,
    "text": "Telemetry opt out",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
telemetry_optout_checkbox = {
    "container": deadlinedialog_generalsettings_box,
    "leftWidget": telemetry_optout_textlabel,
    "type": "QCheckBox",
    "unnamed": 1,
    "visible": 1,
}
conflictresolution_option_text_label = {
    "container": deadlinedialog_generalsettings_box,
    "text": "Conflict resolution option",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
conflictresolution_option_dropdown = {
    "container": deadlinedialog_generalsettings_box,
    "leftWidget": conflictresolution_option_text_label,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
currentlogging_level_text_label = {
    "container": deadlinedialog_generalsettings_box,
    "text": "Current logging level",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
currentlogging_level_dropdown = {
    "container": deadlinedialog_generalsettings_box,
    "leftWidget": currentlogging_level_text_label,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}


def profile_name_locator(profile_name):
    return {
        "container": globalsettings_awsprofile_dropdown,
        "text": profile_name,
        "type": "QModelIndex",
    }
