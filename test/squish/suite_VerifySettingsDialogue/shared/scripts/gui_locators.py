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
    "text": "OK",
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
profilesettings_defaultfarm_dropdown = {
    "container": deadlinedialog_profilesettings_box,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
job_hist_dir_input = {
    "container": deadlinedialog_profilesettings_box,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
# Deadline Cloud Squish Farm element
deadlinecloudsquish_defaultfarm_index = {
    "container": profilesettings_defaultfarm_dropdown,
    "text": "Deadline Cloud Squish Farm",
    "type": "QModelIndex",
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
farmsettings_defaultqueue_dropdown = {
    "container": deadlinedialog_farmsettings_box,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
farmsettings_defaultstorageprofile_dropdown = {
    "container": deadlinedialog_farmsettings_box,
    "occurrence": 2,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
farmsettings_jobattachmentsoptions_dropdown = {
    "container": deadlinedialog_farmsettings_box,
    "occurrence": 3,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
# Squish Automation Queue element
squishautomationqueue_defaultqueue_index = {
    "container": farmsettings_defaultqueue_dropdown,
    "text": "Squish Automation Queue",
    "type": "QModelIndex",
}
# Squish Storage Profile element
squishstorageprofile_defaultstorageprofile_index = {
    "container": farmsettings_defaultstorageprofile_dropdown,
    "text": "Squish Storage Profile",
    "type": "QModelIndex",
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


def farm_name_locator(farm_name):
    return {
        "container": profilesettings_defaultfarm_dropdown,
        "text": farm_name,
        "type": "QModelIndex",
    }


def queue_name_locator(queue_name):
    return {
        "container": farmsettings_defaultqueue_dropdown,
        "text": queue_name,
        "type": "QModelIndex",
    }


def storage_profile_locator(storage_profile):
    return {
        "container": farmsettings_defaultstorageprofile_dropdown,
        "text": storage_profile,
        "type": "QModelIndex",
    }
