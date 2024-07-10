# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# encoding: UTF-8

# workstation configuration dialogue
aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog = {
    "type": "DeadlineConfigDialog",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "AWS Deadline Cloud workstation configuration",
}
# OK button
aWS_Deadline_Cloud_workstation_configuration_OK_QPushButton = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
# Apply button
aWS_Deadline_Cloud_workstation_configuration_Apply_QPushButton = {
    "text": "Apply",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}

# global settings
aWS_Deadline_Cloud_workstation_configuration_Global_settings_QGroupBox = {
    "title": "Global settings",
    "type": "QGroupBox",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
global_settings_AWS_profile_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Global_settings_QGroupBox,
    "text": "AWS profile",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
global_settings_AWS_profile_QComboBox = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Global_settings_QGroupBox,
    "leftWidget": global_settings_AWS_profile_QLabel,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
aWS_profile_deadlinecloud_squish_QModelIndex = {
    "container": global_settings_AWS_profile_QComboBox,
    "text": "deadlinecloud_squish",
    "type": "QModelIndex",
}

# profile settings
aWS_Deadline_Cloud_workstation_configuration_Profile_settings_QGroupBox = {
    "title": "Profile settings",
    "type": "QGroupBox",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
profile_settings_QComboBox = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Profile_settings_QGroupBox,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
profile_settings_QLineEdit = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Profile_settings_QGroupBox,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
deadline_Cloud_Squish_Farm_QModelIndex = {
    "container": profile_settings_QComboBox,
    "text": "Deadline Cloud Squish Farm",
    "type": "QModelIndex",
}

# choose job history directory file browser
profile_settings_QPushButton = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Profile_settings_QGroupBox,
    "text": "...",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
}
qFileDialog_QFileDialog = {"name": "QFileDialog", "type": "QFileDialog", "visible": 1}
profile_settings_Choose_QPushButton = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Profile_settings_QGroupBox,
    "text": "Choose",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
}

# farm settings
aWS_Deadline_Cloud_workstation_configuration_Farm_settings_QGroupBox = {
    "title": "Farm settings",
    "type": "QGroupBox",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
farm_settings_QComboBox = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Farm_settings_QGroupBox,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
farm_settings_QComboBox_2 = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Farm_settings_QGroupBox,
    "occurrence": 2,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
farm_settings_QComboBox_3 = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Farm_settings_QGroupBox,
    "occurrence": 3,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
squish_Automation_Queue_QModelIndex = {
    "container": farm_settings_QComboBox,
    "text": "Squish Automation Queue",
    "type": "QModelIndex",
}
squish_Storage_Profile_QModelIndex = {
    "container": farm_settings_QComboBox_2,
    "text": "Squish Storage Profile",
    "type": "QModelIndex",
}

# general settings
aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox = {
    "title": "General settings",
    "type": "QGroupBox",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
general_settings_Auto_accept_prompt_defaults_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox,
    "text": "Auto accept prompt defaults",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
general_settings_Auto_accept_prompt_defaults_QCheckBox = {
    "container": aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox,
    "leftWidget": general_settings_Auto_accept_prompt_defaults_QLabel,
    "type": "QCheckBox",
    "unnamed": 1,
    "visible": 1,
}
general_settings_Telemetry_opt_out_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox,
    "text": "Telemetry opt out",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
general_settings_Telemetry_opt_out_QCheckBox = {
    "container": aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox,
    "leftWidget": general_settings_Telemetry_opt_out_QLabel,
    "type": "QCheckBox",
    "unnamed": 1,
    "visible": 1,
}
general_settings_Conflict_resolution_option_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox,
    "text": "Conflict resolution option",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
general_settings_Conflict_resolution_option_QComboBox = {
    "container": aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox,
    "leftWidget": general_settings_Conflict_resolution_option_QLabel,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
general_settings_Current_logging_level_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox,
    "text": "Current logging level",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
general_settings_Current_logging_level_QComboBox = {
    "container": aWS_Deadline_Cloud_workstation_configuration_General_settings_QGroupBox,
    "leftWidget": general_settings_Current_logging_level_QLabel,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
