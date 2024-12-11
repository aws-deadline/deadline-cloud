# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# from objectmaphelper import *
refresh_Farms_list_QMessageBox = {
    "type": "QMessageBox",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Refresh Farms list",
}
refresh_Farms_list_OK_QPushButton = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": refresh_Farms_list_QMessageBox,
}
refresh_Queues_list_QMessageBox = {
    "type": "QMessageBox",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Refresh Queues list",
}
refresh_Queues_list_OK_QPushButton = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": refresh_Queues_list_QMessageBox,
}
refresh_Storage_profiles_list_QMessageBox = {
    "type": "QMessageBox",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Refresh Storage profiles list",
}
refresh_Storage_profiles_list_OK_QPushButton = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": refresh_Storage_profiles_list_QMessageBox,
}
aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog = {
    "type": "DeadlineConfigDialog",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "AWS Deadline Cloud workstation configuration",
}
aWS_Deadline_Cloud_workstation_configuration_Credential_source_AuthenticationStatusGroup = {
    "title": "Credential source",
    "type": "AuthenticationStatusGroup",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
aWS_Deadline_Cloud_workstation_configuration_Authentication_status_AuthenticationStatusGroup = {
    "title": "Authentication status",
    "type": "AuthenticationStatusGroup",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
aWS_Deadline_Cloud_workstation_configuration_AWS_Deadline_Cloud_API_AuthenticationStatusGroup = {
    "title": "AWS Deadline Cloud API",
    "type": "AuthenticationStatusGroup",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
credential_source_b_style_color_green_HOST_PROVIDED_b_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Credential_source_AuthenticationStatusGroup,
    "text": "<b style='color:green;'>HOST_PROVIDED</b>",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
credential_source_b_style_color_green_DEADLINE_CLOUD_MONITOR_LOGIN_b_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Credential_source_AuthenticationStatusGroup,
    "text": "<b style='color:green;'>DEADLINE_CLOUD_MONITOR_LOGIN</b>",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
authentication_status_b_style_color_green_AUTHENTICATED_b_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Authentication_status_AuthenticationStatusGroup,
    "text": "<b style='color:green;'>AUTHENTICATED</b>",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
aWS_Deadline_Cloud_API_b_style_color_green_AUTHORIZED_b_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_AWS_Deadline_Cloud_API_AuthenticationStatusGroup,
    "text": "<b style='color:green;'>AUTHORIZED</b>",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
aWS_Deadline_Cloud_workstation_configuration_OK_QPushButton = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
aWS_Deadline_Cloud_workstation_configuration_Apply_QPushButton = {
    "text": "Apply",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": aWS_Deadline_Cloud_workstation_configuration_DeadlineConfigDialog,
}
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
aWS_profile_default_QModelIndex = {
    "container": global_settings_AWS_profile_QComboBox,
    "text": "(default)",
    "type": "QModelIndex",
}
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
farm_settings_Job_attachments_filesystem_options_QLabel = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Farm_settings_QGroupBox,
    "text": "Job attachments filesystem options",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
farm_settings_Job_attachments_filesystem_options_QLabel_2 = {
    "container": aWS_Deadline_Cloud_workstation_configuration_Farm_settings_QGroupBox,
    "leftWidget": farm_settings_Job_attachments_filesystem_options_QLabel,
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
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
qFileDialog_fileNameLabel_QLabel = {
    "name": "fileNameLabel",
    "type": "QLabel",
    "visible": 1,
    "window": qFileDialog_QFileDialog,
}
fileNameEdit_QLineEdit = {
    "buddy": qFileDialog_fileNameLabel_QLabel,
    "name": "fileNameEdit",
    "type": "QLineEdit",
    "visible": 1,
}
qFileDialog_Choose_QPushButton = {
    "text": "Choose",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": qFileDialog_QFileDialog,
}
qFileDialog_Cancel_QPushButton = {
    "text": "Cancel",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": qFileDialog_QFileDialog,
}
error_running_deadline_bundle_gui_submit_browse_QMessageBox = {
    "type": "QMessageBox",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": 'Error running "deadline bundle gui-submit --browse"',
}
error_running_deadline_bundle_gui_submit_browse_OK_QPushButton = {
    "text": "OK",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": error_running_deadline_bundle_gui_submit_browse_QMessageBox,
}
submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog = {
    "type": "SubmitJobToDeadlineDialog",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Submit to AWS Deadline Cloud",
}
submit_to_AWS_Deadline_Cloud_Settings_QPushButton = {
    "text": "Settings...",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog,
}
submit_to_AWS_Deadline_Cloud_qt_tabwidget_stackedwidget_QStackedWidget = {
    "name": "qt_tabwidget_stackedwidget",
    "type": "QStackedWidget",
    "visible": 1,
    "window": submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog,
}
qt_tabwidget_stackedwidget_QScrollArea = {
    "container": submit_to_AWS_Deadline_Cloud_qt_tabwidget_stackedwidget_QStackedWidget,
    "type": "QScrollArea",
    "unnamed": 1,
    "visible": 1,
}
job_Properties_SharedJobPropertiesWidget = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "title": "Job Properties",
    "type": "SharedJobPropertiesWidget",
    "unnamed": 1,
    "visible": 1,
}
job_Properties_Name_QLabel = {
    "container": job_Properties_SharedJobPropertiesWidget,
    "text": "Name",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
name_QLineEdit = {
    "buddy": job_Properties_Name_QLabel,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
qFileDialog_splitter_QSplitter = {
    "name": "splitter",
    "type": "QSplitter",
    "visible": 1,
    "window": qFileDialog_QFileDialog,
}
splitter_frame_QFrame = {
    "container": qFileDialog_splitter_QSplitter,
    "name": "frame",
    "type": "QFrame",
    "visible": 1,
}
frame_stackedWidget_QStackedWidget = {
    "container": splitter_frame_QFrame,
    "name": "stackedWidget",
    "type": "QStackedWidget",
    "visible": 1,
}
stackedWidget_treeView_QTreeView = {
    "container": frame_stackedWidget_QStackedWidget,
    "name": "treeView",
    "type": "QTreeView",
    "visible": 1,
}
job_Properties_Description_QLabel = {
    "container": job_Properties_SharedJobPropertiesWidget,
    "text": "Description",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
job_Properties_Description_QLineEdit = {
    "aboveWidget": name_QLineEdit,
    "container": job_Properties_SharedJobPropertiesWidget,
    "leftWidget": job_Properties_Description_QLabel,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
deadline_Cloud_settings_DeadlineCloudSettingsWidget = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "title": "Deadline Cloud settings",
    "type": "DeadlineCloudSettingsWidget",
    "unnamed": 1,
    "visible": 1,
}
deadline_Cloud_settings_Deadline_Cloud_Squish_Farm_QLabel = {
    "container": deadline_Cloud_settings_DeadlineCloudSettingsWidget,
    "text": "Deadline Cloud Squish Farm",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
deadline_Cloud_settings_Squish_Automation_Queue_QLabel = {
    "container": deadline_Cloud_settings_DeadlineCloudSettingsWidget,
    "text": "Squish Automation Queue",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
queue_Environment_Conda_JobTemplateGroupLayout = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "name": "Queue Environment: Conda",
    "type": "_JobTemplateGroupLayout",
    "visible": 1,
}
queue_Environment_Conda_Conda_Packages_QLabel = {
    "container": queue_Environment_Conda_JobTemplateGroupLayout,
    "text": "Conda Packages",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
queue_Environment_Conda_Conda_Channels_QLabel = {
    "container": queue_Environment_Conda_JobTemplateGroupLayout,
    "text": "Conda Channels",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
o_SharedJobSettingsWidget = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "type": "SharedJobSettingsWidget",
    "unnamed": 1,
    "visible": 1,
}
submit_to_AWS_Deadline_Cloud_DeadlineAuthenticationStatusWidget = {
    "type": "DeadlineAuthenticationStatusWidget",
    "unnamed": 1,
    "visible": 1,
    "window": submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog,
}
queue_Environment_Conda_Conda_Packages_QLineEdit = {
    "container": queue_Environment_Conda_JobTemplateGroupLayout,
    "leftWidget": queue_Environment_Conda_Conda_Packages_QLabel,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
queue_Environment_Conda_Conda_Channels_QLineEdit = {
    "container": queue_Environment_Conda_JobTemplateGroupLayout,
    "leftWidget": queue_Environment_Conda_Conda_Channels_QLabel,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
submit_to_AWS_Deadline_Cloud_QTabWidget = {
    "type": "QTabWidget",
    "unnamed": 1,
    "visible": 1,
    "window": submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog,
}
software_Environment_JobTemplateGroupLayout = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "name": "Software Environment",
    "type": "_JobTemplateGroupLayout",
    "visible": 1,
}
software_Environment_Conda_Packages_QLabel = {
    "container": software_Environment_JobTemplateGroupLayout,
    "text": "Conda Packages",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
software_Environment_Conda_Packages_QLineEdit = {
    "container": software_Environment_JobTemplateGroupLayout,
    "leftWidget": software_Environment_Conda_Packages_QLabel,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
render_Parameters_JobTemplateGroupLayout = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "name": "Render Parameters",
    "type": "_JobTemplateGroupLayout",
    "visible": 1,
}
render_Parameters_QLineEdit = {
    "container": render_Parameters_JobTemplateGroupLayout,
    "occurrence": 3,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
render_Parameters_Output_File_Pattern_QLabel = {
    "container": render_Parameters_JobTemplateGroupLayout,
    "text": "Output File Pattern",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
render_Parameters_Output_File_Pattern_QLineEdit = {
    "container": render_Parameters_JobTemplateGroupLayout,
    "leftWidget": render_Parameters_Output_File_Pattern_QLabel,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
render_Parameters_Output_File_Format_QLabel = {
    "container": render_Parameters_JobTemplateGroupLayout,
    "text": "Output File Format",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
render_Parameters_Output_File_Format_QComboBox = {
    "container": render_Parameters_JobTemplateGroupLayout,
    "leftWidget": render_Parameters_Output_File_Format_QLabel,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
render_Parameters_Frames_QLabel = {
    "container": render_Parameters_JobTemplateGroupLayout,
    "text": "Frames",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
render_Parameters_Frames_QLineEdit = {
    "container": render_Parameters_JobTemplateGroupLayout,
    "leftWidget": render_Parameters_Frames_QLabel,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
o_JobBundleSettingsWidget = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "type": "JobBundleSettingsWidget",
    "unnamed": 1,
    "visible": 1,
}
load_a_different_job_bundle_QPushButton = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "text": "Load a different job bundle",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
}
lookInCombo_QComboBox = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "name": "lookInCombo",
    "type": "QComboBox",
    "visible": 1,
}
