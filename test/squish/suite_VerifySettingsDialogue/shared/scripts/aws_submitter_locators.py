# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-

aws_submitter_dialogue = {
    "type": "SubmitJobToDeadlineDialog",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Submit to AWS Deadline Cloud",
}
settings_button = {
    "text": "Settings...",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": aws_submitter_dialogue,
}
shared_jobsettings_tab = {
    "type": "QTabWidget",
    "unnamed": 1,
    "visible": 1,
    "window": aws_submitter_dialogue,
}
job_specificsettings_tab = {
    "type": "QTabWidget",
    "unnamed": 1,
    "visible": 1,
    "window": aws_submitter_dialogue,
}
submit_to_AWS_Deadline_Cloud_qt_tabwidget_stackedwidget_QStackedWidget = {
    "name": "qt_tabwidget_stackedwidget",
    "type": "QStackedWidget",
    "visible": 1,
    "window": aws_submitter_dialogue,
}
qt_tabwidget_stackedwidget_QScrollArea = {
    "container": submit_to_AWS_Deadline_Cloud_qt_tabwidget_stackedwidget_QStackedWidget,
    "type": "QScrollArea",
    "unnamed": 1,
    "visible": 1,
}
shared_jobsettings_properties = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "type": "SharedJobSettingsWidget",
    "unnamed": 1,
    "visible": 1,
}
job_specificsettings_properties = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "type": "JobBundleSettingsWidget",
    "unnamed": 1,
    "visible": 1,
}
job_specificsettings_properties_box = {
    "container": qt_tabwidget_stackedwidget_QScrollArea,
    "title": "Job Properties",
    "type": "SharedJobPropertiesWidget",
    "unnamed": 1,
    "visible": 1,
}
job_properties_name_label = {
    "container": job_specificsettings_properties_box,
    "text": "Name",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
job_properties_name_input = {
    "buddy": job_properties_name_label,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}

job_properties_desc_label = {
    "container": job_specificsettings_properties_box,
    "text": "Description",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
job_properties_desc_input = {
    "aboveWidget": job_properties_name_input,
    "container": job_specificsettings_properties_box,
    "leftWidget": job_properties_desc_label,
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
condapackages_text_label = {
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
queue_Environment_Conda_Conda_Packages_QLineEdit = {
    "container": queue_Environment_Conda_JobTemplateGroupLayout,
    "leftWidget": condapackages_text_label,
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
# job-specific settings tab
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
