# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-

aws_submitter_dialogue = {
    "type": "SubmitJobToDeadlineDialog",
    "unnamed": 1,
    "visible": 1,
    "windowTitle": "Submit to AWS Deadline Cloud",
}
# Settings button
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
tabs_and_properties_widget = {
    "name": "qt_tabwidget_stackedwidget",
    "type": "QStackedWidget",
    "visible": 1,
    "window": aws_submitter_dialogue,
}
# qt_tabwidget_stackedwidget_QScrollArea
properties_only_widget = {
    "container": tabs_and_properties_widget,
    "type": "QScrollArea",
    "unnamed": 1,
    "visible": 1,
}
# shared job settings tab
shared_jobsettings_jobproperties_widget = {
    "container": properties_only_widget,
    "type": "SharedJobSettingsWidget",
    "unnamed": 1,
    "visible": 1,
}
shared_jobsettings_properties_box = {
    "container": properties_only_widget,
    "title": "Job Properties",
    "type": "SharedJobPropertiesWidget",
    "unnamed": 1,
    "visible": 1,
}
job_properties_name_label = {
    "container": shared_jobsettings_properties_box,
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
    "container": shared_jobsettings_properties_box,
    "text": "Description",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
job_properties_desc_input = {
    "aboveWidget": job_properties_name_input,
    "container": shared_jobsettings_properties_box,
    "leftWidget": job_properties_desc_label,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
deadline_cloud_settings_widget = {
    "container": properties_only_widget,
    "title": "Deadline Cloud settings",
    "type": "DeadlineCloudSettingsWidget",
    "unnamed": 1,
    "visible": 1,
}
# Deadline Cloud Squish Farm text element
deadline_cloud_settings_farm_name = {
    "container": deadline_cloud_settings_widget,
    "text": "Deadline Cloud Squish Farm",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
# Squish Automation Queue text element
deadline_cloud_settings_queue_name = {
    "container": deadline_cloud_settings_widget,
    "text": "Squish Automation Queue",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
queue_environment_conda_widget = {
    "container": properties_only_widget,
    "name": "Queue Environment: Conda",
    "type": "_JobTemplateGroupLayout",
    "visible": 1,
}
conda_packages_text_label = {
    "container": queue_environment_conda_widget,
    "text": "Conda Packages",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
conda_packages_text_input = {
    "container": queue_environment_conda_widget,
    "leftWidget": conda_packages_text_label,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
conda_channels_text_label = {
    "container": queue_environment_conda_widget,
    "text": "Conda Channels",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
conda_channels_text_input = {
    "container": queue_environment_conda_widget,
    "leftWidget": conda_channels_text_label,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
# job-specific settings tab
job_specificsettings_properties = {
    "container": properties_only_widget,
    "type": "JobBundleSettingsWidget",
    "unnamed": 1,
    "visible": 1,
}
render_parameters_widget = {
    "container": properties_only_widget,
    "name": "Render Parameters",
    "type": "_JobTemplateGroupLayout",
    "visible": 1,
}
render_parameters_frames_label = {
    "container": render_parameters_widget,
    "text": "Frames",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
render_parameters_frames_text_input = {
    "container": render_parameters_widget,
    "leftWidget": render_parameters_frames_label,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
render_parameters_output_dir_filepath = {
    "container": render_parameters_widget,
    "occurrence": 3,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
render_parameters_output_file_pattern_label = {
    "container": render_parameters_widget,
    "text": "Output File Pattern",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
render_parameters_output_file_pattern_text_input = {
    "container": render_parameters_widget,
    "leftWidget": render_parameters_output_file_pattern_label,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
render_parameters_output_file_format_label = {
    "container": render_parameters_widget,
    "text": "Output File Format",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
render_parameters_output_file_format_dropdown = {
    "container": render_parameters_widget,
    "leftWidget": render_parameters_output_file_format_label,
    "type": "QComboBox",
    "unnamed": 1,
    "visible": 1,
}
software_environment_widget = {
    "container": properties_only_widget,
    "name": "Software Environment",
    "type": "_JobTemplateGroupLayout",
    "visible": 1,
}
software_environment_condapackages_label = {
    "container": software_environment_widget,
    "text": "Conda Packages",
    "type": "QLabel",
    "unnamed": 1,
    "visible": 1,
}
software_environment_condapackages_text_input = {
    "container": software_environment_widget,
    "leftWidget": software_environment_condapackages_label,
    "type": "QLineEdit",
    "unnamed": 1,
    "visible": 1,
}
# load different job bundle button in AWS Submitter dialogue (job-specific settings tab)
load_different_job_bundle_button = {
    "container": properties_only_widget,
    "text": "Load a different job bundle",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
}
# job history directory default filepath input
job_hist_dir_dropdown = {
    "container": properties_only_widget,
    "name": "lookInCombo",
    "type": "QComboBox",
    "visible": 1,
}


def deadlinecloud_farmname_locator(farm_name):
    return {
        "container": deadline_cloud_settings_widget,
        "text": farm_name,
        "type": "QLabel",
        "unnamed": 1,
        "visible": 1,
    }


def deadlinecloud_queuename_locator(queue_name):
    return {
        "container": deadline_cloud_settings_widget,
        "text": queue_name,
        "type": "QLabel",
        "unnamed": 1,
        "visible": 1,
    }
