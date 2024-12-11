# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-

# choose job bundle directory
choose_job_bundle_dir = {"name": "QFileDialog", "type": "QFileDialog", "visible": 1}
directory_text_label = {
    "name": "fileNameLabel",
    "type": "QLabel",
    "visible": 1,
    "window": choose_job_bundle_dir,
}
jobbundle_filepath_input = {
    "buddy": directory_text_label,
    "name": "fileNameEdit",
    "type": "QLineEdit",
    "visible": 1,
}
choose_jobbundledir_button = {
    "text": "Choose",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": choose_job_bundle_dir,
}
jobbundledir_cancel_button = {
    "text": "Cancel",
    "type": "QPushButton",
    "unnamed": 1,
    "visible": 1,
    "window": choose_job_bundle_dir,
}
