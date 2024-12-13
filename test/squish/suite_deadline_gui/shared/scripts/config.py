# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

# Deadline Workstation Configuration
profile_name = "(default)"
job_hist_dir = "~/.deadline/job_history/(default)"
farm_name = "Deadline Cloud Squish Farm"
farm_desc = "Squish Automation Test Framework"
queue_name = "Squish Automation Queue"
storage_profile = "Squish Storage Profile"
job_attachments = "COPIED"
tooltip_text_copied = (
    "When selected, the worker downloads all job attachments to disk before rendering begins."
)
tooltip_text_lightbulb = "This setting determines how job attachments are loaded on the worker instance. 'COPIED' may be faster if every task needs all attachments, while 'VIRTUAL' may perform better if tasks only require a subset of attachments."
conflict_res_option = "NOT\\_SELECTED"
conflict_res_option_expected_text = conflict_res_option.replace("\\_", "_")
logging_level = "WARNING"

# Deadline GUI Test Samples
# Simple UI with Job Attachments (simple_ui_with_ja) - Shared job settings
simple_ui_with_ja = "./deadline-cloud/test/squish/deadline_gui_test_samples/simple_ui_with_ja"
simple_ui_with_ja_name = "Simple UI with Job Attachments"

# Simple UI - No Job Attachments (simple_ui_no_ja)- Shared job settings
simple_ui_no_ja = "./deadline-cloud/test/squish/deadline_gui_test_samples/simple_ui_no_ja"
simple_ui_no_ja_name = "Simple UI - No Job Attachments"
