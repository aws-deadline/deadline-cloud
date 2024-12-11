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

# GUI Submitter Job Bundle One
# shared job settings
jobbundle_one = "./deadline-cloud/test/squish/deadline_gui_test_samples/job_bundle_one"
jobbundle_one_name = "GUI Submitter Testing - Job Bundle One"

# GUI Submitter Job Bundle Two
# shared job settings
jobbundle_two = "./deadline-cloud/test/squish/deadline_gui_test_samples/job_bundle_two"
jobbundle_two_name = "GUI Submitter Testing - Job Bundle Two"
