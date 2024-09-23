# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

profile_name = "(default)"
job_hist_dir = "~/.deadline/job_history/(default)"
farm_name = "Deadline Cloud Squish Farm"
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
