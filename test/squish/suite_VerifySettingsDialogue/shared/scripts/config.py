# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# mypy: disable-error-code="attr-defined"

# deadline config gui workstation config
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

# AWS Submitter dialogue job properties
empty_desc = ""
tooltip_text_conda_packages = "This is a space-separated list of Conda package match specifications to install for the job. E.g. \"blender=3.6\" for a job that renders frames in Blender 3.6.\nSee https://docs.conda.io/projects/conda/en/latest/user-guide/concepts/pkg-specs.html#package-match-specifications\n"
tooltip_text_conda_channels = "This is a space-separated list of Conda channels from which to install packages. Deadline Cloud SMF packages are installed from the \"deadline-cloud\" channel that is configured by Deadline Cloud.\nAdd \"conda-forge\" to get packages from the https://conda-forge.org/ community, and \"defaults\" to get packages from Anaconda Inc (make sure your usage complies with https://www.anaconda.com/terms-of-use).\n"

# blender job bundle directory
blender_filepath = "/home/rocky/deadline-cloud-fork/deadline-cloud/test/squish/deadline_cloud_samples/blender_render"
blender_job_name = "Blender Render"

# maya job bundle directory
maya_filepath = "/home/rocky/deadline-cloud-fork/deadline-cloud/test/squish/deadline_cloud_samples/turntable_with_maya_arnold"
maya_job_bundle_folder = "turntable_with_maya_arnold"
