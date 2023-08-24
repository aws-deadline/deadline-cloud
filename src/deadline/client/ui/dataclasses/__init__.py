# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Contains dataclasses for holding UI parameter values, used by the widgets.
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class JobBundleSettings:  # pylint: disable=too-many-instance-attributes
    """
    Settings for the Job Bundle submitter dialog.
    """

    # Used in UI elements and when creating the job bundle directory
    submitter_name: str = field(default="JobBundle")

    # Shared settings
    name: str = field(default="Job Bundle")
    description: str = field(default="")
    initial_status: str = field(default="READY")
    failed_tasks_limit: int = field(default=100)
    task_retry_limit: int = field(default=5)
    priority: int = field(default=50)
    override_installation_requirements: bool = field(default=False)
    installation_requirements: str = field(default="")

    # Job Bundle settings
    input_job_bundle_dir: str = field(default="")
    parameter_values: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CliJobSettings:  # pylint: disable=too-many-instance-attributes
    """
    Settings for a CLI Job.
    """

    # Used in UI elements and when creating the job bundle directory
    submitter_name: str = field(default="CLI")

    # Shared settings
    name: str = field(default="CLI Job")
    description: str = field(default="")
    initial_status: str = field(default="READY")
    failed_tasks_limit: int = field(default=100)
    task_retry_limit: int = field(default=5)
    priority: int = field(default=50)
    override_installation_requirements: bool = field(default=False)
    installation_requirements: str = field(default="")

    # CLI job settings
    bash_script_contents: str = field(
        default="""#!/usr/bin/env bash
echo "Data Dir is {{Param.DataDir}}"
cd "{{Param.DataDir}}"

echo "The file contents attached to this job:"
ls

echo "Running index {{Task.Param.Index}}"
sleep 35

# Generate an output file for this task
echo "Content for generated file {{Task.Param.Index}}" > task_output_file_{{Task.Param.Index}}.txt
"""
    )
    use_array_parameter: bool = field(default=True)
    array_parameter_name: str = field(default="Index")
    array_parameter_values: str = field(default="1-5")
    data_dir: str = field(default=os.path.join("~", "CLIJobData"))
    file_format: str = field(default="YAML")
