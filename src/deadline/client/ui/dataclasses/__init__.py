# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Contains dataclasses for holding UI parameter values, used by the widgets.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from ...job_bundle.parameters import JobParameter


@dataclass
class JobBundleSettings:  # pylint: disable=too-many-instance-attributes
    """
    Settings for the Job Bundle submitter dialog.
    """

    # Used in UI elements and when creating the job bundle directory
    submitter_name: str = field(default="JobBundle")

    # Shared settings
    name: str = field(default="Job bundle")
    description: str = field(default="")

    # Job Bundle settings
    input_job_bundle_dir: str = field(default="")
    parameters: list[JobParameter] = field(default_factory=list)

    # Whether to allow ability to "Load a different job bundle"
    browse_enabled: bool = field(default=False)


@dataclass
class CliJobSettings:  # pylint: disable=too-many-instance-attributes
    """
    Settings for a CLI Job.
    """

    # Used in UI elements and when creating the job bundle directory
    submitter_name: str = field(default="CLI")

    # Shared settings
    name: str = field(default="CLI job")
    description: str = field(default="")

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
