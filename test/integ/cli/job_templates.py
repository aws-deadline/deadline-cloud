# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Job template utilities for incremental download tests.
"""

from pathlib import Path
from typing import Optional

from deadline.client.api._submit_job_bundle import create_job_from_job_bundle
from deadline.client.config import config_file


def get_job_bundle_path(name: str) -> str:
    """
    Get the path to a job bundle directory.

    Args:
        name: Name of the bundle (e.g., 'make_many_small_files')

    Returns:
        Path to the job bundle directory
    """
    current_dir = Path(__file__).parent
    bundle_path = current_dir / "job_bundles" / name

    if not bundle_path.exists():
        raise FileNotFoundError(f"Job bundle not found: {bundle_path}")

    return str(bundle_path)


def submit_job_bundle(
    farm_id: str, queue_id: str, template_name: str, parameters: Optional[dict] = None
) -> str:
    """
    Submit a job using a local job bundle template.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use
        template_name: Name of the template directory
        parameters: Optional parameters to pass to the job

    Returns:
        The job ID of the submitted job
    """
    bundle_path = get_job_bundle_path(template_name)

    # Convert parameters to the format expected by create_job_from_job_bundle
    job_parameters = []
    if parameters:
        job_parameters = [{"name": key, "value": value} for key, value in parameters.items()]

    # Set farm and queue in config
    config = config_file.read_config()
    config_file.set_setting("defaults.farm_id", farm_id, config)
    config_file.set_setting("defaults.queue_id", queue_id, config)

    return create_job_from_job_bundle(
        job_bundle_dir=bundle_path, job_parameters=job_parameters, config=config
    )


def submit_make_many_small_files_job(
    farm_id: str,
    queue_id: str,
    files_per_task: int = 100,
    task_count: int = 100,
    output_dir: str = "output",
) -> str:
    """
    Submit a job that creates many small files.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use
        files_per_task: Number of files to create per task
        task_count: Number of tasks to run
        output_dir: The output directory to use (defaults to "output")

    Returns:
        The job ID of the submitted job
    """
    parameters = {"FilesPerTask": files_per_task, "Tasks": f"1-{task_count}", "DataDir": output_dir}

    return submit_job_bundle(farm_id, queue_id, "make_many_small_files", parameters)


def submit_dep_data_flow_job(
    farm_id: str, queue_id: str, data_dir: Optional[str] = None, input_dir: Optional[str] = None
) -> str:
    """
    Submit a job with branching and merging step dependencies.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use
        data_dir: The data directory to use (optional, defaults to ./data_dir)
        input_dir: The input directory to use (optional, defaults to ./input_dir)

    Returns:
        The job ID of the submitted job
    """
    parameters = {"JobName": "Step-Step Dependency Test", "Frames": "8-11"}

    # Override DataDir if provided, otherwise use default ./data_dir from template
    if data_dir:
        parameters["DataDir"] = data_dir

    # Override InputDir if provided, otherwise use default ./input_dir from template
    if input_dir:
        parameters["InputDir"] = input_dir

    return submit_job_bundle(farm_id, queue_id, "dep_data_flow", parameters)


def submit_dep_chain_job(farm_id: str, queue_id: str, output_dir: str = "output") -> str:
    """
    Submit a job with a chain of step dependencies.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use
        output_dir: The output directory to use

    Returns:
        The job ID of the submitted job
        :param output_dir:
    """
    parameters = {
        "JobName": "Step-Step Chain JA Output Test",
        "OutputPath": output_dir,
        # JobScriptDir uses default "scripts" from template
    }

    return submit_job_bundle(farm_id, queue_id, "dep_chain", parameters)
