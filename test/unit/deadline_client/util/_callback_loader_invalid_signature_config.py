# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from typing import Any, Optional

from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (  # type: ignore
    SubmitJobToDeadlineDialog,
    JobBundlePurpose,
)
from deadline.client.job_bundle.submission import AssetReferences


def on_pre_submit_callback(
    widget: SubmitJobToDeadlineDialog,
    job_bundle_dir: str,
    settings: int,
    asset_references: AssetReferences,
    host_requirements: Optional[dict[str, Any]] = None,
    purpose: JobBundlePurpose = JobBundlePurpose.SUBMISSION,
):
    return True


def on_post_submit_callback(
    widget: SubmitJobToDeadlineDialog,
    job_bundle_dir: str,
    settings: object,
    queue_parameters: list[dict[str, Any]],
    asset_references: AssetReferences,
):
    return True
