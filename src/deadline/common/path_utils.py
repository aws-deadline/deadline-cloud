# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from ..job_attachments._path_summarization import (
    human_readable_file_size,
    summarize_paths_by_nested_directory,
    summarize_paths_by_sequence,
    summarize_path_list,
    PathSummary,
)

import warnings

warnings.warn(
    "The deadline.common module is deprecated. Please use deadline.job_attachments.api instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "human_readable_file_size",
    "summarize_paths_by_nested_directory",
    "summarize_paths_by_sequence",
    "summarize_path_list",
    "PathSummary",
]
