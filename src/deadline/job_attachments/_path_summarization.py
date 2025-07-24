# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

# Re-export path summarization functions from common module for backward compatibility
from ..common.path_utils import (  # noqa: F401
    human_readable_file_size,
    summarize_paths_by_nested_directory,
    summarize_paths_by_sequence,
    summarize_path_list,
    PathSummary,
    _int_set_to_range_expr,
    _NumberedPath,
    _divide_numbered_path_group,
    _collapse_each_path_summary,
)
