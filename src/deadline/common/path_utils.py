# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations
import os
import re
from typing import Optional, Iterable, Union
from collections.abc import Collection
from pathlib import Path


def human_readable_file_size(size_in_bytes: int) -> str:
    """
    Convert a size in bytes to something human readable. For example 1000 bytes will be converted
    to 1 KB. Sizes close enough to a postfix threshold will be rounded up to the next threshold.
    For example 999999 bytes would be output as 1.0 MB and NOT 999.99 KB (or as a consequence of
    Python's round function 1000.0 KB).

    This function is for display purposes only.
    """
    converted_size: Union[int, float] = size_in_bytes
    rounded: Union[int, float]
    postfixes = ["B", "KB", "MB", "GB", "TB", "PB"]

    for postfix in postfixes:
        rounded = round(converted_size, ndigits=2)

        if rounded < 1000:
            return f"{rounded} {postfix}"

        converted_size /= 1000

    # If we go higher than the provided postfix,
    # then return as a large amount of the highest postfix we've specified.
    return f"{rounded} {postfixes[-1]}"


_NUMBERED_PATH_REGEX = re.compile(r"^(.*\D|)(\d+)(\.[^/\\]+)?$")


class _NumberedPath:
    """
    Representation of a file system path that may be numbered like frame_001.png.

    Some example properties for paths:
      frame_001.png: grouping='frame_#.png', padding_min=3, padding_max=3, number=1
      sequence_v907: grouping='sequence_v#', padding_min=1, padding_max=3, number=907
    """

    path: str
    """The path that may or may not be numbered"""
    parts: Optional[list[str]] = None
    """The path split into parts"""
    grouping: str
    """The path with the number as '#' for grouping purposes"""
    padding_min: int
    """The minimum padding or -1. e.g. '%04d' for padding of 4, that produces the number in the path"""
    padding_max: int
    """The maximum padding or -1. e.g. '%04d' for padding of 4, that produces the number in the path"""
    number: Optional[int] = None
    """The number in the path, or None if the path is not numbered"""

    def __init__(self, path: Union[Path, str]):
        if isinstance(path, Path):
            path = str(path)
        m = _NUMBERED_PATH_REGEX.match(path)
        if m:
            self.path = path
            self.parts = [m.group(1), m.group(2), m.group(3) or ""]
            self.grouping = f"{self.parts[0]}#.{self.parts[2]}"
            number = self.parts[1]
            if number[0] == "0":
                self.padding_min = len(number)
            else:
                self.padding_min = 1
            self.padding_max = len(number)
            self.number = int(number)
        else:
            self.path = self.grouping = path
            self.padding_min = self.padding_max = -1


def _divide_numbered_path_group(group: list[_NumberedPath]) -> dict[str, set[int]]:
    """Given a list of numbered paths that all have the same grouping string, check
    for padding consistency and split into multiple groups if necessary. Convert
    into a dictionary from a printf pattern to the set of numbers for it. Groups
    of size 2 are divided into individual paths.

    For example, the paths frame_001.png and frame_0002.png cannot be together,
    because they require different padding values."""

    result: dict[str, set[int]] = {}

    while len(group) > 0:
        # Treat groups of size 1 or 2 as individual paths
        if len(group) <= 2:
            for numbered_path in group:
                result[numbered_path.path] = set()
            break

        # The largest minimum padding is likely the right padding for the group
        padding = max(numbered_path.padding_min for numbered_path in group)
        pattern = f"%0{padding}d" if padding > 1 else "%d"
        consistent_group = [
            numbered_path for numbered_path in group if numbered_path.padding_max >= padding
        ]
        pattern_path = f"{consistent_group[0].parts[0]}{pattern}{consistent_group[0].parts[2]}"  # type: ignore
        result[pattern_path] = {numbered_path.number for numbered_path in consistent_group}  # type: ignore
        # Process the remaining paths separately
        group = [path for path in group if path.padding_max < padding]

    return result


class PathSummary:
    """
    Represents a summary of a path, including the path itself, the number of files,
    and the total size. The summary represents a sequence of files when the index_set
    value is non-empty. If the summary is a nested accumulation of paths, child
    paths are in the dictionary 'children'.

    If a path represents a directory, it ends with a directory separator.
    """

    path: str
    """Either the path, or a printf pattern if index_set is non-empty"""
    index_set: set[int]
    """The set of indexes if the path is a printf pattern or an empty set otherwise"""
    file_count: int
    """The number of files"""
    total_size: Optional[int]
    """The total size of all files, if sizes are provided"""
    children: Optional[dict[str, "PathSummary"]]
    """The children of this path, if the summary is nested"""

    def __init__(
        self,
        path: str,
        *,
        index_set: Optional[set[int]] = None,
        file_count: Optional[int] = None,
        total_size: Optional[int] = None,
        children: Optional[dict[str, "PathSummary"]] = None,
    ):
        self.path = path
        self.index_set = index_set or set()
        if index_set:
            self.file_count = len(index_set)
        elif file_count is None:
            self.file_count = 0 if self.is_dir() else 1
        else:
            self.file_count = file_count
        self.total_size = total_size
        self.children = children

    def is_dir(self) -> bool:
        """Returns True if the path is a directory (indicated by a trailing '/')"""
        # On Windows, both '/' and '\\' are directory separators, so check both sep and altsep
        return self.path.endswith(os.path.sep) or (
            os.path.altsep and self.path.endswith(os.path.altsep)
        )  # type: ignore

    def summary(self, *, include_totals=True, relative_to: Optional[Union[Path, str]] = None):
        """Returns the path summary, including file count and size totals by default."""
        relpath = self.path
        if relative_to is not None:
            relpath = os.path.relpath(self.path, relative_to)
            # Ensure a trailing separator for directories
            if self.is_dir():
                relpath = os.path.join(relpath, "")

        if include_totals:
            return f"{relpath} ({self.summary_totals()})"
        elif self.index_set:
            return f"{relpath} (sequence indexes {_int_set_to_range_expr(self.index_set)})"
        else:
            return relpath

    def summary_totals(self) -> str:
        """Returns the totals of the summary, like 'sequence indexes 1-3, 3 files, 30 MB'
        if the path represents a sequence or '1 file' if the path represents a file
        and there is no size information available."""
        if self.index_set:
            seq_summary = f", sequence {_int_set_to_range_expr(self.index_set)}"
        else:
            seq_summary = ""
        if self.total_size is not None:
            size_summary = f", {human_readable_file_size(self.total_size)}"
        else:
            size_summary = ""
        plural = "s" if self.file_count != 1 else ""
        return f"{self.file_count} file{plural}{size_summary}{seq_summary}"

    def __str__(self):
        return self.summary()

    def __repr__(self):
        parts = ["PathSummary(", repr(self.path)]
        if self.is_dir():
            if self.file_count != 0:
                parts.append(f", file_count={self.file_count!r}")
        else:
            if self.index_set:
                parts.append(f", index_set={{{', '.join(str(v) for v in sorted(self.index_set))}}}")
        if self.total_size is not None:
            parts.append(f", total_size={self.total_size!r}")
        if self.children is not None:
            parts.append(f", children={self.children!r}")
        parts.append(")")
        return "".join(parts)

    def __eq__(self, value):
        if isinstance(value, PathSummary):
            return (
                self.path == value.path
                and self.index_set == value.index_set
                and self.file_count == value.file_count
                and self.total_size == value.total_size
                and self.children == value.children
            )
        else:
            return False


def _int_set_to_range_expr(int_set: set[int]) -> str:
    """
    Converts a set of integers into a range expression.
    For example, {1,2,3,4,5,7,8,9,10} -> "1-5,7-10"
    """
    int_list = sorted(set(int_set))
    range_expr_components = []
    last_interval_start = last_interval_end = int_list[0]

    def add_interval(start: int, end: int):
        if start == last_interval_end:
            range_expr_components.append(str(start))
        else:
            range_expr_components.append(f"{start}-{end}")

    for value in int_list[1:]:
        if value == last_interval_end + 1:
            last_interval_end = value
        else:
            add_interval(last_interval_start, last_interval_end)
            last_interval_start = last_interval_end = value
    add_interval(last_interval_start, last_interval_end)
    return ",".join(range_expr_components)


def summarize_paths_by_sequence(
    path_list: Collection[Union[Path, str]], *, total_size_by_path: Optional[dict[str, int]] = None
) -> list[PathSummary]:
    """
    Identifies numbered sequences of files/directories within a list of paths.
    Returns a sorted list of PathSummary objects. If total_size_by_path is provided, it
    must provide a total size for every path in path_list.

    >> group_sequence_paths(["frame_1.png", "frame_3.png", "frame_20.png", "readme.txt"])
    {PathSummary("frame_%d.png", index_set={1, 3, 20}), PathSummary("readme.txt")}

    >> group_sequence_paths(["frame_01.png", "frame_1.png", "frame_30.png", "frame_09.png"])
    {PathSummary("frame_%02d.png", index_set={1, 9, 20}), PathSummary("frame_1.png")}
    """
    if len(path_list) == 0:
        return []

    # Group according to the _NumberedPath.grouping property
    raw_grouped_paths: dict[str, list[_NumberedPath]] = {}
    for path in path_list:
        numbered_path = _NumberedPath(path)
        raw_grouped_paths.setdefault(numbered_path.grouping, []).append(numbered_path)

    # Divide any groups with inconsistent padding into smaller consistent groups,
    # and merge into a dictionary {printf_pattern: set of indexes}.
    grouped_paths: dict[str, set[int]] = {}
    for raw_group in raw_grouped_paths.values():
        grouped_paths.update(_divide_numbered_path_group(raw_group))

    # Sort the result by the printf pattern and convert to PathSummary objects
    result = [
        PathSummary(path, index_set=index_set)
        for path, index_set in sorted(grouped_paths.items(), key=lambda x: x[0])
    ]

    # If sizes are provided, populate them in the path summary objects
    if total_size_by_path:
        for path_summary in result:
            if path_summary.index_set:
                path_summary.total_size = sum(
                    total_size_by_path[path_summary.path % i] for i in path_summary.index_set
                )
            else:
                path_summary.total_size = total_size_by_path[path_summary.path]

    return result


def _collapse_each_path_summary(path_summary_list: Iterable[PathSummary]) -> list[PathSummary]:
    """
    Collapses each path summary in the list while it has a single child.
    """
    result = []
    for path_summary in path_summary_list:  # type: ignore
        while path_summary.children is not None and len(path_summary.children) == 1:
            path_summary = next(iter(path_summary.children.values()))
        result.append(path_summary)

    return result


def summarize_paths_by_nested_directory(
    path_list: Collection[Union[Path, str]], *, total_size_by_path: Optional[dict[str, int]] = None
) -> list[PathSummary]:
    """Summarizes the provided paths by sequence, and then nests them into
    common parent paths. The returned summaries do not contain a common parent,
    for example if they are different relative paths, or absolute paths for
    different drives on Windows"""
    if len(path_list) == 0:
        return []

    # First summarize the paths by sequence
    summary_list = summarize_paths_by_sequence(path_list, total_size_by_path=total_size_by_path)

    # Put all the summaries into a temporary common root.
    nested_summary = PathSummary("ROOT/")
    for path_summary in summary_list:
        # Split the path into its components
        path_components = Path(path_summary.path).parts
        # Start with the root component, and build up the nested structure
        current_level: PathSummary = nested_summary
        for i in range(len(path_components) - 1):
            component = path_components[i]
            # Add the child if it's not already there
            if current_level.children is None:
                current_level.children = {}
            if component not in current_level.children:
                current_level.children[component] = PathSummary(
                    os.path.join(*path_components[: i + 1], ""),
                    total_size=0 if total_size_by_path else None,
                )
            # Descend into the new level
            current_level = current_level.children[component]
            # Accumulate the file counts and sizes
            current_level.file_count += path_summary.file_count
            if total_size_by_path:
                current_level.total_size += path_summary.total_size  # type: ignore
        # Add the path summary to the end
        if current_level.children is None:
            current_level.children = {}
        current_level.children[path_components[-1]] = path_summary

    # For each distinct root, collapse it while it contains a single child
    return _collapse_each_path_summary(nested_summary.children.values())  # type: ignore


def summarize_path_list(
    path_list: Collection[Union[Path, str]],
    *,
    total_size_by_path: Optional[dict[str, int]] = None,
    max_entries=10,
    include_totals=True,
) -> str:
    """
    Creates a string summary of the files in the list provided,
    grouping numbered filenames by their sequence pattern, and nesting
    summaries of the directory into the specified maximum number of entries.

    If total_size_by_path is provided, it must provide a total size for every path in path_list.

        >>> print(summarize_path_list(["frame_1.png", "frame_3.png", "frame_20.png", "readme.txt"]))
        frame_%d.png (3 files, sequence 1,3,20)
        readme.txt (1 file)
    """
    if len(path_list) == 0:
        return ""

    lines = []
    summary_list = summarize_paths_by_nested_directory(
        path_list, total_size_by_path=total_size_by_path
    )

    # If the summary list has one entry, and its path is a very shallow root like '/' or 'C:/',
    # then take all its children at the outer level. This makes the root paths longer so
    # the individually summarized paths will be shorter and easier to look through.
    if (
        len(summary_list) == 1
        and summary_list[0].children is not None
        and len(summary_list[0].children) <= max_entries / 2
    ):
        summary_list = _collapse_each_path_summary(summary_list[0].children.values())

    if total_size_by_path:
        # Sort the list so the largest size is first
        summary_list.sort(key=lambda v: (-v.total_size, v.path))  # type: ignore
    else:
        # Sort the list so the largest file count is first
        summary_list.sort(key=lambda v: (-v.file_count, v.path))  # type: ignore

    # Determine how many entries to show at the outer level and one level in,
    # with a total less than or equal to max_entries
    entry_counts = [
        0 if summary_path.children is None else min(len(summary_path.children), max_entries)
        for summary_path in summary_list[:max_entries]
    ]
    while len(entry_counts) + sum(entry_counts) > max_entries:
        max_entry_count = max(entry_counts)
        if max_entry_count > len(entry_counts):
            # If the largest entry count under a root path is more than the number of root paths,
            # then decrease that entry count.
            for i, entry_count in enumerate(reversed(entry_counts)):
                if entry_count == max_entry_count:
                    entry_counts[len(entry_counts) - i - 1] -= 1
                    break
        else:
            # Otherwise drop a root path from the summary
            entry_counts.pop()

    # If we're going to show "... and 1 more ..." after the items, might as well show
    # the last item instead
    if len(entry_counts) == len(summary_list) - 1 and not summary_list[-1].is_dir():
        entry_counts.append(0)

    for entry_count, summary_path in zip(entry_counts, summary_list):
        if summary_path.children is None:
            lines.append(f"{summary_path.summary(include_totals=include_totals)}\n")
        else:
            lines.append(f"{summary_path.summary(include_totals=include_totals)}:\n")
            children = list(summary_path.children.values())
            if total_size_by_path:
                # Sort the list so the largest size is first
                children.sort(key=lambda v: (-v.total_size, v.path))  # type: ignore
            else:
                # Sort the list so the largest file count is first
                children.sort(key=lambda v: (-v.file_count, v.path))  # type: ignore

            # If we're going to show "... and 1 more ..." after the items, might as well show
            # the last item instead
            if entry_count == len(children) - 1:
                entry_count += 1

            for child in children[:entry_count]:
                lines.append(
                    f"  {child.summary(include_totals=include_totals, relative_to=summary_path.path)}\n"
                )
            if len(summary_path.children) > entry_count:
                lines.append(f"  ... and {len(summary_path.children) - entry_count} more\n")

    if len(summary_list) > len(entry_counts):
        file_count = sum(v.file_count for v in summary_list[len(entry_counts) :])
        if total_size_by_path and include_totals:
            total_size = sum(v.total_size for v in summary_list[len(entry_counts) :])  # type: ignore
            lines.append(
                f"... and {len(summary_list) - len(entry_counts)} more ({file_count} files, {human_readable_file_size(total_size)})\n"
            )
        elif include_totals:
            lines.append(
                f"... and {len(summary_list) - len(entry_counts)} more ({file_count} files)\n"
            )
        else:
            lines.append(f"... and {len(summary_list) - len(entry_counts)} more\n")

    return "".join(lines)
