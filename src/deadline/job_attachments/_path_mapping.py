# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Callable, Optional, Union

from .models import PathFormat, PathMappingRule, StorageProfileOperatingSystemFamily

__all__ = ["_generate_path_mapping_rules", "_PathMappingRuleApplier"]

"""
This file contains functionality related to path mapping rules. This functionality is internal-only for now,
to be marked as public after developing some experience with it.
"""


def _generate_path_mapping_rules(
    source_storage_profile: dict[str, Any],
    destination_storage_profile: dict[str, Any],
) -> list[PathMappingRule]:
    """
    Given a pair of storage profiles, generate all the path mapping rules to transform paths
    from the source to the destination.

    A mapping rule is generated for every file system location name that's shared between
    the storage profile regardless of the type (SHARED vs LOCAL), to account for the broadest
    possible storage profile configurations.

    Args:
        source_storage_profile: A storage profile as returned by boto3 deadline.get_storage_profile or
            deadline.get_storage_profile_for_queue.
        destination_storage_profile: A storage profile as returned by boto3 deadline.get_storage_profile or
            deadline.get_storage_profile_for_queue.
    Returns:
        A list of path mapping rules to transform paths.
    """
    # If the source and destination are identical, no transformation is needed
    if (
        source_storage_profile["storageProfileId"]
        == destination_storage_profile["storageProfileId"]
    ):
        return []

    # Put the locations into dictionaries to match up the names
    source_locations = {
        location["name"]: location for location in source_storage_profile["fileSystemLocations"]
    }
    destination_locations = {
        location["name"]: location
        for location in destination_storage_profile["fileSystemLocations"]
    }

    if (
        source_storage_profile["osFamily"].lower()
        == StorageProfileOperatingSystemFamily.WINDOWS.value
    ):
        source_path_format = PathFormat.WINDOWS.value
    else:
        source_path_format = PathFormat.POSIX.value

    path_mapping_rules: list[PathMappingRule] = []
    for source_name, source_location in source_locations.items():
        if source_name in destination_locations:
            path_mapping_rules.append(
                PathMappingRule(
                    source_path_format,
                    source_location["path"],
                    destination_locations[source_name]["path"],
                )
            )

    return path_mapping_rules


class _PathMappingRuleApplier:
    """
    This class provides an accelerated implementation for transforming paths according to a list of
    path mapping rules. For details about how rules are applied, see the documentation
    https://github.com/OpenJobDescription/openjd-specifications/wiki/How-Jobs-Are-Run#applying-path-mapping-rules-within-a-job-template

    When mapping a path, the most specific rule is the one that applies. For example, if there are two rules
        * '/mnt/Projects -> X:\\Projects'
        * '/mnt/Projects/Special -> Y:\\'
    then '/mnt/Projects/Special/data.txt' maps to 'Y:\\data.txt', not to 'X:\\Projects\\Special\\data.txt'.

    The implementation uses a trie data structure for acceleration, as follows:

    1. The trie is a dictionary, where every key is a string and every value is another trie.
       The one exception is the key ".", which holds the destination path of a rule instead.
    2. Each source path is divided into parts by the PurePosixPath or PureWindowsPath class.
    3. Each subsequent level of the trie corresponds to the matching subsequent part of a path.
    4. A rule with parts (part1, part2, ..., partN) -> destination_path is represented in the trie
       by the equation trie[part1][part2]...[partN]["."] == destination_path.

    For Windows source paths, the parts are transformed to lower case within the trie to make the transformation
    case insensitive while still case preserving.
    """

    source_path_format: Optional[str] = None
    path_mapping_rules: list[PathMappingRule]

    _path_mapping_trie: dict[str, Any]

    # These two members implement the windows- or posix-specific parts of the trie.
    # _split_source_path is used to divide a source path into parts, and _part_normalization is used
    # to normalize a part for trie insertion or lookup.
    _split_source_path: Callable[[str], tuple[str, ...]]
    _normalize_part: Callable[[str], str]

    def __init__(self, path_mapping_rules: list[PathMappingRule]):
        self.path_mapping_rules = path_mapping_rules
        self._path_mapping_trie = {}

        trie_entry: dict

        if path_mapping_rules:
            self.source_path_format = path_mapping_rules[0].source_path_format
            if not all(
                rule.source_path_format == self.source_path_format for rule in path_mapping_rules
            ):
                formats = list({rule.source_path_format for rule in path_mapping_rules})
                raise ValueError(
                    f"The path mapping rules included multiple source path formats {', '.join(formats)}, only one is permitted."
                )

            if self.source_path_format == PathFormat.POSIX.value:
                self._split_source_path = lambda v: PurePosixPath(v).parts
                self._normalize_part = lambda v: v
            elif self.source_path_format == PathFormat.WINDOWS.value:
                self._split_source_path = lambda v: PureWindowsPath(v).parts
                self._normalize_part = lambda v: v.lower()
            else:
                raise ValueError(f"Unexpected source path format {self.source_path_format}")

            for rule in path_mapping_rules:
                trie_entry = self._path_mapping_trie
                parts = self._split_source_path(rule.source_path)
                # Traverse all the parts using trie_entry
                for part in parts:
                    trie_entry = trie_entry.setdefault(self._normalize_part(part), {})
                # Set the destination path of the trie entry
                trie_entry["."] = Path(rule.destination_path)
        else:
            self.source_path_format = None

    def _transform(self, path: str) -> Union[None, Path]:
        parts = self._split_source_path(path)

        matched_destination_path = None
        matched_remaining_parts = None

        # Traverse the trie using trie_entry
        trie_entry: dict = self._path_mapping_trie
        for i, part in enumerate(parts):
            next_trie_entry = trie_entry.get(self._normalize_part(part))
            # Stop if there are no rules with this path prefix
            if next_trie_entry is None:
                break
            # Record the match if there is one at this path prefix,
            # overwriting any previous match to apply the longest rule.
            destination_path = next_trie_entry.get(".")
            if destination_path:
                matched_destination_path = destination_path
                matched_remaining_parts = parts[i + 1 :]
            trie_entry = next_trie_entry

        if matched_destination_path is None:
            return None
        else:
            return matched_destination_path.joinpath(*matched_remaining_parts)

    def strict_transform(self, source_path: str) -> Path:
        """Transform the provided path according to the path mapping rules. Raise ValueError if no rule applied."""
        if self.source_path_format is not None:
            result = self._transform(source_path)
            if result:
                return result

        raise ValueError("No path mapping rule could be applied")

    def transform(self, source_path: str) -> Union[str, Path]:
        """Transform the provided path according to the path mapping rules. Return an untransformed path if no rule applied."""
        if self.source_path_format is not None:
            result = self._transform(source_path)
            if result:
                return result

        return source_path
