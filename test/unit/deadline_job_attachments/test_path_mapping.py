# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from pathlib import Path
import dataclasses

import pytest

from deadline.job_attachments._path_mapping import (
    _generate_path_mapping_rules,
    _PathMappingRuleApplier,
)
from deadline.job_attachments.models import (
    PathFormat,
    PathMappingRule,
    StorageProfileOperatingSystemFamily,
)


@dataclasses.dataclass
class DestPaths:
    path1: Path
    path2: Path
    path3: Path


# Fixtures for shared resources
@pytest.fixture
def dest_paths(tmp_path_factory: pytest.TempPathFactory):
    """Create a set of local directory paths to use."""
    base_dir = tmp_path_factory.mktemp("checkpoint")
    dest_path1 = base_dir / "path1"
    dest_path2 = base_dir / "path2" / "with" / "some" / "nesting"
    dest_path3 = base_dir / ("a" * 1000)
    yield DestPaths(dest_path1, dest_path2, dest_path3)


# Sample storage profiles for testing
SAMPLE_WINDOWS_STORAGE_PROFILE = {
    "storageProfileId": "sp-windows-123",
    "osFamily": StorageProfileOperatingSystemFamily.WINDOWS.value,
    "fileSystemLocations": [
        {"name": "shared", "path": "C:\\shared"},
        {"name": "temp", "path": "C:\\temp"},
    ],
}

SAMPLE_LINUX_STORAGE_PROFILE = {
    "storageProfileId": "sp-linux-456",
    "osFamily": StorageProfileOperatingSystemFamily.LINUX.value,
    "fileSystemLocations": [
        {"name": "shared", "path": "/mnt/shared"},
        {"name": "temp", "path": "/tmp"},
    ],
}

SAMPLE_MACOS_STORAGE_PROFILE = {
    "storageProfileId": "sp-macos-789",
    "osFamily": StorageProfileOperatingSystemFamily.MACOS.value,
    "fileSystemLocations": [
        {"name": "shared", "path": "/Volumes/shared"},
        {"name": "temp", "path": "/tmp"},
    ],
}

# Storage profiles for edge cases
EMPTY_LOCATIONS_PROFILE = {
    "storageProfileId": "sp-empty-001",
    "osFamily": StorageProfileOperatingSystemFamily.LINUX.value,
    "fileSystemLocations": [],
}

NO_MATCHING_LOCATIONS_PROFILE = {
    "storageProfileId": "sp-nomatch-002",
    "osFamily": StorageProfileOperatingSystemFamily.LINUX.value,
    "fileSystemLocations": [
        {"name": "different", "path": "/mnt/different"},
        {"name": "other", "path": "/mnt/other"},
    ],
}

WINDOWS_DESTINATION_PROFILE = {
    "storageProfileId": "sp-windows-dest-456",
    "osFamily": StorageProfileOperatingSystemFamily.WINDOWS.value,
    "fileSystemLocations": [
        {"name": "shared", "path": "D:\\shared"},
        {"name": "temp", "path": "D:\\temp"},
    ],
}

LINUX_DESTINATION_PROFILE = {
    "storageProfileId": "sp-linux-dest-789",
    "osFamily": StorageProfileOperatingSystemFamily.LINUX.value,
    "fileSystemLocations": [
        {"name": "shared", "path": "/opt/shared"},
        {"name": "temp", "path": "/var/tmp"},
    ],
}

# Test cases for _generate_path_mapping_rules function
GENERATE_PATH_MAPPING_RULES_CASES: tuple = (
    # (source_profile, destination_profile, expected_rules)
    pytest.param(
        SAMPLE_WINDOWS_STORAGE_PROFILE,
        SAMPLE_WINDOWS_STORAGE_PROFILE,
        [],
        id="identical profiles return empty list",
    ),
    pytest.param(
        SAMPLE_LINUX_STORAGE_PROFILE,
        NO_MATCHING_LOCATIONS_PROFILE,
        [],
        id="no matching locations return empty list",
    ),
    pytest.param(
        EMPTY_LOCATIONS_PROFILE,
        SAMPLE_LINUX_STORAGE_PROFILE,
        [],
        id="empty source locations return empty list",
    ),
    pytest.param(
        SAMPLE_WINDOWS_STORAGE_PROFILE,
        WINDOWS_DESTINATION_PROFILE,
        [
            PathMappingRule(PathFormat.WINDOWS.value, "C:\\shared", "D:\\shared"),
            PathMappingRule(PathFormat.WINDOWS.value, "C:\\temp", "D:\\temp"),
        ],
        id="Windows profiles generate Windows format rules",
    ),
    pytest.param(
        SAMPLE_LINUX_STORAGE_PROFILE,
        LINUX_DESTINATION_PROFILE,
        [
            PathMappingRule(PathFormat.POSIX.value, "/mnt/shared", "/opt/shared"),
            PathMappingRule(PathFormat.POSIX.value, "/tmp", "/var/tmp"),
        ],
        id="Linux profiles generate POSIX format rules",
    ),
    pytest.param(
        SAMPLE_MACOS_STORAGE_PROFILE,
        LINUX_DESTINATION_PROFILE,
        [
            PathMappingRule(PathFormat.POSIX.value, "/Volumes/shared", "/opt/shared"),
            PathMappingRule(PathFormat.POSIX.value, "/tmp", "/var/tmp"),
        ],
        id="macOS profiles generate POSIX format rules",
    ),
)


@pytest.mark.parametrize(
    (
        "source_profile",
        "destination_profile",
        "expected_rules",
    ),
    GENERATE_PATH_MAPPING_RULES_CASES,
)
def test_generate_path_mapping_rules(
    source_profile,
    destination_profile,
    expected_rules,
):
    """Test that _generate_path_mapping_rules generates correct path mapping rules."""
    rules = _generate_path_mapping_rules(source_profile, destination_profile)

    assert rules == expected_rules


def test_path_mapping_rule_applier_create_empty():
    applier = _PathMappingRuleApplier([])
    assert applier.path_mapping_rules == []
    assert applier._path_mapping_trie == {}

    assert applier.transform("/some/path") == "/some/path"
    with pytest.raises(ValueError):
        applier.strict_transform("/some/path")


def test_path_mapping_rule_applier_create_bad_source_path(dest_paths: DestPaths):
    with pytest.raises(ValueError):
        _PathMappingRuleApplier([PathMappingRule("xisop", "/mnt/shared", str(dest_paths.path1))])


def test_path_mapping_rule_applier_create_posix(dest_paths: DestPaths):
    rules = [
        PathMappingRule(PathFormat.POSIX.value, "/mnt/shared", str(dest_paths.path1)),
        PathMappingRule(PathFormat.POSIX.value, "/mnt/projects", str(dest_paths.path2)),
        PathMappingRule(PathFormat.POSIX.value, "/tmp", str(dest_paths.path3)),
    ]
    applier = _PathMappingRuleApplier(rules)
    assert applier.path_mapping_rules == rules
    assert applier.source_path_format == PathFormat.POSIX.value
    assert set(applier._path_mapping_trie.keys()) == {"/"}
    assert set(applier._path_mapping_trie["/"].keys()) == {"mnt", "tmp"}
    assert set(applier._path_mapping_trie["/"]["mnt"].keys()) == {"shared", "projects"}


def test_path_mapping_rule_applier_create_windows(dest_paths: DestPaths):
    rules = [
        PathMappingRule(PathFormat.WINDOWS.value, "C:\\Mnt\\Shared", str(dest_paths.path1)),
        PathMappingRule(PathFormat.WINDOWS.value, "C:\\Mnt\\proJects", str(dest_paths.path2)),
        PathMappingRule(PathFormat.WINDOWS.value, "D:\\tmp", str(dest_paths.path3)),
    ]
    applier = _PathMappingRuleApplier(rules)
    assert applier.path_mapping_rules == rules
    assert applier.source_path_format == PathFormat.WINDOWS.value
    assert set(applier._path_mapping_trie.keys()) == {"c:\\", "d:\\"}
    assert set(applier._path_mapping_trie["c:\\"].keys()) == {"mnt"}
    assert set(applier._path_mapping_trie["d:\\"].keys()) == {"tmp"}
    assert set(applier._path_mapping_trie["c:\\"]["mnt"].keys()) == {"shared", "projects"}


def test_path_mapping_rule_applier_create_mixed(dest_paths: DestPaths):
    with pytest.raises(ValueError):
        rules = [
            PathMappingRule(PathFormat.POSIX.value, "/mnt/shared", str(dest_paths.path1)),
            PathMappingRule(PathFormat.WINDOWS.value, "D:\\tmp", str(dest_paths.path3)),
        ]
        _PathMappingRuleApplier(rules)


def test_source_posix_rule(dest_paths: DestPaths):
    applier = _PathMappingRuleApplier(
        [
            PathMappingRule(PathFormat.POSIX.value, "/mnt/shared", str(dest_paths.path1)),
            PathMappingRule(PathFormat.POSIX.value, "/mnt/projects", str(dest_paths.path2)),
            PathMappingRule(PathFormat.POSIX.value, "/tmp", str(dest_paths.path3)),
        ]
    )

    # All three rules can be used with both regular and strict transform
    assert applier.transform("/mnt/shared") == dest_paths.path1
    assert applier.transform("/mnt/projects") == dest_paths.path2
    assert applier.transform("/tmp") == dest_paths.path3
    assert applier.strict_transform("/mnt/shared") == dest_paths.path1
    assert applier.strict_transform("/mnt/projects") == dest_paths.path2
    assert applier.strict_transform("/tmp") == dest_paths.path3

    # transform passes through other paths
    assert applier.transform("/other/path") == "/other/path"

    # strict_transform raises for other paths
    with pytest.raises(ValueError):
        applier.strict_transform("/other/path")


def test_source_windows_rule(dest_paths: DestPaths):
    applier = _PathMappingRuleApplier(
        [
            PathMappingRule(PathFormat.WINDOWS.value, "C:\\Shared", str(dest_paths.path1)),
            PathMappingRule(PathFormat.WINDOWS.value, "C:\\proJects", str(dest_paths.path2)),
            PathMappingRule(PathFormat.WINDOWS.value, "D:\\tmp", str(dest_paths.path3)),
        ]
    )

    # All three rules can be used with both regular and strict transform
    assert applier.transform("C:\\Shared") == dest_paths.path1
    assert applier.transform("C:\\proJects") == dest_paths.path2
    assert applier.transform("D:\\tmp") == dest_paths.path3
    assert applier.strict_transform("C:\\Shared") == dest_paths.path1
    assert applier.strict_transform("C:\\proJects") == dest_paths.path2
    assert applier.strict_transform("D:\\tmp") == dest_paths.path3

    # Windows is case insensitive but case preserving
    assert applier.transform("C:\\ShArEd") == dest_paths.path1
    assert (
        applier.transform("C:\\PROJECTS\\Case\\Of\\tail\\PreServed")
        == dest_paths.path2 / "Case" / "Of" / "tail" / "PreServed"
    )

    # transform passes through other paths
    assert applier.transform("C:\\other\\path") == "C:\\other\\path"

    # strict_transform raises for other paths
    with pytest.raises(ValueError):
        applier.strict_transform("C:\\other\\path")


def test_source_posix_rule_edge_cases(dest_paths: DestPaths):
    applier = _PathMappingRuleApplier(
        [
            PathMappingRule(PathFormat.POSIX.value, "/mnt/shared", str(dest_paths.path1)),
            PathMappingRule(PathFormat.POSIX.value, "/mnt/shared/projects", str(dest_paths.path2)),
            PathMappingRule(PathFormat.POSIX.value, "/tmp", str(dest_paths.path3)),
        ]
    )

    # Paths that are not transformed
    assert applier.transform("") == ""
    assert applier.transform("/") == "/"
    assert applier.transform("/other/path") == "/other/path"
    assert applier.transform("/mnt/other/path") == "/mnt/other/path"
    assert applier.transform("/Mnt/shared") == "/Mnt/shared"

    # Edge cases with unicode and spaces
    assert applier.transform("/mnt/shared/файл.txt") == dest_paths.path1 / "файл.txt"
    assert (
        applier.transform("/mnt/shared/file with spaces.txt")
        == dest_paths.path1 / "file with spaces.txt"
    )

    # The second rule applies because it is longer and more specific than the first rule
    assert applier.transform("/mnt/shared/projects") == dest_paths.path2
    assert applier.transform("/mnt/shared/projects/file.txt") == dest_paths.path2 / "file.txt"


def test_source_windows_rule_edge_cases(dest_paths: DestPaths):
    applier = _PathMappingRuleApplier(
        [
            PathMappingRule(PathFormat.WINDOWS.value, "C:\\shared", str(dest_paths.path1)),
            PathMappingRule(
                PathFormat.WINDOWS.value, "C:\\shared\\projects", str(dest_paths.path2)
            ),
            PathMappingRule(PathFormat.WINDOWS.value, "D:\\temp", str(dest_paths.path3)),
        ]
    )

    # Paths that are not transformed
    assert applier.transform("") == ""
    assert applier.transform("C:\\other\\path") == "C:\\other\\path"

    # Edge cases with unicode and spaces
    assert applier.transform("C:\\shared\\файл.txt") == dest_paths.path1 / "файл.txt"
    assert (
        applier.transform("C:\\shared\\file with spaces.txt")
        == dest_paths.path1 / "file with spaces.txt"
    )

    # The second rule applies because it is longer and more specific than the first rule
    assert applier.transform("C:\\shared\\projects") == dest_paths.path2
    assert applier.transform("C:\\shared\\projects\\file.txt") == dest_paths.path2 / "file.txt"
