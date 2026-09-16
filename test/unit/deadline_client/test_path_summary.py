# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the path-group summary helper.

Windows semantics go through an explicit ``ntpath`` so these run on every platform.
"""

import ntpath
import posixpath

import pytest

from deadline.client._path_summary import common_ancestor
from deadline.client._path_utils import is_path_contained


@pytest.mark.parametrize("path_module", [ntpath, posixpath])
def test_common_ancestor_contains_its_inputs(path_module):
    """A non-empty common_ancestor must contain every path it was derived from."""
    paths = (
        [
            r"\\host\share\a",
            r"\\host\s2\b",
            r"C:\a\b",
            r"C:\a\c",
            "C:foo",
            r"..\a\b",
            r"..\a\c",
            # Drive-relative '..' puts the run behind an anchor, where a guard counting
            # from index zero would miss it.
            r"C:..\x",
            r"C:..\..\x",
            r"C:..\a\y",
            r"\\?\C:",
            r"\\?\C:\a",
        ]
        if path_module is ntpath
        else ["/a/b", "/a/c", "//a/d", "../a/b", "../a/c", "../../a/b", "rel/f", "rel/g"]
    )
    asserted = 0
    for first in paths:
        for second in paths:
            ancestor = common_ancestor([first, second], path_module=path_module)
            if not ancestor:
                continue
            asserted += 1
            assert is_path_contained(first, ancestor, path_module=path_module), (first, ancestor)
            assert is_path_contained(second, ancestor, path_module=path_module), (second, ancestor)

    # Most pairs here share no ancestor, and an empty answer is skipped above, so without a
    # floor a regression that returned nothing for everything would pass this vacuously.
    assert asserted >= len(paths), asserted


@pytest.mark.parametrize(
    "paths, path_module, expected",
    [
        # The common ancestor of paths under one share, spelled with its real case.
        (
            [r"\\host\Share\Proj\a.txt", r"\\host\Share\Proj\sub\b.txt"],
            ntpath,
            r"\\host\Share\Proj",
        ),
        # Different shares on one host share only the host. os.path.commonpath raises
        # ValueError for this pair.
        ([r"\\host\s1\a", r"\\host\s2\b"], ntpath, r"\\host"),
        # Different hosts share nothing. There is no location above a UNC host, so the
        # bare '\\\\' that their leading components have in common is not an answer.
        ([r"\\host1\s\a", r"\\host2\s\b"], ntpath, ""),
        ([r"\\host1", r"\\host2"], ntpath, ""),
        # Different drives share nothing.
        ([r"C:\a\b", r"D:\a\b"], ntpath, ""),
        ([r"C:\a\b", r"\\host\share\b"], ntpath, ""),
        ([r"C:\proj\a", r"C:\proj\b"], ntpath, r"C:\proj"),
        ([r"C:\proj\a"], ntpath, r"C:\proj\a"),
        (["/a/b/c", "/a/b/d"], posixpath, "/a/b"),
        (["/a/b", "/c/d"], posixpath, "/"),
        # A doubled POSIX root is the same space as '/', so these behave like ordinary
        # absolute paths rather than a separate namespace.
        (["//a/b", "//c/d"], posixpath, "/"),
        (["//a/b", "//a/c"], posixpath, "/a"),
        (["/a", "/b"], posixpath, "/"),
        (["/a/b"], posixpath, "/a/b"),
        (["a/b", "/c/d"], posixpath, ""),
        ([], posixpath, ""),
        # Paths whose unresolved leading '..' runs differ in depth are rooted at
        # different unknown directories, so they share none. Positional comparison
        # would wrongly read the shared '..' as one directory and return '..', which
        # is not an ancestor of '../../up'. os.path.commonpath has that bug.
        (["../up", "../../up"], posixpath, ""),
        (["../../up", "../up"], posixpath, ""),
        ([r"..\up", r"..\..\up"], ntpath, ""),
        # The '..' run can sit behind an anchor, where a guard counting from index 0
        # would not see it. 'C:..' is the cwd's parent on C:, 'C:..\..' its grandparent.
        ([r"C:..\x", r"C:..\..\x"], ntpath, ""),
        ([r"C:..", r"C:..\.."], ntpath, ""),
        # Equal depth behind an anchor is still comparable.
        ([r"C:..\a\x", r"C:..\a\y"], ntpath, r"C:..\a"),
        # Equal '..' depth is comparable again.
        (["../a/x", "../a/y"], posixpath, "../a"),
        (["../../a/x", "../../a/y"], posixpath, "../../a"),
        # Relative inputs keep their own spelling and gain no leading separator. A
        # Windows-style path read under posixpath semantics is one of these, since
        # 'C:' is an ordinary component there rather than a drive.
        (["a/b/c", "a/b/d"], posixpath, "a/b"),
        (
            ["C:/Users/u/renders/i1.png", "C:/Users/u/renders/i2.png"],
            posixpath,
            "C:/Users/u/renders",
        ),
    ],
)
def test_common_ancestor(paths, path_module, expected):
    assert common_ancestor(paths, path_module=path_module) == expected
