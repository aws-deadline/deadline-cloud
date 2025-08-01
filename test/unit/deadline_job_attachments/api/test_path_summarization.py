# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import os

import pytest

from deadline.job_attachments.api import (
    PathSummary,
    human_readable_file_size,
    summarize_paths_by_sequence,
    summarize_paths_by_nested_directory,
    summarize_path_list,
)
from deadline.job_attachments.models import PathFormat


PARAMETRIZE_CASES: tuple = (
    (1000000000000000000, "1000.0 PB"),
    (89234597823492938, "89.23 PB"),
    (1000000000000001, "1.0 PB"),
    (1000000000000000, "1.0 PB"),
    (999999999999999, "1.0 PB"),
    (999995000000000, "1.0 PB"),
    (999994000000000, "999.99 TB"),
    (8934587945678, "8.93 TB"),
    (1000000000001, "1.0 TB"),
    (1000000000000, "1.0 TB"),
    (999999999999, "1.0 TB"),
    (999995000000, "1.0 TB"),
    (999994000000, "999.99 GB"),
    (83748237582, "83.75 GB"),
    (1000000001, "1.0 GB"),
    (1000000000, "1.0 GB"),
    (999999999, "1.0 GB"),
    (999995000, "1.0 GB"),
    (999994000, "999.99 MB"),
    (500229150, "500.23 MB"),
    (1000001, "1.0 MB"),
    (1000000, "1.0 MB"),
    (999999, "1.0 MB"),
    (999995, "1.0 MB"),
    (999994, "999.99 KB"),
    (96771, "96.77 KB"),
    (1001, "1.0 KB"),
    (1000, "1.0 KB"),
    (999, "999 B"),
    (934, "934 B"),
    (0, "0 B"),
)


@pytest.mark.parametrize(
    ("file_size", "expected_output"),
    PARAMETRIZE_CASES,
)
def test_human_readable_file_size(file_size: int, expected_output: str):
    """
    Test that given a file size in bytes, the expected human readable file size is output.
    """
    assert human_readable_file_size(file_size) == expected_output


@pytest.mark.parametrize(
    ("path_list", "expected_output"),
    [
        (
            ["frame_1.png", "frame_3.png", "frame_20.png", "readme.txt"],
            [PathSummary("frame_%d.png", index_set={1, 3, 20}), PathSummary("readme.txt")],
        ),
        (
            ["frame_01.png", "frame_1.png", "frame_30.png", "frame_09.png"],
            [PathSummary("frame_%02d.png", index_set={1, 9, 30}), PathSummary("frame_1.png")],
        ),
        (["00", "02", "03", "07", "10"], [PathSummary("%02d", index_set={0, 2, 3, 7, 10})]),
        (["0", "5", "99", "207"], [PathSummary("%d", index_set={0, 5, 99, 207})]),
        (
            [
                "dataset_2.tar.gz",
                "dataset_821.tar.gz",
                "dataset_1.tar.gz",
                "dataset_3.tar.gz",
                "dataset_12345.tar.gz",
                "dataset_23.tar.gz",
            ],
            [PathSummary("dataset_%d.tar.gz", index_set={1, 2, 3, 23, 821, 12345})],
        ),
        (
            ["frame_1.png", "frame_20.png", "7", "12", "000", "100", "789", "1000"],
            [
                PathSummary("%03d", index_set={0, 100, 789, 1000}),
                PathSummary("12"),
                PathSummary("7"),
                PathSummary("frame_1.png"),
                PathSummary("frame_20.png"),
            ],
        ),
    ],
)
def test_summarize_paths_without_nesting_without_sizes(path_list, expected_output):
    """Given each list of paths, confirm the expected output."""
    assert summarize_paths_by_sequence(path_list) == expected_output
    # Since there's no nesting in these cases, this is equivalent
    assert summarize_paths_by_nested_directory(path_list) == expected_output


PARAMETRIZE_CASES = (
    (
        ["frame_1.png", "frame_3.png", "frame_20.png", "readme.txt"],
        {"frame_1.png": 1, "frame_3.png": 2, "frame_20.png": 4, "readme.txt": 8},
        [
            PathSummary("frame_%d.png", index_set={1, 3, 20}, total_size=7),
            PathSummary("readme.txt", total_size=8),
        ],
    ),
    (
        ["frame_01.png", "frame_1.png", "frame_30.png", "frame_09.png"],
        {"frame_01.png": 1, "frame_1.png": 2, "frame_30.png": 4, "frame_09.png": 8},
        [
            PathSummary("frame_%02d.png", index_set={1, 9, 30}, total_size=13),
            PathSummary("frame_1.png", total_size=2),
        ],
    ),
    (
        ["00", "02", "03", "07", "10"],
        {"00": 1, "02": 2, "03": 4, "07": 8, "10": 16},
        [PathSummary("%02d", index_set={0, 2, 3, 7, 10}, total_size=31)],
    ),
    (
        ["0", "5", "99", "207"],
        {"0": 1, "5": 2, "99": 4, "207": 8},
        [PathSummary("%d", index_set={0, 5, 99, 207}, total_size=15)],
    ),
    (
        [
            "dataset_2.tar.gz",
            "dataset_821.tar.gz",
            "dataset_1.tar.gz",
            "dataset_3.tar.gz",
            "dataset_12345.tar.gz",
            "dataset_23.tar.gz",
        ],
        {
            "dataset_2.tar.gz": 1,
            "dataset_821.tar.gz": 2,
            "dataset_1.tar.gz": 4,
            "dataset_3.tar.gz": 8,
            "dataset_12345.tar.gz": 16,
            "dataset_23.tar.gz": 32,
        },
        [PathSummary("dataset_%d.tar.gz", index_set={1, 2, 3, 23, 821, 12345}, total_size=63)],
    ),
    (
        ["frame_1.png", "frame_20.png", "7", "12", "000", "100", "789", "1000"],
        {
            "frame_1.png": 1,
            "frame_20.png": 2,
            "7": 4,
            "12": 8,
            "000": 16,
            "100": 32,
            "789": 64,
            "1000": 128,
        },
        [
            PathSummary("%03d", index_set={0, 100, 789, 1000}, total_size=16 + 32 + 64 + 128),
            PathSummary("12", total_size=8),
            PathSummary("7", total_size=4),
            PathSummary("frame_1.png", total_size=1),
            PathSummary("frame_20.png", total_size=2),
        ],
    ),
)


@pytest.mark.parametrize(
    ("path_list", "sizes", "expected_output"),
    PARAMETRIZE_CASES,
)
def test_summarize_paths_without_nesting_with_sizes(path_list, sizes, expected_output):
    """Given each list of paths, confirm the expected output."""
    assert summarize_paths_by_sequence(path_list, total_size_by_path=sizes) == expected_output
    # Since there's no nesting in these cases, this is equivalent
    assert (
        summarize_paths_by_nested_directory(path_list, total_size_by_path=sizes) == expected_output
    )


PARAMETRIZE_CASES = (
    (
        [],
        [],
        [],
    ),
    (
        ["a/b/c/frame_1.png", "a/b/c/frame_3.png", "a/b/c/frame_20.png", "a/b/d/readme.txt"],
        [
            PathSummary("a/b/c/frame_%d.png".replace("/", os.path.sep), index_set={1, 3, 20}),
            PathSummary("a/b/d/readme.txt".replace("/", os.path.sep)),
        ],
        [
            PathSummary(
                "a/b/".replace("/", os.path.sep),
                file_count=4,
                children={
                    "c": PathSummary(
                        "a/b/c/".replace("/", os.path.sep),
                        file_count=3,
                        children={
                            "frame_%d.png": PathSummary(
                                "a/b/c/frame_%d.png".replace("/", os.path.sep),
                                index_set={1, 3, 20},
                            )
                        },
                    ),
                    "d": PathSummary(
                        "a/b/d/".replace("/", os.path.sep),
                        file_count=1,
                        children={
                            "readme.txt": PathSummary("a/b/d/readme.txt".replace("/", os.path.sep))
                        },
                    ),
                },
            )
        ],
    ),
    (
        ["seq/frame_01.png", "frame_1.png", "seq/frame_30.png", "seq/frame_09.png"],
        [
            PathSummary("frame_1.png"),
            PathSummary("seq/frame_%02d.png".replace("/", os.path.sep), index_set={1, 9, 30}),
        ],
        [
            PathSummary("frame_1.png"),
            PathSummary("seq/frame_%02d.png".replace("/", os.path.sep), index_set={1, 9, 30}),
        ],
    ),
    (
        ["/abc/def/ghi/00", "/abc/xyz/02", "/abc/def/jkl/mno/03", "/www/07", "/abc/def/10"],
        [
            PathSummary("/abc/def/10".replace("/", os.path.sep)),
            PathSummary("/abc/def/ghi/00".replace("/", os.path.sep)),
            PathSummary("/abc/def/jkl/mno/03".replace("/", os.path.sep)),
            PathSummary("/abc/xyz/02".replace("/", os.path.sep)),
            PathSummary("/www/07".replace("/", os.path.sep)),
        ],
        [
            PathSummary(
                os.path.sep,
                file_count=5,
                children={
                    "abc": PathSummary(
                        "/abc/".replace("/", os.path.sep),
                        file_count=4,
                        children={
                            "def": PathSummary(
                                "/abc/def/".replace("/", os.path.sep),
                                file_count=3,
                                children={
                                    "ghi": PathSummary(
                                        "/abc/def/ghi/".replace("/", os.path.sep),
                                        file_count=1,
                                        children={
                                            "00": PathSummary(
                                                "/abc/def/ghi/00".replace("/", os.path.sep)
                                            )
                                        },
                                    ),
                                    "jkl": PathSummary(
                                        "/abc/def/jkl/".replace("/", os.path.sep),
                                        file_count=1,
                                        children={
                                            "mno": PathSummary(
                                                "/abc/def/jkl/mno/".replace("/", os.path.sep),
                                                file_count=1,
                                                children={
                                                    "03": PathSummary(
                                                        "/abc/def/jkl/mno/03".replace(
                                                            "/", os.path.sep
                                                        )
                                                    )
                                                },
                                            )
                                        },
                                    ),
                                    "10": PathSummary("/abc/def/10".replace("/", os.path.sep)),
                                },
                            ),
                            "xyz": PathSummary(
                                "/abc/xyz/".replace("/", os.path.sep),
                                file_count=1,
                                children={
                                    "02": PathSummary("/abc/xyz/02".replace("/", os.path.sep))
                                },
                            ),
                        },
                    ),
                    "www": PathSummary(
                        "/www/".replace("/", os.path.sep),
                        file_count=1,
                        children={"07": PathSummary("/www/07".replace("/", os.path.sep))},
                    ),
                },
            )
        ],
    ),
)


@pytest.mark.parametrize(
    ("path_list", "expected_seq_output", "expected_nest_output"),
    PARAMETRIZE_CASES,
)
def test_summarize_paths_with_nesting_without_sizes(
    path_list, expected_seq_output, expected_nest_output
):
    """Given each list of paths, confirm the expected output."""
    assert summarize_paths_by_sequence(path_list) == expected_seq_output
    assert summarize_paths_by_nested_directory(path_list) == expected_nest_output


PARAMETRIZE_CASES = (
    ([], {}, [], []),
    (
        # Repeated paths will be deduplicated
        ["a/b/c", "a/b/c", "a/b/c", "a/b/c"],
        {"a/b/c": 1},
        [PathSummary("a/b/c".replace("/", os.path.sep), total_size=1)],
        [PathSummary("a/b/c".replace("/", os.path.sep), total_size=1)],
    ),
    (
        ["a/b/c/frame_1.png", "a/b/c/frame_3.png", "a/b/c/frame_20.png", "a/b/d/readme.txt"],
        {
            "a/b/c/frame_1.png": 1,
            "a/b/c/frame_3.png": 2,
            "a/b/c/frame_20.png": 4,
            "a/b/d/readme.txt": 8,
        },
        [
            PathSummary(
                "a/b/c/frame_%d.png".replace("/", os.path.sep), index_set={1, 3, 20}, total_size=7
            ),
            PathSummary("a/b/d/readme.txt".replace("/", os.path.sep), total_size=8),
        ],
        [
            PathSummary(
                "a/b/".replace("/", os.path.sep),
                file_count=4,
                total_size=15,
                children={
                    "c": PathSummary(
                        "a/b/c/".replace("/", os.path.sep),
                        file_count=3,
                        total_size=7,
                        children={
                            "frame_%d.png": PathSummary(
                                "a/b/c/frame_%d.png".replace("/", os.path.sep),
                                index_set={1, 3, 20},
                                total_size=7,
                            )
                        },
                    ),
                    "d": PathSummary(
                        "a/b/d/".replace("/", os.path.sep),
                        file_count=1,
                        total_size=8,
                        children={
                            "readme.txt": PathSummary(
                                "a/b/d/readme.txt".replace("/", os.path.sep), total_size=8
                            )
                        },
                    ),
                },
            )
        ],
    ),
    (
        ["seq/frame_01.png", "frame_1.png", "seq/frame_30.png", "seq/frame_09.png"],
        {"seq/frame_01.png": 1, "frame_1.png": 2, "seq/frame_30.png": 4, "seq/frame_09.png": 8},
        [
            PathSummary("frame_1.png", total_size=2),
            PathSummary(
                "seq/frame_%02d.png".replace("/", os.path.sep), index_set={1, 9, 30}, total_size=13
            ),
        ],
        [
            PathSummary("frame_1.png", total_size=2),
            PathSummary(
                "seq/frame_%02d.png".replace("/", os.path.sep), index_set={1, 9, 30}, total_size=13
            ),
        ],
    ),
    (
        ["/abc/def/ghi/00", "/abc/xyz/02", "/abc/def/jkl/mno/03", "/www/07", "/abc/def/10"],
        {
            "/abc/def/ghi/00": 1,
            "/abc/xyz/02": 2,
            "/abc/def/jkl/mno/03": 4,
            "/www/07": 8,
            "/abc/def/10": 16,
        },
        [
            PathSummary("/abc/def/10".replace("/", os.path.sep), total_size=16),
            PathSummary("/abc/def/ghi/00".replace("/", os.path.sep), total_size=1),
            PathSummary("/abc/def/jkl/mno/03".replace("/", os.path.sep), total_size=4),
            PathSummary("/abc/xyz/02".replace("/", os.path.sep), total_size=2),
            PathSummary("/www/07".replace("/", os.path.sep), total_size=8),
        ],
        [
            PathSummary(
                os.path.sep,
                file_count=5,
                total_size=31,
                children={
                    "abc": PathSummary(
                        "/abc/".replace("/", os.path.sep),
                        file_count=4,
                        total_size=23,
                        children={
                            "def": PathSummary(
                                "/abc/def/".replace("/", os.path.sep),
                                file_count=3,
                                total_size=21,
                                children={
                                    "ghi": PathSummary(
                                        "/abc/def/ghi/".replace("/", os.path.sep),
                                        file_count=1,
                                        total_size=1,
                                        children={
                                            "00": PathSummary(
                                                "/abc/def/ghi/00".replace("/", os.path.sep),
                                                total_size=1,
                                            )
                                        },
                                    ),
                                    "jkl": PathSummary(
                                        "/abc/def/jkl/".replace("/", os.path.sep),
                                        file_count=1,
                                        total_size=4,
                                        children={
                                            "mno": PathSummary(
                                                "/abc/def/jkl/mno/".replace("/", os.path.sep),
                                                file_count=1,
                                                total_size=4,
                                                children={
                                                    "03": PathSummary(
                                                        "/abc/def/jkl/mno/03".replace(
                                                            "/", os.path.sep
                                                        ),
                                                        total_size=4,
                                                    )
                                                },
                                            )
                                        },
                                    ),
                                    "10": PathSummary(
                                        "/abc/def/10".replace("/", os.path.sep), total_size=16
                                    ),
                                },
                            ),
                            "xyz": PathSummary(
                                "/abc/xyz/".replace("/", os.path.sep),
                                file_count=1,
                                total_size=2,
                                children={
                                    "02": PathSummary(
                                        "/abc/xyz/02".replace("/", os.path.sep), total_size=2
                                    )
                                },
                            ),
                        },
                    ),
                    "www": PathSummary(
                        "/www/".replace("/", os.path.sep),
                        file_count=1,
                        total_size=8,
                        children={
                            "07": PathSummary("/www/07".replace("/", os.path.sep), total_size=8)
                        },
                    ),
                },
            )
        ],
    ),
)


@pytest.mark.parametrize(
    ("path_list", "sizes", "expected_seq_output", "expected_nest_output"), PARAMETRIZE_CASES
)
def test_summarize_paths_with_nesting_with_sizes(
    path_list, sizes, expected_seq_output, expected_nest_output
):
    """Given each list of paths, confirm the expected output."""
    assert summarize_paths_by_sequence(path_list, total_size_by_path=sizes) == expected_seq_output
    assert (
        summarize_paths_by_nested_directory(path_list, total_size_by_path=sizes)
        == expected_nest_output
    )


PATH_LIST_DATASET_SEQ_TAR_GZ = [
    "dataset_2.tar.gz",
    "dataset_821.tar.gz",
    "dataset_1.tar.gz",
    "dataset_3.tar.gz",
    "dataset_12345.tar.gz",
    "dataset_23.tar.gz",
]
PATH_LIST_DATASET_SEQ_TAR_GZ_SIZES = {
    "dataset_2.tar.gz": 1,
    "dataset_821.tar.gz": 2,
    "dataset_1.tar.gz": 4,
    "dataset_3.tar.gz": 8,
    "dataset_12345.tar.gz": 16,
    "dataset_23.tar.gz": 32,
}

PATH_LIST_UNNUMBERED_FILES = [
    "file1.tar.gz",
    "file2.tar.gz",
    "sword.txt",
    "stone.png",
    "imagination.md",
]
PATH_LIST_UNNUMBERED_FILES_SIZES = {
    "file1.tar.gz": 1,
    "file2.tar.gz": 2,
    "sword.txt": 4,
    "stone.png": 8,
    "imagination.md": 16,
}

PATH_LIST_NESTED_FILES = [
    "seq/file1.tar.gz",
    "seq/file2.tar.gz",
    "seq/file3.tar.gz",
    "doc/sword.txt",
    "doc/images/stone.png",
    "doc/imagination.md",
    "README.md",
]
PATH_LIST_NESTED_FILES_SIZES = {
    "seq/file1.tar.gz": 1,
    "seq/file2.tar.gz": 2,
    "seq/file3.tar.gz": 4,
    "doc/sword.txt": 8,
    "doc/images/stone.png": 16,
    "doc/imagination.md": 32,
    "README.md": 64,
}

PARAMETRIZE_CASES = (
    (
        [],
        dict(),
        "",
    ),
    (
        PATH_LIST_DATASET_SEQ_TAR_GZ,
        dict(),
        "dataset_%d.tar.gz (6 files, sequence 1-3,23,821,12345)\n",
    ),
    (
        PATH_LIST_DATASET_SEQ_TAR_GZ,
        dict(include_totals=False),
        "dataset_%d.tar.gz (sequence indexes 1-3,23,821,12345)\n",
    ),
    (
        PATH_LIST_DATASET_SEQ_TAR_GZ,
        dict(total_size_by_path=PATH_LIST_DATASET_SEQ_TAR_GZ_SIZES),
        "dataset_%d.tar.gz (6 files, 63 B, sequence 1-3,23,821,12345)\n",
    ),
    (
        PATH_LIST_DATASET_SEQ_TAR_GZ,
        dict(total_size_by_path=PATH_LIST_DATASET_SEQ_TAR_GZ_SIZES, include_totals=False),
        "dataset_%d.tar.gz (sequence indexes 1-3,23,821,12345)\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(),
        "file1.tar.gz (1 file)\nfile2.tar.gz (1 file)\nimagination.md (1 file)\nstone.png (1 file)\nsword.txt (1 file)\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(max_entries=5),
        "file1.tar.gz (1 file)\nfile2.tar.gz (1 file)\nimagination.md (1 file)\nstone.png (1 file)\nsword.txt (1 file)\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(
            max_entries=4
        ),  # Special case, this shows 5 entries because the 5th line "..." would be less info than showing the actual file
        "file1.tar.gz (1 file)\nfile2.tar.gz (1 file)\nimagination.md (1 file)\nstone.png (1 file)\nsword.txt (1 file)\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(include_totals=False),
        "file1.tar.gz\nfile2.tar.gz\nimagination.md\nstone.png\nsword.txt\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(include_totals=False, max_entries=3),
        "file1.tar.gz\nfile2.tar.gz\nimagination.md\n... and 2 more\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(total_size_by_path=PATH_LIST_UNNUMBERED_FILES_SIZES),
        "imagination.md (1 file, 16 B)\nstone.png (1 file, 8 B)\nsword.txt (1 file, 4 B)\nfile2.tar.gz (1 file, 2 B)\nfile1.tar.gz (1 file, 1 B)\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(total_size_by_path=PATH_LIST_UNNUMBERED_FILES_SIZES, max_entries=3),
        "imagination.md (1 file, 16 B)\nstone.png (1 file, 8 B)\nsword.txt (1 file, 4 B)\n... and 2 more (2 files, 3 B)\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(total_size_by_path=PATH_LIST_UNNUMBERED_FILES_SIZES, include_totals=False),
        "imagination.md\nstone.png\nsword.txt\nfile2.tar.gz\nfile1.tar.gz\n",
    ),
    (
        PATH_LIST_UNNUMBERED_FILES,
        dict(
            total_size_by_path=PATH_LIST_UNNUMBERED_FILES_SIZES, include_totals=False, max_entries=3
        ),
        "imagination.md\nstone.png\nsword.txt\n... and 2 more\n",
    ),
    (
        PATH_LIST_NESTED_FILES,
        dict(),
        "doc/ (3 files):\n  images/ (1 file)\n  imagination.md (1 file)\n  sword.txt (1 file)\nseq/file%d.tar.gz (3 files, sequence 1-3)\nREADME.md (1 file)\n",
    ),
    (
        PATH_LIST_NESTED_FILES,
        dict(max_entries=2),
        "doc/ (3 files):\n  images/ (1 file)\n  ... and 2 more\n... and 2 more (4 files)\n".replace(
            "/", os.path.sep
        ),
    ),
    (
        PATH_LIST_NESTED_FILES,
        dict(max_entries=2, total_size_by_path=PATH_LIST_NESTED_FILES_SIZES),
        "README.md (1 file, 64 B)\n... and 2 more (6 files, 63 B)\n".replace("/", os.path.sep),
    ),
    (
        PATH_LIST_NESTED_FILES,
        dict(max_entries=5, total_size_by_path=PATH_LIST_NESTED_FILES_SIZES),
        "README.md (1 file, 64 B)\ndoc/ (3 files, 56 B):\n  imagination.md (1 file, 32 B)\n  images/ (1 file, 16 B)\n  sword.txt (1 file, 8 B)\nseq/file%d.tar.gz (3 files, 7 B, sequence 1-3)\n".replace(
            "/", os.path.sep
        ),
    ),
    (
        PATH_LIST_NESTED_FILES,
        dict(max_entries=5, total_size_by_path=PATH_LIST_NESTED_FILES_SIZES, include_totals=False),
        "README.md\ndoc/:\n  imagination.md\n  images/\n  sword.txt\nseq/file%d.tar.gz (sequence indexes 1-3)\n".replace(
            "/", os.path.sep
        ),
    ),
    (
        PATH_LIST_NESTED_FILES,
        dict(max_entries=5, include_totals=False),
        "doc/:\n  images/\n  imagination.md\n  sword.txt\nseq/file%d.tar.gz (sequence indexes 1-3)\nREADME.md\n".replace(
            "/", os.path.sep
        ),
    ),
)


@pytest.mark.parametrize(("path_list", "kwargs", "expected_output"), PARAMETRIZE_CASES)
def test_summarize_path_list(path_list, kwargs, expected_output):
    if "total_size_by_path" in kwargs:
        kwargs["total_size_by_path"] = {
            path.replace("/", os.path.sep): size
            for path, size in kwargs["total_size_by_path"].items()
        }
    assert summarize_path_list(
        [path.replace("/", os.path.sep) for path in path_list], **kwargs
    ) == expected_output.replace("/", os.path.sep)


PARAMETRIZE_CASES = (
    (
        PATH_LIST_NESTED_FILES,
        dict(path_format=PathFormat.POSIX),
        "doc/ (3 files):\n  images/ (1 file)\n  imagination.md (1 file)\n  sword.txt (1 file)\nseq/file%d.tar.gz (3 files, sequence 1-3)\nREADME.md (1 file)\n",
    ),
    (
        PATH_LIST_NESTED_FILES,
        dict(path_format=PathFormat.WINDOWS),
        "doc\\ (3 files):\n  images\\ (1 file)\n  imagination.md (1 file)\n  sword.txt (1 file)\nseq\\file%d.tar.gz (3 files, sequence 1-3)\nREADME.md (1 file)\n",
    ),
    (
        [path.replace("/", "\\") for path in PATH_LIST_NESTED_FILES],
        dict(path_format=PathFormat.WINDOWS),
        "doc\\ (3 files):\n  images\\ (1 file)\n  imagination.md (1 file)\n  sword.txt (1 file)\nseq\\file%d.tar.gz (3 files, sequence 1-3)\nREADME.md (1 file)\n",
    ),
)


@pytest.mark.parametrize(("path_list", "kwargs", "expected_output"), PARAMETRIZE_CASES)
def test_summarize_path_list_with_path_format(path_list, kwargs, expected_output):
    assert summarize_path_list(path_list, **kwargs) == expected_output
