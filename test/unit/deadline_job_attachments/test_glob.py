# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
from deadline.client.exceptions import NonValidInputError
import pytest
from typing import List
from deadline.job_attachments._glob import _glob_paths, _process_glob_inputs


def test_glob_inputs_string(glob_config_file):
    """
    Test case to test glob config as a string.
    """
    glob: str
    with open(glob_config_file) as f:
        glob = f.read()
    glob_config = _process_glob_inputs(glob)
    assert "include.file" in glob_config.include_glob
    assert "exclude.file" in glob_config.exclude_glob


def test_glob_inputs_file(glob_config_file):
    """
    Test case to test glob config as a file.
    """
    glob_config = _process_glob_inputs(glob_config_file)
    assert "include.file" in glob_config.include_glob
    assert "exclude.file" in glob_config.exclude_glob


def test_bad_glob_string():
    """
    Test case to test a bad glob config will raise an exception.
    """
    glob: str = "This is not a json"
    with pytest.raises(NonValidInputError):
        _process_glob_inputs(glob)


def test_glob_path_default(test_glob_folder: str):
    """
    Test case to glob all files.
    """
    globbed_files: List[str] = _glob_paths(path=test_glob_folder)

    # There are 4 files
    assert len(globbed_files) == 4
    assert os.path.join(os.sep, test_glob_folder, "include.txt") in globbed_files
    assert os.path.join(os.sep, test_glob_folder, "exclude.txt") in globbed_files
    assert os.path.join(os.sep, test_glob_folder, "nested", "nested_include.txt") in globbed_files
    assert os.path.join(os.sep, test_glob_folder, "nested", "nested_exclude.txt") in globbed_files


def test_glob_path_default_include(test_glob_folder: str):
    """
    Test case to glob all files.
    """
    globbed_files: List[str] = _glob_paths(
        path=test_glob_folder, include=["*include.txt", "*/*include.txt"]
    )

    # There are 2 files
    assert len(globbed_files) == 2
    assert os.path.join(os.sep, test_glob_folder, "include.txt") in globbed_files
    assert os.path.join(os.sep, test_glob_folder, "nested", "nested_include.txt") in globbed_files


def test_glob_path_exclude(test_glob_folder: str):
    """
    Test case to glob all files and exclude some.
    """
    globbed_files: List[str] = _glob_paths(
        path=test_glob_folder, exclude=["*exclude.txt", "*/*exclude.txt"]
    )

    # There are 4 files
    assert len(globbed_files) == 2
    assert os.path.join(os.sep, test_glob_folder, "include.txt") in globbed_files
    assert os.path.join(os.sep, test_glob_folder, "nested", "nested_include.txt") in globbed_files
