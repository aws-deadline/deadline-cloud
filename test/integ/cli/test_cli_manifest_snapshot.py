# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integ tests for the CLI asset commands.
"""
import json
import os
from pathlib import Path
from click.testing import CliRunner
import pytest
import tempfile

from deadline.client.cli import main


class TestManifestSnapshot:

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield tmpdir_path

    def _create_test_manifest(self, tmp_path: str, root_dir: str) -> str:
        """
        Create some test files in the temp dir, snapshot it and return the manifest file.
        """
        TEST_MANIFEST_DIR = "manifest_dir"

        # Given
        manifest_dir = os.path.join(tmp_path, TEST_MANIFEST_DIR)
        os.makedirs(manifest_dir)

        subdir1 = os.path.join(root_dir, "subdir1")
        subdir2 = os.path.join(root_dir, "subdir2")
        os.makedirs(subdir1)
        os.makedirs(subdir2)
        Path(os.path.join(subdir1, "file1.txt")).touch()
        Path(os.path.join(subdir2, "file2.txt")).touch()

        # When snapshot is called.
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "manifest",
                "snapshot",
                "--root",
                root_dir,
                "--destination",
                manifest_dir,
                "--name",
                "test",
            ],
        )
        assert result.exit_code == 0, result.output

        manifest_files = os.listdir(manifest_dir)
        assert (
            len(manifest_files) == 1
        ), f"Expected exactly one manifest file, but got {len(manifest_files)}"
        manifest = manifest_files[0]
        assert "test" in manifest, f"Expected test in manifest file name, got {manifest}"

        # Return the manifest that we found.
        return os.path.join(manifest_dir, manifest)

    @pytest.mark.parametrize(
        "json_output",
        [
            pytest.param(True),
            pytest.param(False),
        ],
    )
    def test_manifest_diff(self, tmp_path: str, json_output: bool):
        """
        Tests if manifest diff CLI works, basic case. Variation on JSON as printout.
        Business logic testing will be done at the API level where we can check the outputs.
        """
        TEST_ROOT_DIR = "root_dir"

        # Given a created manifest file...
        root_dir = os.path.join(tmp_path, TEST_ROOT_DIR)
        manifest = self._create_test_manifest(tmp_path, root_dir)

        # Lets add another file.
        new_file = "file3.txt"
        Path(os.path.join(root_dir, new_file)).touch()

        # When
        runner = CliRunner()
        args = ["manifest", "diff", "--root", root_dir, "--manifest", manifest]
        if json_output:
            args.append("--json")
        result = runner.invoke(main, args)

        # Then
        assert result.exit_code == 0, result.output
        if json_output:
            # If JSON mode was specified, make sure the output is JSON and contains the new file.
            diff = json.loads(result.output)
            assert len(diff["new"]) == 1
            assert new_file in diff["new"]
