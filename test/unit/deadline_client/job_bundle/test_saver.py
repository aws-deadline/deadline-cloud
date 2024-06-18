# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
from pathlib import Path
import tempfile
import pytest
from deadline.client.job_bundle.saver import save_yaml_or_json_to_file


def test_save_yaml_or_json_to_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        save_yaml_or_json_to_file(
            bundle_dir=temp_dir, filename="test", data={"test": "test"}, file_type="YAML"
        )
        assert Path(os.path.join(temp_dir, "test.yaml")).read_text() == "test: test\n"

    with tempfile.TemporaryDirectory() as temp_dir:
        save_yaml_or_json_to_file(
            bundle_dir=temp_dir, filename="test", data={"test": "test"}, file_type="JSON"
        )
        assert Path(os.path.join(temp_dir, "test.json")).read_text() == '{\n  "test": "test"\n}'

    with tempfile.TemporaryDirectory() as temp_dir:
        with pytest.raises(RuntimeError):
            save_yaml_or_json_to_file(
                bundle_dir=temp_dir, filename="test", data={"test": "test"}, file_type="NOT_VALID"
            )
