# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for submission hooks functionality."""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time

import pytest
import yaml

from typing import List

from deadline.client import api, config
from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle._hooks import (
    HookConfiguration,
    HookDefinition,
    HookManager,
    HookMetadata,
    HookResult,
    collect_pre_gui_hook_sources,
    collect_submission_hook_sources,
)
from deadline.client.job_bundle._hooks._executor import HookExecutor
from deadline.client.job_bundle._hooks._merger import merge_asset_references, merge_payload
from deadline.client.job_bundle._hooks._validator import (
    validate_pre_gui_output,
    validate_configuration,
    validate_modified_payload,
)

from ..testing_utilities import patch_calls_for_create_job_from_job_bundle


class TestHookDefinition:
    """Tests for HookDefinition data model."""

    def test_from_dict_minimal(self):
        """Test parsing hook with only required fields."""
        data = {"command": "python"}
        hook = HookDefinition.from_dict(data)
        assert hook.command == "python"
        assert hook.args == []
        assert hook.timeout == 60
        assert hook.env == {}

    def test_from_dict_full(self):
        """Test parsing hook with all fields."""
        data = {
            "command": "python",
            "args": ["-c", "print('hello')"],
            "timeout": 30,
            "env": {"FOO": "bar"},
        }
        hook = HookDefinition.from_dict(data)
        assert hook.command == "python"
        assert hook.args == ["-c", "print('hello')"]
        assert hook.timeout == 30
        assert hook.env == {"FOO": "bar"}


class TestHookConfiguration:
    """Tests for HookConfiguration data model."""

    def test_from_dict_empty(self):
        """Test parsing empty configuration."""
        config = HookConfiguration.from_dict({})
        assert config.pre_gui == []
        assert config.pre_submission == []
        assert config.post_submission == []
        assert config.version == "1.0"

    def test_from_dict_with_version(self):
        """Test parsing configuration with explicit version."""
        data = {"version": "1.0", "preSubmission": [{"command": "test.py"}]}
        config = HookConfiguration.from_dict(data)
        assert config.version == "1.0"

    def test_from_dict_with_hooks(self):
        """Test parsing configuration with hooks."""
        data = {
            "preSubmission": [{"command": "validate.py"}],
            "postSubmission": [{"command": "notify.py"}, {"command": "log.py"}],
        }
        config = HookConfiguration.from_dict(data)
        assert len(config.pre_submission) == 1
        assert len(config.post_submission) == 2
        assert config.pre_submission[0].command == "validate.py"


class TestHookMetadata:
    """Tests for HookMetadata data model."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        metadata = HookMetadata(
            job_name="TestJob",
            priority=50,
            farm_id="farm-123",
            queue_id="queue-456",
            job_bundle_dir="/path/to/bundle",
            parameters={"Param1": "value1"},
            submitter_name="TestSubmitter",
            asset_references={"inputFilenames": ["/file.txt"]},
            submission_payload={"farmId": "farm-123"},
            storage_profile_id="sp-789",
            job_id="job-abc",
        )
        d = metadata.to_dict()
        assert d["jobName"] == "TestJob"
        assert d["priority"] == 50
        assert d["farmId"] == "farm-123"
        assert d["storageProfileId"] == "sp-789"
        assert d["jobId"] == "job-abc"

    def test_to_dict_without_optional(self):
        """Test serialization without optional fields."""
        metadata = HookMetadata(
            job_name="TestJob",
            priority=50,
            farm_id="farm-123",
            queue_id="queue-456",
            job_bundle_dir="/path/to/bundle",
            parameters={},
            submitter_name="TestSubmitter",
            asset_references={},
            submission_payload={},
        )
        d = metadata.to_dict()
        assert "storageProfileId" not in d
        assert "jobId" not in d

    def test_to_json(self):
        """Test JSON serialization."""
        metadata = HookMetadata(
            job_name="TestJob",
            priority=50,
            farm_id="farm-123",
            queue_id="queue-456",
            job_bundle_dir="/path/to/bundle",
            parameters={},
            submitter_name="TestSubmitter",
            asset_references={},
            submission_payload={},
        )
        j = metadata.to_json()
        parsed = json.loads(j)
        assert parsed["jobName"] == "TestJob"

    def test_to_environment_variables(self):
        """Test environment variable generation."""
        metadata = HookMetadata(
            job_name="TestJob",
            priority=50,
            farm_id="farm-123",
            queue_id="queue-456",
            job_bundle_dir="/path/to/bundle",
            parameters={},
            submitter_name="TestSubmitter",
            asset_references={},
            submission_payload={},
            storage_profile_id="sp-789",
            job_id="job-abc",
        )
        env = metadata.to_environment_variables()
        assert env["DEADLINE_JOB_NAME"] == "TestJob"
        assert env["DEADLINE_PRIORITY"] == "50"
        assert env["DEADLINE_FARM_ID"] == "farm-123"
        assert env["DEADLINE_QUEUE_ID"] == "queue-456"
        assert env["DEADLINE_JOB_BUNDLE_DIR"] == "/path/to/bundle"
        assert env["DEADLINE_STORAGE_PROFILE_ID"] == "sp-789"
        assert env["DEADLINE_JOB_ID"] == "job-abc"


class TestHookResult:
    """Tests for HookResult data model."""

    def test_is_success_true(self):
        """Test successful result."""
        result = HookResult(exit_code=0, stdout="", stderr="", execution_time=1.0, timed_out=False)
        assert result.is_success()

    def test_is_success_false_exit_code(self):
        """Test failed result due to exit code."""
        result = HookResult(exit_code=1, stdout="", stderr="", execution_time=1.0, timed_out=False)
        assert not result.is_success()

    def test_is_success_false_timeout(self):
        """Test failed result due to timeout."""
        result = HookResult(exit_code=0, stdout="", stderr="", execution_time=1.0, timed_out=True)
        assert not result.is_success()


class TestValidateConfiguration:
    """Tests for configuration validation."""

    def test_valid_configuration(self):
        """Test valid configuration passes."""
        config = {
            "preSubmission": [{"command": "python", "args": ["-c", "pass"], "timeout": 30}],
            "postSubmission": [{"command": "echo", "env": {"FOO": "bar"}}],
        }
        validate_configuration(config)  # Should not raise

    def test_invalid_pre_submission_not_list(self):
        """Test preSubmission must be a list."""
        with pytest.raises(DeadlineOperationError, match="must be a list"):
            validate_configuration({"preSubmission": "not a list"})

    def test_invalid_hook_not_dict(self):
        """Test hook must be a dict."""
        with pytest.raises(DeadlineOperationError, match="must be an object"):
            validate_configuration({"preSubmission": ["not a dict"]})

    def test_invalid_missing_command(self):
        """Test command is required."""
        with pytest.raises(DeadlineOperationError, match="missing required 'command'"):
            validate_configuration({"preSubmission": [{"args": []}]})

    def test_invalid_command_not_string(self):
        """Test command must be string."""
        with pytest.raises(DeadlineOperationError, match="'command' must be a string"):
            validate_configuration({"preSubmission": [{"command": 123}]})

    def test_invalid_args_not_list(self):
        """Test args must be a list."""
        with pytest.raises(DeadlineOperationError, match="'args' must be a list"):
            validate_configuration({"preSubmission": [{"command": "echo", "args": "not list"}]})

    def test_invalid_timeout_not_positive(self):
        """Test timeout must be positive integer."""
        with pytest.raises(DeadlineOperationError, match="'timeout' must be a positive integer"):
            validate_configuration({"preSubmission": [{"command": "echo", "timeout": 0}]})

    def test_invalid_timeout_negative(self):
        """Test timeout cannot be negative."""
        with pytest.raises(DeadlineOperationError, match="'timeout' must be a positive integer"):
            validate_configuration({"preSubmission": [{"command": "echo", "timeout": -1}]})

    def test_invalid_env_not_dict(self):
        """Test env must be a dict."""
        with pytest.raises(DeadlineOperationError, match="'env' must be an object"):
            validate_configuration({"preSubmission": [{"command": "echo", "env": "not dict"}]})

    def test_valid_version(self):
        """Test valid version passes."""
        validate_configuration({"version": "1.0", "preSubmission": [{"command": "echo"}]})

    def test_invalid_version(self):
        """Test unsupported version raises error."""
        with pytest.raises(DeadlineOperationError, match="Unsupported hooks version"):
            validate_configuration({"version": "2.0", "preSubmission": [{"command": "echo"}]})


class TestValidateModifiedPayload:
    """Tests for modified payload validation."""

    def test_valid_payload(self):
        """Test valid payload passes."""
        validate_modified_payload({"priority": 100}, "test_hook")

    def test_invalid_not_dict(self):
        """Test payload must be dict."""
        with pytest.raises(DeadlineOperationError, match="must be a JSON object"):
            validate_modified_payload("not a dict", "test_hook")  # type: ignore[arg-type]

    def test_invalid_attachments_not_dict(self):
        """Test attachments must be dict."""
        with pytest.raises(DeadlineOperationError, match="'attachments' must be an object"):
            validate_modified_payload({"attachments": "not dict"}, "test_hook")

    def test_invalid_asset_references_not_dict(self):
        """Test assetReferences must be dict."""
        with pytest.raises(DeadlineOperationError, match="'assetReferences' must be an object"):
            validate_modified_payload({"attachments": {"assetReferences": "not dict"}}, "test_hook")

    def test_invalid_input_filenames_not_list(self):
        """Test inputFilenames must be list."""
        with pytest.raises(DeadlineOperationError, match="inputFilenames.*must be a list"):
            validate_modified_payload(
                {"attachments": {"assetReferences": {"inputFilenames": "not list"}}}, "test_hook"
            )

    def test_valid_parameters_object(self):
        """A 'parameters' object is accepted."""
        validate_modified_payload({"parameters": {"Foo": "bar"}}, "test_hook")

    def test_invalid_parameters_not_dict(self):
        """A 'parameters' value that is not an object is rejected with a clear error."""
        with pytest.raises(DeadlineOperationError, match="'parameters' must be an object"):
            validate_modified_payload({"parameters": "not a dict"}, "test_hook")


class TestMergeAssetReferences:
    """Tests for asset reference merging."""

    def test_merge_empty(self):
        """Test merging empty references."""
        result = merge_asset_references(None, None)
        assert result == {}

    def test_merge_original_only(self):
        """Test merging with only original."""
        original = {"inputFilenames": ["/a.txt", "/b.txt"]}
        result = merge_asset_references(original, None)
        assert set(result["inputFilenames"]) == {"/a.txt", "/b.txt"}

    def test_merge_modified_only(self):
        """Test merging with only modified."""
        modified = {"inputFilenames": ["/c.txt"]}
        result = merge_asset_references(None, modified)
        assert result["inputFilenames"] == ["/c.txt"]

    def test_merge_union(self):
        """Test merging replaces nested keys from modified."""
        original = {"inputFilenames": ["/a.txt", "/b.txt"]}
        modified = {"inputFilenames": ["/b.txt", "/c.txt"]}
        result = merge_asset_references(original, modified)
        assert result["inputFilenames"] == ["/b.txt", "/c.txt"]

    def test_merge_all_fields(self):
        """Test merging all asset reference fields."""
        original = {
            "inputFilenames": ["/a.txt"],
            "inputDirectories": ["/dir1"],
            "outputDirectories": ["/out1"],
            "referencedPaths": ["/ref1"],
        }
        modified = {
            "inputFilenames": ["/b.txt"],
            "inputDirectories": ["/dir2"],
            "outputDirectories": ["/out2"],
            "referencedPaths": ["/ref2"],
        }
        result = merge_asset_references(original, modified)
        assert result["inputFilenames"] == ["/b.txt"]
        assert result["inputDirectories"] == ["/dir2"]
        assert result["outputDirectories"] == ["/out2"]
        assert result["referencedPaths"] == ["/ref2"]


class TestMergePayload:
    """Tests for payload merging."""

    def test_merge_simple_field(self):
        """Test merging simple fields."""
        original = {"priority": 50, "farmId": "farm-123"}
        modified = {"priority": 100}
        result = merge_payload(original, modified)
        assert result["priority"] == 100
        assert result["farmId"] == "farm-123"

    def test_merge_new_field(self):
        """Test adding new fields."""
        original = {"priority": 50}
        modified = {"maxWorkerCount": 10}
        result = merge_payload(original, modified)
        assert result["priority"] == 50
        assert result["maxWorkerCount"] == 10

    def test_merge_asset_references(self):
        """Test merging asset references."""
        original = {
            "attachments": {
                "assetReferences": {"inputFilenames": ["/a.txt"]},
                "fileSystem": "COPIED",
            }
        }
        modified = {"attachments": {"assetReferences": {"inputFilenames": ["/b.txt"]}}}
        result = merge_payload(original, modified)
        assert result["attachments"]["assetReferences"]["inputFilenames"] == ["/b.txt"]
        assert result["attachments"]["fileSystem"] == "COPIED"

    def test_merge_parameters_per_key(self):
        """Parameters merge per-key across sequential merges (multiple pre-submission
        hooks) so a later hook does not discard parameters set by an earlier one."""
        # Simulates hook #1 then hook #2 each emitting a parameters map.
        after_hook_1 = merge_payload({}, {"parameters": {"A": "1"}})
        after_hook_2 = merge_payload(after_hook_1, {"parameters": {"B": "2"}})
        assert after_hook_2["parameters"] == {"A": "1", "B": "2"}

    def test_merge_parameters_later_overrides_same_key(self):
        """On a key conflict, the later hook wins."""
        after_hook_1 = merge_payload({}, {"parameters": {"A": "1"}})
        after_hook_2 = merge_payload(after_hook_1, {"parameters": {"A": "2"}})
        assert after_hook_2["parameters"] == {"A": "2"}


class TestHookManager:
    """Tests for HookManager."""

    def test_load_hooks_no_file(self):
        """Test loading when no hooks file exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = HookManager(tmpdir, print)
            hooks = manager.load_hooks()
            assert hooks is None

    def test_load_hooks_yaml(self):
        """Test loading hooks from YAML file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump({"preSubmission": [{"command": "python", "args": ["-c", "pass"]}]}, f)
            manager = HookManager(tmpdir, print)
            hooks = manager.load_hooks()
            assert hooks is not None
            assert len(hooks.pre_submission) == 1
            assert hooks.pre_submission[0].command == "python"

    def test_load_hooks_json(self):
        """Test loading hooks from JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.json")
            with open(hooks_file, "w") as f:
                json.dump({"postSubmission": [{"command": "echo", "args": ["done"]}]}, f)
            manager = HookManager(tmpdir, print)
            hooks = manager.load_hooks()
            assert hooks is not None
            assert len(hooks.post_submission) == 1

    def test_load_hooks_yaml_precedence(self):
        """Test error when both YAML and JSON exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yaml_file = os.path.join(tmpdir, "hooks.yaml")
            json_file = os.path.join(tmpdir, "hooks.json")
            with open(yaml_file, "w") as f:
                yaml.dump({"preSubmission": [{"command": "from_yaml"}]}, f)
            with open(json_file, "w") as f:
                json.dump({"preSubmission": [{"command": "from_json"}]}, f)
            manager = HookManager(tmpdir, print)
            with pytest.raises(DeadlineOperationError, match="both hooks.json and hooks.yaml"):
                manager.load_hooks()

    def test_execute_pre_submission_hooks_success(self):
        """Test successful pre-submission hook execution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {"preSubmission": [{"command": sys.executable, "args": ["-c", "pass"]}]}, f
                )

            messages: List[str] = []
            manager = HookManager(tmpdir, messages.append)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={"priority": 50},
            )
            result = manager.execute_pre_submission_hooks(metadata, {"priority": 50})
            assert result == {"priority": 50}
            assert any("Running pre-submission hook" in m for m in messages)

    def test_execute_pre_submission_hooks_modifies_payload(self):
        """Test pre-submission hook can modify payload."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": ["-c", 'import json; print(json.dumps({"priority": 100}))'],
                            }
                        ]
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={"priority": 50},
            )
            result = manager.execute_pre_submission_hooks(metadata, {"priority": 50})
            assert result["priority"] == 100

    def test_execute_pre_submission_hooks_failure_blocks(self):
        """Test failed pre-submission hook blocks submission."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {"preSubmission": [{"command": sys.executable, "args": ["-c", "exit(1)"]}]}, f
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            with pytest.raises(DeadlineOperationError, match="failed with exit code 1"):
                manager.execute_pre_submission_hooks(metadata, {})

    def test_execute_pre_submission_hooks_timeout(self):
        """Test pre-submission hook timeout."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": ["-c", "import time; time.sleep(10)"],
                                "timeout": 1,
                            }
                        ]
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            with pytest.raises(DeadlineOperationError, match="timed out"):
                manager.execute_pre_submission_hooks(metadata, {})

    def test_execute_post_submission_hooks_failure_warns(self):
        """Test failed post-submission hook only warns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {"postSubmission": [{"command": sys.executable, "args": ["-c", "exit(1)"]}]}, f
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
                job_id="job-123",
            )
            # Should not raise
            manager.execute_post_submission_hooks(metadata)

    def test_execute_hooks_receives_env_vars(self):
        """Test hooks receive environment variables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            output_file = os.path.join(tmpdir, "output.txt")
            # Escape backslashes for Windows paths in Python code string
            escaped_output = output_file.replace("\\", "\\\\")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    f"import os; open('{escaped_output}', 'w').write(os.environ.get('DEADLINE_JOB_NAME', ''))",
                                ],
                            }
                        ]
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="MyTestJob",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            manager.execute_pre_submission_hooks(metadata, {})

            with open(output_file) as f:
                assert f.read() == "MyTestJob"

    def test_execute_hooks_receives_custom_env(self):
        """Test hooks receive custom environment variables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            output_file = os.path.join(tmpdir, "output.txt")
            # Escape backslashes for Windows paths in Python code string
            escaped_output = output_file.replace("\\", "\\\\")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    f"import os; open('{escaped_output}', 'w').write(os.environ.get('CUSTOM_VAR', ''))",
                                ],
                                "env": {"CUSTOM_VAR": "custom_value"},
                            }
                        ]
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            manager.execute_pre_submission_hooks(metadata, {})

            with open(output_file) as f:
                assert f.read() == "custom_value"

    def test_execute_hooks_receives_stdin_json(self):
        """Test hooks receive metadata via stdin."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            output_file = os.path.join(tmpdir, "output.txt")
            # Escape backslashes for Windows paths in Python code string
            escaped_output = output_file.replace("\\", "\\\\")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    f"import sys, json; d = json.load(sys.stdin); open('{escaped_output}', 'w').write(d['jobName'])",
                                ],
                            }
                        ]
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="StdinTestJob",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            manager.execute_pre_submission_hooks(metadata, {})

            with open(output_file) as f:
                assert f.read() == "StdinTestJob"

    def test_hooks_origin_file_resolution(self):
        """Test that .hooks_origin file is used for script resolution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create original bundle dir with script
            original_dir = os.path.join(tmpdir, "original")
            os.makedirs(original_dir)
            script_file = os.path.join(original_dir, "myscript.py")
            with open(script_file, "w") as f:
                f.write("pass")  # No output to avoid JSON parsing

            # Create job history bundle dir with hooks.yaml and .hooks_origin
            history_dir = os.path.join(tmpdir, "history")
            os.makedirs(history_dir)
            hooks_file = os.path.join(history_dir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {"preSubmission": [{"command": sys.executable, "args": ["myscript.py"]}]}, f
                )

            # Write .hooks_origin pointing to original dir
            with open(os.path.join(history_dir, ".hooks_origin"), "w") as f:
                f.write(original_dir)

            manager = HookManager(history_dir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=history_dir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            # Should resolve myscript.py from original_dir
            manager.execute_pre_submission_hooks(metadata, {})

    def test_command_not_found_error(self):
        """Test error when hook command is not found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump({"preSubmission": [{"command": "nonexistent_command_xyz"}]}, f)

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            with pytest.raises(DeadlineOperationError, match="not found"):
                manager.execute_pre_submission_hooks(metadata, {})

    def test_absolute_command_path(self):
        """Test hook with absolute command path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {"preSubmission": [{"command": sys.executable, "args": ["-c", "pass"]}]}, f
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            manager.execute_pre_submission_hooks(metadata, {})

    def test_post_submission_hook_timeout_warns(self):
        """Test post-submission hook timeout only warns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "postSubmission": [
                            {
                                "command": sys.executable,
                                "args": ["-c", "import time; time.sleep(5)"],
                                "timeout": 1,
                            }
                        ]
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
                job_id="job-123",
            )
            # Should not raise, just warn
            manager.execute_post_submission_hooks(metadata)

    def test_post_submission_hook_with_output(self):
        """Test post-submission hook stdout is logged."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "postSubmission": [
                            {"command": sys.executable, "args": ["-c", "print('success')"]}
                        ]
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
                job_id="job-123",
            )
            manager.execute_post_submission_hooks(metadata)

    def test_confirmation_message_generation(self):
        """Test hook confirmation message generation."""
        from deadline.client.job_bundle._hooks import _generate_hooks_confirmation_message

        hooks = HookConfiguration(
            version="1.0",
            pre_gui=[HookDefinition(command="python", args=["prefill.py"])],
            pre_submission=[HookDefinition(command="python", args=["validate.py"])],
            post_submission=[HookDefinition(command="bash", args=["notify.sh"])],
        )
        message = _generate_hooks_confirmation_message(hooks, "/path/to/bundle")
        assert "Pre-GUI hooks:" in message
        assert "python prefill.py" in message
        assert "Pre-submission hooks:" in message
        assert "python validate.py" in message
        assert "Post-submission hooks:" in message
        assert "bash notify.sh" in message
        assert "/path/to/bundle" in message
        # Defaults to the job-bundle wording so existing callers are unchanged.
        assert "This job bundle contains submission hooks" in message

    def test_confirmation_message_labels_environment_source(self):
        """An environment (DEADLINE_HOOKS_DIR) source is identified as such — not shown as if
        it came from the job bundle — so the consent prompt reflects the true hook origin."""
        from deadline.client.job_bundle._hooks import _generate_hooks_confirmation_message

        hooks = HookConfiguration(
            version="1.0",
            pre_gui=[],
            pre_submission=[HookDefinition(command="python", args=["validate.py"])],
            post_submission=[],
        )
        message = _generate_hooks_confirmation_message(
            hooks, "/studio/hooks", "environment (DEADLINE_HOOKS_DIR)"
        )
        assert "This environment (DEADLINE_HOOKS_DIR) contains submission hooks" in message
        assert "Location: /studio/hooks" in message
        # Must not masquerade as a job-bundle source.
        assert "This job bundle contains" not in message

    def test_post_hooks_not_called_on_create_job_failure(self):
        """Test that post-submission hooks are not executed when CreateJob fails.

        When CreateJob raises an exception, the caller should not invoke
        execute_post_submission_hooks. This test verifies that post hooks
        have no side effects when not called.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a hook that writes a marker file
            marker = os.path.join(tmpdir, "post_hook_ran")
            marker_escaped = marker.replace("\\", "\\\\")
            script = os.path.join(tmpdir, "marker.py")
            with open(script, "w") as f:
                f.write(f"open('{marker_escaped}', 'w').write('ran')")

            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "version": "1.0",
                        "postSubmission": [{"command": sys.executable, "args": [script]}],
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            # Simulate CreateJob failure: don't call execute_post_submission_hooks
            assert not os.path.exists(marker)

    def test_post_hooks_run_after_successful_create_job(self):
        """Test that post-submission hooks execute after CreateJob succeeds.

        Post hooks run after the CreateJob API returns successfully. The job
        may still fail async validation, but post hooks should still run since
        the API call itself succeeded.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            marker = os.path.join(tmpdir, "post_hook_ran")
            marker_escaped = marker.replace("\\", "\\\\")
            script = os.path.join(tmpdir, "marker.py")
            with open(script, "w") as f:
                f.write(f"open('{marker_escaped}', 'w').write('ran')")

            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "version": "1.0",
                        "postSubmission": [{"command": sys.executable, "args": [script]}],
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()

            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=tmpdir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
                job_id="job-789",
            )
            manager.execute_post_submission_hooks(metadata)
            assert os.path.exists(marker)

    def test_pre_submission_hook_preserves_job_bundle_dir(self):
        """Test that pre-submission hooks receive the metadata's job_bundle_dir, not the hooks dir."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create separate hooks dir and bundle dir
            hooks_dir = os.path.join(tmpdir, "hooks")
            bundle_dir = os.path.join(tmpdir, "bundle")
            os.makedirs(hooks_dir)
            os.makedirs(bundle_dir)

            output_file = os.path.join(tmpdir, "output.txt")
            escaped_output = output_file.replace("\\", "\\\\")

            hooks_file = os.path.join(hooks_dir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    f"import sys, json; d = json.load(sys.stdin); open('{escaped_output}', 'w').write(d['jobBundleDir'])",
                                ],
                            }
                        ]
                    },
                    f,
                )

            # HookManager is created with hooks_dir (simulating environment hooks)
            manager = HookManager(hooks_dir, lambda x: None)
            manager.load_hooks()

            # Metadata has the actual bundle dir
            metadata = HookMetadata(
                job_name="Test",
                priority=50,
                farm_id="farm-123",
                queue_id="queue-456",
                job_bundle_dir=bundle_dir,
                parameters={},
                submitter_name="Test",
                asset_references={},
                submission_payload={},
            )
            manager.execute_pre_submission_hooks(metadata, {})

            with open(output_file) as f:
                # Hook should receive the bundle_dir, not the hooks_dir
                assert f.read() == bundle_dir


class TestHookStdoutStreaming:
    """Tests that a hook's stderr is surfaced to the user while the hook runs.

    Bea-57642: submission hooks previously produced no feedback until they finished, so a
    slow hook (e.g. generating auth tokens for several services) looked like a hang. Hooks
    write human-readable progress to stderr (stdout is reserved for the JSON contract), and
    the executor now forwards each stderr line to ``print_callback`` as it arrives.
    """

    def _make_metadata(self, tmpdir: str) -> HookMetadata:
        return HookMetadata(
            job_name="Test",
            priority=50,
            farm_id="farm-123",
            queue_id="queue-456",
            job_bundle_dir=tmpdir,
            parameters={},
            submitter_name="Test",
            asset_references={},
            submission_payload={},
        )

    @staticmethod
    def _streamed_lines(messages: List[str]) -> List[str]:
        """The subset of callback messages that are streamed hook output.

        Streamed hook output carries the ``  [<hook_type> hook <index>] `` prefix added by
        HookExecutor. This lets a test distinguish real streamed stderr from the
        ``Running ... hook`` / failure-report lines, which echo the hook's command and can
        incidentally contain the same text the hook printed.
        """
        return [m for m in messages if m.lstrip().startswith("[")]

    def test_pre_submission_stderr_is_forwarded_to_callback(self):
        """Each line a hook writes to stderr is streamed to print_callback."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    "import sys; print('step one', file=sys.stderr); "
                                    + "print('step two', file=sys.stderr)",
                                ],
                            }
                        ]
                    },
                    f,
                )

            messages: List[str] = []
            manager = HookManager(tmpdir, messages.append)
            manager.load_hooks()
            manager.execute_pre_submission_hooks(self._make_metadata(tmpdir), {})

            streamed = "\n".join(self._streamed_lines(messages))
            assert "step one" in streamed
            assert "step two" in streamed

    def test_stdout_json_is_not_streamed_as_progress(self):
        """stdout is the JSON contract, so it is consumed for the payload and not echoed to
        the user as progress output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    "import json; print(json.dumps({'priority': 100}))",
                                ],
                            }
                        ]
                    },
                    f,
                )

            messages: List[str] = []
            manager = HookManager(tmpdir, messages.append)
            manager.load_hooks()
            result = manager.execute_pre_submission_hooks(self._make_metadata(tmpdir), {})

            # The JSON reached the payload...
            assert result["priority"] == 100
            # ...but was not streamed back to the user as a progress line (stdout is the
            # JSON contract, not progress output).
            assert self._streamed_lines(messages) == []

    def test_stderr_streamed_incrementally_before_hook_exits(self):
        """A progress line reaches the callback before the hook finishes, not just after.

        The hook writes one stderr line, then blocks on stdin until we feed it. Because the
        executor writes stdin and reads stderr on separate threads, the first progress line
        is delivered while the hook is still running.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    # Emit progress, then read stdin (the metadata) to prove
                                    # the reader thread saw the line before we blocked here.
                                    "import sys; print('started', file=sys.stderr, flush=True); "
                                    + "sys.stdin.read()",
                                ],
                            }
                        ]
                    },
                    f,
                )

            first_message = threading.Event()

            def _callback(msg: str) -> None:
                if "started" in msg:
                    first_message.set()

            manager = HookManager(tmpdir, _callback)
            manager.load_hooks()
            manager.execute_pre_submission_hooks(self._make_metadata(tmpdir), {})

            assert first_message.is_set()

    def test_failure_report_does_not_duplicate_streamed_stderr(self):
        """On failure, stderr already streamed live is not re-dumped as a blob, but stdout
        (which is not streamed) is surfaced for debugging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    "import sys; print('progress line', file=sys.stderr); "
                                    + "print('not-json-stdout'); sys.exit(2)",
                                ],
                            }
                        ]
                    },
                    f,
                )

            messages: List[str] = []
            manager = HookManager(tmpdir, messages.append)
            manager.load_hooks()
            with pytest.raises(DeadlineOperationError, match="exit code 2"):
                manager.execute_pre_submission_hooks(self._make_metadata(tmpdir), {})

            # The stderr progress line was streamed exactly once during execution...
            assert sum("progress line" in m for m in self._streamed_lines(messages)) == 1
            # ...and is not repeated as a "stderr:\n..." blob in the failure report.
            assert not any(m.startswith("stderr:") for m in messages)
            # stdout is not streamed, so the failure report surfaces it for debugging.
            assert any(m.startswith("stdout:") and "not-json-stdout" in m for m in messages)

    @staticmethod
    def _reap(pidfile: str) -> None:
        """Kill the grandchild whose PID a lingering-pipe hook wrote to ``pidfile``.

        The grandchild is detached from the hook process, so the test owns cleanup: without
        this it would keep the stderr pipe open (and the reader thread blocked) until its own
        backstop timeout, leaking a process and thread into the rest of the CI run.
        """
        try:
            with open(pidfile) as f:
                pid = int(f.read().strip())
        except (OSError, ValueError):
            return
        try:
            if sys.platform == "win32":
                subprocess.run(
                    ["taskkill", "/F", "/PID", str(pid)],
                    capture_output=True,
                    check=False,
                )
            else:
                os.kill(pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            # Best-effort cleanup: the grandchild may already have exited (its own backstop
            # sleep elapsed) or never started, so a failure to signal it is fine to ignore.
            pass

    def test_lingering_child_holding_pipe_does_not_hang(self, monkeypatch):
        """A hook that exits but leaves a child holding the stderr pipe open must not hang
        submission. process.wait() returns (the hook itself exited), but the reader threads
        never see EOF; the bounded join must give up after the grace period and report a
        timeout instead of blocking forever.
        """
        # Keep the test fast: shrink the reader-join grace window.
        monkeypatch.setattr(HookExecutor, "_READER_JOIN_GRACE_SECONDS", 0.5)

        with tempfile.TemporaryDirectory() as tmpdir:
            # The hook spawns a detached child that inherits stderr, records its PID so the
            # test can reap it, then the hook process itself exits. The child keeps the
            # stderr write end open past the parent's exit, so the drainer never reaches EOF
            # on its own. The child's own short sleep is only a backstop in case cleanup is
            # skipped — the test kills it explicitly so nothing lingers into later tests.
            pidfile = os.path.join(tmpdir, "grandchild.pid")
            child = (
                "import subprocess, sys; "
                "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(5)'], "
                "stderr=sys.stderr); "
                f"open({pidfile!r}, 'w').write(str(p.pid)); "
                "sys.exit(0)"
            )
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preSubmission": [
                            {"command": sys.executable, "args": ["-c", child], "timeout": 30}
                        ]
                    },
                    f,
                )

            manager = HookManager(tmpdir, lambda _msg: None)
            manager.load_hooks()

            start = time.monotonic()
            try:
                # A blocks-forever regression would hang here; the bounded join instead
                # surfaces a timeout well within the 30s hook timeout.
                with pytest.raises(DeadlineOperationError, match="timed out"):
                    manager.execute_pre_submission_hooks(self._make_metadata(tmpdir), {})
                elapsed = time.monotonic() - start
                assert elapsed < 10, f"submission hung on a lingering pipe holder ({elapsed:.1f}s)"
            finally:
                self._reap(pidfile)

    def test_abandoned_reader_does_not_call_callback_after_return(self, monkeypatch):
        """A leaked reader thread (lingering-child timeout path) must not keep calling
        print_callback or mutating the output buffers after execute() returns — that would
        race the next hook's output and the main thread's "".join of the buffers. The
        ``abandoned`` flag makes the leaked reader bow out; assert no callback fires once we
        record the method as returned.
        """
        monkeypatch.setattr(HookExecutor, "_READER_JOIN_GRACE_SECONDS", 0.5)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Child inherits stderr, records its PID for cleanup, and keeps emitting lines
            # past the grace period so the drainer is still active when execute() returns.
            pidfile = os.path.join(tmpdir, "grandchild.pid")
            grandchild_body = (
                "import sys, time\n"
                "for i in range(100):\n"
                "    print('leaked', i, file=sys.stderr, flush=True)\n"
                "    time.sleep(0.02)\n"
            )
            child = (
                "import subprocess, sys; "
                f"p = subprocess.Popen([sys.executable, '-c', {grandchild_body!r}], "
                "stderr=sys.stderr); "
                f"open({pidfile!r}, 'w').write(str(p.pid)); "
                "sys.exit(0)"
            )

            returned = threading.Event()
            calls_after_return = []

            def _callback(msg):
                if returned.is_set():
                    calls_after_return.append(msg)

            executor = HookExecutor(tmpdir, _callback)
            hook = HookDefinition(command=sys.executable, args=["-c", child], timeout=30)

            try:
                result = executor.execute(hook, self._make_metadata(tmpdir), "pre-submission", 1)
                returned.set()
                assert result.timed_out is True

                # Give the leaked child time to emit more lines; the abandoned reader must
                # have stopped forwarding them to the callback.
                time.sleep(0.5)
                assert calls_after_return == [], (
                    "leaked reader kept calling print_callback after execute() returned: "
                    f"{calls_after_return[:3]}"
                )
            finally:
                self._reap(pidfile)


class TestValidateBeforeGUIOutput:
    """Tests for pre-GUI hook output validation."""

    def test_valid_empty(self):
        validate_pre_gui_output({}, "hook")

    def test_valid_all_fields(self):
        validate_pre_gui_output(
            {
                "name": "My Job",
                "description": "desc",
                "parameters": {"deadline:priority": 75, "k": "v"},
            },
            "hook",
        )

    def test_invalid_not_dict(self):
        with pytest.raises(DeadlineOperationError, match="must be a JSON object"):
            validate_pre_gui_output("string", "hook")  # type: ignore[arg-type]

    def test_invalid_unknown_field(self):
        with pytest.raises(DeadlineOperationError, match="unrecognised fields"):
            validate_pre_gui_output({"farmId": "farm-123"}, "hook")

    def test_invalid_parameters_not_dict(self):
        with pytest.raises(DeadlineOperationError, match="'parameters' must be an object"):
            validate_pre_gui_output({"parameters": ["list"]}, "hook")

    def test_invalid_unknown_field_priority(self):
        with pytest.raises(DeadlineOperationError, match="unrecognised fields"):
            validate_pre_gui_output({"priority": 75}, "hook")

    def test_invalid_name_not_string(self):
        with pytest.raises(DeadlineOperationError, match="'name' must be a string"):
            validate_pre_gui_output({"name": 123}, "hook")

    def test_invalid_description_not_string(self):
        with pytest.raises(DeadlineOperationError, match="'description' must be a string"):
            validate_pre_gui_output({"description": []}, "hook")


class TestHookConfigurationBeforeGUI:
    """Tests for preGUI in HookConfiguration."""

    def test_from_dict_pre_gui(self):
        data = {"preGUI": [{"command": "prefill.py"}]}
        config = HookConfiguration.from_dict(data)
        assert len(config.pre_gui) == 1
        assert config.pre_gui[0].command == "prefill.py"

    def test_from_dict_all_phases(self):
        data = {
            "preGUI": [{"command": "prefill.py"}],
            "preSubmission": [{"command": "validate.py"}],
            "postSubmission": [{"command": "notify.py"}],
        }
        config = HookConfiguration.from_dict(data)
        assert len(config.pre_gui) == 1
        assert len(config.pre_submission) == 1
        assert len(config.post_submission) == 1


class TestValidateConfigurationBeforeGUI:
    """Tests for preGUI phase in validate_configuration."""

    def test_valid_pre_gui(self):
        validate_configuration({"preGUI": [{"command": "prefill.py"}]})

    def test_invalid_pre_gui_not_list(self):
        with pytest.raises(DeadlineOperationError, match="must be a list"):
            validate_configuration({"preGUI": "not a list"})

    def test_invalid_pre_gui_hook_missing_command(self):
        with pytest.raises(DeadlineOperationError, match="missing required 'command'"):
            validate_configuration({"preGUI": [{"args": []}]})


class TestExecuteBeforeGUIHooks:
    """Tests for HookManager.execute_pre_gui_hooks."""

    def _make_metadata(self, tmpdir: str) -> HookMetadata:
        return HookMetadata(
            job_name="Test",
            priority=50,
            farm_id="farm-123",
            queue_id="queue-456",
            job_bundle_dir=tmpdir,
            parameters={},
            submitter_name="Test",
            asset_references={},
            submission_payload={},
        )

    def test_no_pre_gui_hooks_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {"preSubmission": [{"command": sys.executable, "args": ["-c", "pass"]}]}, f
                )
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            result = manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))
            assert result == {}

    def test_returns_merged_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            output = {
                "name": "Prefilled Job",
                "parameters": {"deadline:priority": 80, "Foo": "bar"},
            }
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preGUI": [
                            {
                                "command": sys.executable,
                                "args": ["-c", f"import json; print(json.dumps({output!r}))"],
                            }
                        ]
                    },
                    f,
                )
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            result = manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))
            assert result["name"] == "Prefilled Job"
            assert result["parameters"] == {"deadline:priority": 80, "Foo": "bar"}

    def test_metadata_job_bundle_dir_is_respected(self):
        """execute_pre_gui_hooks must NOT override metadata.job_bundle_dir with the
        manager's own directory. The hook should receive the job_bundle_dir the caller set
        (the real bundle), even when the hooks live in a different directory (e.g. an
        environment DEADLINE_HOOKS_DIR). Relative script paths still resolve against the
        manager's directory."""
        with (
            tempfile.TemporaryDirectory() as hooks_dir,
            tempfile.TemporaryDirectory() as bundle_dir,
        ):
            hooks_file = os.path.join(hooks_dir, "hooks.yaml")
            # The hook echoes the DEADLINE_JOB_BUNDLE_DIR it was given as the job name.
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preGUI": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    "import os, json; print(json.dumps("
                                    + "{'name': os.environ['DEADLINE_JOB_BUNDLE_DIR']}))",
                                ],
                            }
                        ]
                    },
                    f,
                )
            manager = HookManager(hooks_dir, lambda x: None)
            manager.load_hooks()
            # Caller sets job_bundle_dir to the real bundle, distinct from hooks_dir.
            result = manager.execute_pre_gui_hooks(self._make_metadata(bundle_dir))
            assert result["name"] == bundle_dir  # not hooks_dir

    def test_later_hook_overrides_scalar(self):
        """Later hooks override earlier hooks for scalar fields like name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preGUI": [
                            {
                                "command": sys.executable,
                                "args": ["-c", 'import json; print(json.dumps({"name": "first"}))'],
                            },
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    'import json; print(json.dumps({"name": "second"}))',
                                ],
                            },
                        ]
                    },
                    f,
                )
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            result = manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))
            assert result["name"] == "second"

    def test_parameters_merged_across_hooks(self):
        """Parameters from multiple hooks are merged (not replaced)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preGUI": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    'import json; print(json.dumps({"parameters": {"A": "1"}}))',
                                ],
                            },
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    'import json; print(json.dumps({"parameters": {"B": "2"}}))',
                                ],
                            },
                        ]
                    },
                    f,
                )
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            result = manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))
            assert result["parameters"] == {"A": "1", "B": "2"}

    def test_failure_blocks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump({"preGUI": [{"command": sys.executable, "args": ["-c", "exit(1)"]}]}, f)
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            with pytest.raises(DeadlineOperationError, match="failed with exit code 1"):
                manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))

    def test_timeout_blocks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preGUI": [
                            {
                                "command": sys.executable,
                                "args": ["-c", "import time; time.sleep(10)"],
                                "timeout": 1,
                            }
                        ]
                    },
                    f,
                )
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            with pytest.raises(DeadlineOperationError, match="timed out"):
                manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))

    def test_invalid_json_output_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {"preGUI": [{"command": sys.executable, "args": ["-c", "print('not json')"]}]},
                    f,
                )
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            with pytest.raises(DeadlineOperationError, match="invalid JSON"):
                manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))

    def test_no_output_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump({"preGUI": [{"command": sys.executable, "args": ["-c", "pass"]}]}, f)
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            result = manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))
            assert result == {}

    def test_deadline_prefix_params_go_to_shared_values(self):
        """deadline: params land in initial_shared_parameter_values, not initial_settings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hooks_file = os.path.join(tmpdir, "hooks.yaml")
            with open(hooks_file, "w") as f:
                yaml.dump(
                    {
                        "preGUI": [
                            {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    'import json; print(json.dumps({"parameters": {"deadline:priority": 80}}))',
                                ],
                            }
                        ]
                    },
                    f,
                )
            manager = HookManager(tmpdir, lambda x: None)
            manager.load_hooks()
            result = manager.execute_pre_gui_hooks(self._make_metadata(tmpdir))
            assert result["parameters"]["deadline:priority"] == 80

    def test_confirmation_message_includes_pre_gui(self):
        from deadline.client.job_bundle._hooks import _generate_hooks_confirmation_message

        hooks = HookConfiguration(
            version="1.0",
            pre_gui=[HookDefinition(command="python", args=["prefill.py"])],
            pre_submission=[],
            post_submission=[],
        )
        message = _generate_hooks_confirmation_message(hooks, "/bundle")
        assert "Pre-GUI hooks:" in message
        assert "python prefill.py" in message


_PARAM_MOCK_FARM_ID = "farm-0123456789abcdef0123456789abcdef"
_PARAM_MOCK_QUEUE_ID = "queue-0123456789abcdef0123456789abcdef"

_PARAM_TEMPLATE = """specificationVersion: 'jobtemplate-2023-09'
name: ParamHookTest
parameterDefinitions:
- name: Foo
  type: STRING
  default: original_value
steps:
- name: StepOriginal
  script:
    actions:
      onRun:
        command: echo
"""


def _write_param_bundle(bundle_dir):
    with open(os.path.join(bundle_dir, "template.yaml"), "w", encoding="utf8") as f:
        f.write(_PARAM_TEMPLATE)
    with open(os.path.join(bundle_dir, "parameter_values.yaml"), "w", encoding="utf8") as f:
        f.write("parameterValues:\n- name: Foo\n  value: original_value\n")


def _foo_value_sent_to_create_job(mock):
    kwargs = mock.get_boto3_client().create_job.call_args.kwargs
    params = kwargs.get("parameters")
    if isinstance(params, list):
        for p in params:
            if p.get("name") == "Foo":
                return p.get("value")
    elif isinstance(params, dict) and "Foo" in params:
        return next(iter(params["Foo"].values()))
    return None


class TestPreSubmissionHooks:
    """Tests for pre-submission hooks.

    A pre-submission hook changing a parameter value should reach CreateJob via both
    channels: rewriting parameter_values.yaml on disk, and emitting a ``parameters`` map on
    stdout. A template edit on disk is also honored (template test).
    """

    def _configure(self):
        config.set_setting("defaults.farm_id", _PARAM_MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", _PARAM_MOCK_QUEUE_ID)
        config.set_setting("settings.allow_bundle_hooks", "true")
        config.set_setting("settings.auto_accept", "true")

    def test_ondisk_param_change_is_respected(self, fresh_deadline_config, tmp_path):
        """A hook that rewrites parameter_values.yaml on disk should change the value
        sent to CreateJob."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._configure()
        _write_param_bundle(bundle)
        with open(os.path.join(bundle, "rewrite.py"), "w", encoding="utf8") as f:
            f.write(
                "import os\n"
                "b = os.environ['DEADLINE_JOB_BUNDLE_DIR']\n"
                "open(os.path.join(b, 'parameter_values.yaml'), 'w').write("
                "'parameterValues:\\n- name: Foo\\n  value: CHANGED_ON_DISK\\n')\n"
            )
        with open(os.path.join(bundle, "hooks.yaml"), "w", encoding="utf8") as f:
            f.write(
                "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [rewrite.py]\n"
            )

        with patch_calls_for_create_job_from_job_bundle() as mock:
            api.create_job_from_job_bundle(job_bundle_dir=bundle, queue_parameter_definitions=[])
            foo = _foo_value_sent_to_create_job(mock)

        assert foo == "CHANGED_ON_DISK"

    def test_stdout_parameters_are_respected(self, fresh_deadline_config, tmp_path):
        """A hook emitting {"parameters": {"Foo": "..."}} on stdout should change the value
        sent to CreateJob."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._configure()
        _write_param_bundle(bundle)
        with open(os.path.join(bundle, "emit.py"), "w", encoding="utf8") as f:
            f.write(
                "import json\nprint(json.dumps({'parameters': {'Foo': 'CHANGED_VIA_STDOUT'}}))\n"
            )
        with open(os.path.join(bundle, "hooks.yaml"), "w", encoding="utf8") as f:
            f.write("version: '1.0'\npreSubmission:\n  - command: python3\n    args: [emit.py]\n")

        with patch_calls_for_create_job_from_job_bundle() as mock:
            api.create_job_from_job_bundle(job_bundle_dir=bundle, queue_parameter_definitions=[])
            foo = _foo_value_sent_to_create_job(mock)

        assert foo == "CHANGED_VIA_STDOUT"

    def test_ondisk_template_change_is_respected(self, fresh_deadline_config, tmp_path):
        """An on-disk TEMPLATE edit IS honored (template re-read from disk after hooks).
        Confirms the template-vs-parameter difference and that the hook ran."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._configure()
        _write_param_bundle(bundle)
        new_template = _PARAM_TEMPLATE + (
            "- name: StepAddedByHook\n"
            "  script:\n"
            "    actions:\n"
            "      onRun:\n"
            "        command: echo\n"
        )
        with open(os.path.join(bundle, "addstep.py"), "w", encoding="utf8") as f:
            f.write(
                "import os\n"
                "b = os.environ['DEADLINE_JOB_BUNDLE_DIR']\n"
                f"open(os.path.join(b, 'template.yaml'), 'w').write({new_template!r})\n"
            )
        with open(os.path.join(bundle, "hooks.yaml"), "w", encoding="utf8") as f:
            f.write(
                "version: '1.0'\npreSubmission:\n  - command: python3\n    args: [addstep.py]\n"
            )

        with patch_calls_for_create_job_from_job_bundle() as mock:
            api.create_job_from_job_bundle(job_bundle_dir=bundle, queue_parameter_definitions=[])
            template_sent = mock.get_boto3_client().create_job.call_args.kwargs.get("template", "")

        assert "StepAddedByHook" in template_sent

    def test_stdout_relative_path_parameter_is_rejected(self, fresh_deadline_config, tmp_path):
        """A hook emitting a relative PATH value on stdout is rejected — a hook does not run
        from the submitting working directory, so a relative PATH would be ambiguous. Hooks
        must emit absolute paths (or rewrite parameter_values.yaml on disk)."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._configure()
        template = (
            "specificationVersion: 'jobtemplate-2023-09'\n"
            "name: PathHookTest\n"
            "parameterDefinitions:\n"
            "- name: ScenePath\n"
            "  type: PATH\n"
            "  dataFlow: NONE\n"
            "  default: placeholder.ma\n"
            "steps:\n"
            "- name: StepOriginal\n"
            "  script:\n"
            "    actions:\n"
            "      onRun:\n"
            "        command: echo\n"
        )
        with open(os.path.join(bundle, "template.yaml"), "w", encoding="utf8") as f:
            f.write(template)
        with open(os.path.join(bundle, "emit.py"), "w", encoding="utf8") as f:
            f.write(
                "import json\n"
                "print(json.dumps({'parameters': {'ScenePath': 'relative/scene.ma'}}))\n"
            )
        with open(os.path.join(bundle, "hooks.yaml"), "w", encoding="utf8") as f:
            f.write("version: '1.0'\npreSubmission:\n  - command: python3\n    args: [emit.py]\n")

        with patch_calls_for_create_job_from_job_bundle():
            with pytest.raises(DeadlineOperationError, match="relative PATH"):
                api.create_job_from_job_bundle(
                    job_bundle_dir=bundle, queue_parameter_definitions=[]
                )

    def test_stdout_absolute_path_parameter_is_respected(self, fresh_deadline_config, tmp_path):
        """An absolute PATH value emitted on stdout is accepted and reaches CreateJob."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._configure()
        abs_scene = os.path.abspath(os.path.join(str(tmp_path), "scene.ma"))
        template = (
            "specificationVersion: 'jobtemplate-2023-09'\n"
            "name: PathHookTest\n"
            "parameterDefinitions:\n"
            "- name: ScenePath\n"
            "  type: PATH\n"
            "  dataFlow: NONE\n"
            "  default: placeholder.ma\n"
            "steps:\n"
            "- name: StepOriginal\n"
            "  script:\n"
            "    actions:\n"
            "      onRun:\n"
            "        command: echo\n"
        )
        with open(os.path.join(bundle, "template.yaml"), "w", encoding="utf8") as f:
            f.write(template)
        with open(os.path.join(bundle, "emit.py"), "w", encoding="utf8") as f:
            f.write(
                "import json\n"
                f"print(json.dumps({{'parameters': {{'ScenePath': {abs_scene!r}}}}}))\n"
            )
        with open(os.path.join(bundle, "hooks.yaml"), "w", encoding="utf8") as f:
            f.write("version: '1.0'\npreSubmission:\n  - command: python3\n    args: [emit.py]\n")

        with patch_calls_for_create_job_from_job_bundle() as mock:
            api.create_job_from_job_bundle(job_bundle_dir=bundle, queue_parameter_definitions=[])
            kwargs = mock.get_boto3_client().create_job.call_args.kwargs
            params = kwargs.get("parameters")

        scene_value = None
        if isinstance(params, list):
            for p in params:
                if p.get("name") == "ScenePath":
                    scene_value = p.get("value")
        elif isinstance(params, dict) and "ScenePath" in params:
            scene_value = next(iter(params["ScenePath"].values()))
        assert scene_value == abs_scene


class TestEnvAndBundleSubmissionHooks:
    """Environment (DEADLINE_HOOKS_DIR) and bundle submission hooks must run together.

    Regression test for the bug where the pre/post-submission submit path collapsed both hook
    sources into a single HookManager and could only ever execute one of them — so environment
    and bundle hooks never ran together (and the merge branch was unreachable dead code).

    Source *selection* (single source, disabled sources, ordering, dedup) is covered without
    spawning subprocesses by ``TestCollectSubmissionHookSources``. This one end-to-end test
    covers what selection cannot: that both a DEADLINE_HOOKS_DIR source and a bundle source
    actually *execute* — for pre- and post-submission — in a single real submission, in order.
    """

    @staticmethod
    def _write_appending_hook(directory, marker, results_file):
        """Write a hooks.yaml into ``directory`` whose pre- and post-submission hooks each
        append a ``<marker>-pre`` / ``<marker>-post`` line to ``results_file``, so the caller
        can assert which sources ran, for which phase, and in what order.

        Uses ``sys.executable`` (not the ``python3`` literal): on Windows the ``python3`` name
        can resolve to the Microsoft Store app-execution-alias stub, which hangs when run
        non-interactively — leaving the hook subprocess (and the test worker) unable to exit.
        """
        script_name = f"{marker}_hook.py"
        escaped = results_file.replace("\\", "\\\\")
        with open(os.path.join(directory, script_name), "w", encoding="utf8") as f:
            f.write(
                f"import sys\nopen(r'{escaped}', 'a').write('{marker}-' + sys.argv[1] + '\\n')\n"
            )
        with open(os.path.join(directory, "hooks.yaml"), "w", encoding="utf8") as f:
            yaml.dump(
                {
                    "version": "1.0",
                    "preSubmission": [{"command": sys.executable, "args": [script_name, "pre"]}],
                    "postSubmission": [{"command": sys.executable, "args": [script_name, "post"]}],
                },
                f,
            )

    @staticmethod
    def _markers(results_file):
        if not os.path.exists(results_file):
            return []
        with open(results_file, encoding="utf8") as f:
            return [line.strip() for line in f if line.strip()]

    def test_env_and_bundle_hooks_both_run(self, fresh_deadline_config, tmp_path, monkeypatch):
        """Both a DEADLINE_HOOKS_DIR source and a bundle source execute their pre- and
        post-submission hooks, environment before bundle for each phase."""
        bundle = str(tmp_path / "bundle")
        studio = str(tmp_path / "studio")
        os.makedirs(bundle)
        os.makedirs(studio)
        config.set_setting("defaults.farm_id", _PARAM_MOCK_FARM_ID)
        config.set_setting("defaults.queue_id", _PARAM_MOCK_QUEUE_ID)
        config.set_setting("settings.allow_bundle_hooks", "true")
        config.set_setting("settings.allow_environment_hooks", "true")
        config.set_setting("settings.auto_accept", "true")
        _write_param_bundle(bundle)
        results = str(tmp_path / "results.txt")
        self._write_appending_hook(studio, "env", results)
        self._write_appending_hook(bundle, "bundle", results)
        monkeypatch.setenv("DEADLINE_HOOKS_DIR", studio)

        with patch_calls_for_create_job_from_job_bundle():
            api.create_job_from_job_bundle(job_bundle_dir=bundle, queue_parameter_definitions=[])

        # Pre-submission: env then bundle; post-submission: env then bundle.
        assert self._markers(results) == ["env-pre", "bundle-pre", "env-post", "bundle-post"]


class TestPreGuiHooks:
    """Tests for pre-GUI hooks."""

    @staticmethod
    def _write_pre_gui_hooks(directory):
        """Write a hooks.yaml with a single preGUI hook into ``directory``."""
        with open(os.path.join(directory, "hooks.yaml"), "w", encoding="utf8") as f:
            f.write("version: '1.0'\npreGUI:\n  - command: python3\n    args: [x.py]\n")

    def test_env_dir_pregui_hooks_are_collected(self, tmp_path):
        """A DEADLINE_HOOKS_DIR with preGUI hooks (and no bundle hooks) is collected as a
        source when environment hooks are allowed. This is the #2 fix — the loader must
        consult DEADLINE_HOOKS_DIR, not just the bundle dir."""
        bundle = str(tmp_path / "bundle")
        studio = str(tmp_path / "studio")
        os.makedirs(bundle)
        os.makedirs(studio)
        self._write_pre_gui_hooks(studio)  # bundle has no hooks.yaml

        sources = collect_pre_gui_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=studio,
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert [m.job_bundle_dir for m in sources] == [studio]

    def test_env_dir_ignored_when_env_hooks_disabled(self, tmp_path):
        """With environment hooks disabled, a DEADLINE_HOOKS_DIR preGUI hook is not run."""
        bundle = str(tmp_path / "bundle")
        studio = str(tmp_path / "studio")
        os.makedirs(bundle)
        os.makedirs(studio)
        self._write_pre_gui_hooks(studio)

        sources = collect_pre_gui_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=studio,
            allow_bundle_hooks=True,
            allow_environment_hooks=False,
            print_callback=lambda _msg: None,
        )

        assert sources == []

    def test_env_and_bundle_ordering(self, tmp_path):
        """When both sources have preGUI hooks and are enabled, environment hooks come
        first, then bundle hooks."""
        bundle = str(tmp_path / "bundle")
        studio = str(tmp_path / "studio")
        os.makedirs(bundle)
        os.makedirs(studio)
        self._write_pre_gui_hooks(bundle)
        self._write_pre_gui_hooks(studio)

        sources = collect_pre_gui_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=studio,
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert [m.job_bundle_dir for m in sources] == [studio, bundle]

    def test_bundle_hooks_ignored_when_bundle_hooks_disabled(self, tmp_path):
        """With bundle hooks disabled, a bundle preGUI hook is not run."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._write_pre_gui_hooks(bundle)

        sources = collect_pre_gui_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=None,
            allow_bundle_hooks=False,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert sources == []

    def test_empty_bundle_dir_uses_env_source_only(self, tmp_path):
        """A DCC caller passes bundle_dir="" (no on-disk bundle). Only the env source is
        collected; the empty bundle dir must not become a hook source."""
        studio = str(tmp_path / "studio")
        os.makedirs(studio)
        self._write_pre_gui_hooks(studio)

        sources = collect_pre_gui_hook_sources(
            bundle_dir="",
            env_hooks_dir=studio,
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert [m.job_bundle_dir for m in sources] == [studio]

    def test_empty_bundle_dir_does_not_load_hooks_from_cwd(self, tmp_path, monkeypatch):
        """Regression: bundle_dir="" must NOT resolve hooks.yaml relative to the process
        CWD. Otherwise a stray hooks file in a DCC's launch directory would be loaded (and,
        with bundle hooks enabled, executed) for a submission that has no bundle."""
        # Put a bundle hooks.yaml in the current working directory.
        self._write_pre_gui_hooks(str(tmp_path))
        monkeypatch.chdir(tmp_path)

        sources = collect_pre_gui_hook_sources(
            bundle_dir="",  # DCC: no bundle
            env_hooks_dir=None,
            allow_bundle_hooks=True,  # even with bundle hooks enabled...
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert sources == []  # ...the CWD hooks.yaml is not picked up

    def test_env_dir_equal_to_bundle_dir_is_not_duplicated(self, tmp_path):
        """If DEADLINE_HOOKS_DIR points at the job bundle, the shared hooks.yaml yields a
        single source (not two) so preGUI hooks do not run twice."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._write_pre_gui_hooks(bundle)

        sources = collect_pre_gui_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=bundle,  # same directory
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert [m.job_bundle_dir for m in sources] == [bundle]

    def test_env_dir_equal_to_bundle_dir_runs_when_only_env_hooks_enabled(self, tmp_path):
        """When env dir == bundle dir, enabling only environment hooks still permits the
        shared source to run (the single source is gated by either grant)."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._write_pre_gui_hooks(bundle)

        sources = collect_pre_gui_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=bundle,
            allow_bundle_hooks=False,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert [m.job_bundle_dir for m in sources] == [bundle]


class TestCollectSubmissionHookSources:
    """Source-selection tests for pre/post-submission hooks.

    The pre/post-submission analog of ``TestPreGuiHooks``. Both sources (environment and
    bundle) must be returnable together so submission hooks from both can run — the earlier
    single-manager merge could only ever run one.
    """

    @staticmethod
    def _write_submission_hooks(directory, phase="preSubmission"):
        """Write a hooks.yaml with a single ``phase`` hook into ``directory``."""
        with open(os.path.join(directory, "hooks.yaml"), "w", encoding="utf8") as f:
            f.write(f"version: '1.0'\n{phase}:\n  - command: python3\n    args: [x.py]\n")

    def test_env_and_bundle_ordering(self, tmp_path):
        """When both sources have submission hooks and are enabled, environment comes first,
        then bundle."""
        bundle = str(tmp_path / "bundle")
        studio = str(tmp_path / "studio")
        os.makedirs(bundle)
        os.makedirs(studio)
        self._write_submission_hooks(bundle)
        self._write_submission_hooks(studio)

        sources = collect_submission_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=studio,
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert [m.job_bundle_dir for m in sources] == [studio, bundle]
        # Each source is labeled with its true origin for the consent prompt.
        assert [m.source_label for m in sources] == [
            "environment (DEADLINE_HOOKS_DIR)",
            "job bundle",
        ]

    def test_post_submission_hooks_make_a_source_runnable(self, tmp_path):
        """A source with only postSubmission hooks (no preSubmission) is still collected."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._write_submission_hooks(bundle, phase="postSubmission")

        sources = collect_submission_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=None,
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert [m.job_bundle_dir for m in sources] == [bundle]

    def test_pre_gui_only_hooks_are_not_collected(self, tmp_path):
        """A source with only preGUI hooks is not a submission source."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._write_submission_hooks(bundle, phase="preGUI")

        sources = collect_submission_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=None,
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert sources == []

    def test_env_dir_ignored_when_env_hooks_disabled_warns(self, tmp_path):
        """A disabled environment submission hook is skipped and warns about environment
        hooks (not preGUI)."""
        bundle = str(tmp_path / "bundle")
        studio = str(tmp_path / "studio")
        os.makedirs(bundle)
        os.makedirs(studio)
        self._write_submission_hooks(studio)
        warnings: List[str] = []

        sources = collect_submission_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=studio,
            allow_bundle_hooks=True,
            allow_environment_hooks=False,
            print_callback=lambda _msg: None,
            warning_callback=warnings.append,
        )

        assert sources == []
        assert any("DEADLINE_HOOKS_DIR contains submission hooks" in w for w in warnings)

    def test_bundle_hooks_ignored_when_bundle_hooks_disabled_warns(self, tmp_path):
        """A disabled bundle submission hook is skipped and warns about bundle hooks."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._write_submission_hooks(bundle)
        warnings: List[str] = []

        sources = collect_submission_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=None,
            allow_bundle_hooks=False,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
            warning_callback=warnings.append,
        )

        assert sources == []
        assert any("Job bundle contains submission hooks" in w for w in warnings)

    def test_env_dir_equal_to_bundle_dir_is_not_duplicated(self, tmp_path):
        """If DEADLINE_HOOKS_DIR points at the job bundle, the shared hooks.yaml yields a
        single source so submission hooks do not run twice."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        self._write_submission_hooks(bundle)

        sources = collect_submission_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=bundle,  # same directory
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
        )

        assert [m.job_bundle_dir for m in sources] == [bundle]

    def test_invalid_env_dir_warns(self, tmp_path):
        """A DEADLINE_HOOKS_DIR that is not a directory warns and yields no env source."""
        bundle = str(tmp_path / "bundle")
        os.makedirs(bundle)
        missing = str(tmp_path / "does_not_exist")
        warnings: List[str] = []

        sources = collect_submission_hook_sources(
            bundle_dir=bundle,
            env_hooks_dir=missing,
            allow_bundle_hooks=True,
            allow_environment_hooks=True,
            print_callback=lambda _msg: None,
            warning_callback=warnings.append,
        )

        assert sources == []
        assert any("is not a valid directory" in w for w in warnings)
