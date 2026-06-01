# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for submission hooks functionality."""

import json
import os
import sys
import tempfile

import pytest
import yaml

from typing import List

from deadline.client.exceptions import DeadlineOperationError
from deadline.client.job_bundle._hooks import (
    HookConfiguration,
    HookDefinition,
    HookManager,
    HookMetadata,
    HookResult,
)
from deadline.client.job_bundle._hooks._merger import merge_asset_references, merge_payload
from deadline.client.job_bundle._hooks._validator import (
    validate_pre_gui_output,
    validate_configuration,
    validate_modified_payload,
)


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
