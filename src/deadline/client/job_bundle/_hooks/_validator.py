# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Validation for hook configurations and outputs."""

from __future__ import annotations

from typing import Any as _Any, Dict as _Dict

from deadline.client.exceptions import DeadlineOperationError as _DeadlineOperationError


_SUPPORTED_VERSIONS = {"1.0"}


def _validate_hook(hook: _Any, index: int, key: str) -> None:
    """Validate a single hook definition."""
    if not isinstance(hook, dict):
        raise _DeadlineOperationError(f"Hook {index} in '{key}' must be an object")
    if "command" not in hook:
        raise _DeadlineOperationError(f"Hook {index} in '{key}' missing required 'command' field")
    if not isinstance(hook["command"], str):
        raise _DeadlineOperationError(f"Hook {index} in '{key}' 'command' must be a string")
    if "args" in hook and not isinstance(hook["args"], list):
        raise _DeadlineOperationError(f"Hook {index} in '{key}' 'args' must be a list")
    if "timeout" in hook:
        if not isinstance(hook["timeout"], int) or hook["timeout"] <= 0:
            raise _DeadlineOperationError(
                f"Hook {index} in '{key}' 'timeout' must be a positive integer"
            )
    if "env" in hook and not isinstance(hook["env"], dict):
        raise _DeadlineOperationError(f"Hook {index} in '{key}' 'env' must be an object")


def validate_configuration(config: _Dict[str, _Any]) -> None:
    """Validate hook configuration structure."""
    version = config.get("version", "1.0")
    if version not in _SUPPORTED_VERSIONS:
        raise _DeadlineOperationError(
            f"Unsupported hooks version '{version}'. Supported: {', '.join(sorted(_SUPPORTED_VERSIONS))}"
        )

    for key in ("preSubmission", "postSubmission"):
        if key in config and not isinstance(config[key], list):
            raise _DeadlineOperationError(f"Hook configuration '{key}' must be a list")

        for i, hook in enumerate(config.get(key, [])):
            _validate_hook(hook, i, key)


def _validate_asset_references(refs: _Any, hook_name: str) -> None:
    """Validate asset references in a modified payload."""
    if not isinstance(refs, dict):
        raise _DeadlineOperationError(f"Hook '{hook_name}' 'assetReferences' must be an object")
    for field in ("inputFilenames", "inputDirectories", "outputDirectories", "referencedPaths"):
        if field in refs and not isinstance(refs[field], list):
            raise _DeadlineOperationError(
                f"Hook '{hook_name}' 'assetReferences.{field}' must be a list"
            )


def validate_modified_payload(payload: _Dict[str, _Any], hook_name: str) -> None:
    """Validate that a modified payload from a hook is valid."""
    if not isinstance(payload, dict):
        raise _DeadlineOperationError(f"Hook '{hook_name}' output must be a JSON object")

    if "attachments" in payload:
        attachments = payload["attachments"]
        if not isinstance(attachments, dict):
            raise _DeadlineOperationError(f"Hook '{hook_name}' 'attachments' must be an object")
        if "assetReferences" in attachments:
            _validate_asset_references(attachments["assetReferences"], hook_name)
