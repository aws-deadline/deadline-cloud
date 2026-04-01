# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Submission hooks for job bundles."""

from ._manager import HookManager, _generate_hooks_confirmation_message
from ._models import HookConfiguration, HookDefinition, HookMetadata, HookResult

__all__ = [
    "HookManager",
    "HookConfiguration",
    "HookDefinition",
    "HookMetadata",
    "HookResult",
    "_generate_hooks_confirmation_message",
]
