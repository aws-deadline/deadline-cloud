# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Data models for submission hooks."""

from __future__ import annotations

import json as _json
from dataclasses import dataclass as _dataclass, field as _field
from typing import Any as _Any, Dict as _Dict, List as _List, Optional as _Optional


@_dataclass
class HookDefinition:
    """Definition of a single hook."""

    command: str
    args: _List[str] = _field(default_factory=list)
    timeout: int = 60
    env: _Dict[str, str] = _field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: _Dict[str, _Any]) -> HookDefinition:
        return cls(
            command=data["command"],
            args=data.get("args", []),
            timeout=data.get("timeout", 60),
            env=data.get("env", {}),
        )


@_dataclass
class HookConfiguration:
    """Complete hook configuration from hooks.yaml/json."""

    version: str
    pre_submission: _List[HookDefinition]
    post_submission: _List[HookDefinition]

    @classmethod
    def from_dict(cls, data: _Dict[str, _Any]) -> HookConfiguration:
        return cls(
            version=data.get("version", "1.0"),
            pre_submission=[HookDefinition.from_dict(h) for h in data.get("preSubmission", [])],
            post_submission=[HookDefinition.from_dict(h) for h in data.get("postSubmission", [])],
        )


@_dataclass
class HookMetadata:
    """Metadata provided to hooks via stdin and environment variables."""

    job_name: str
    priority: int
    farm_id: str
    queue_id: str
    job_bundle_dir: str
    parameters: _Dict[str, _Any]
    submitter_name: str
    asset_references: _Dict[str, _Any]
    submission_payload: _Dict[str, _Any]
    storage_profile_id: _Optional[str] = None
    job_id: _Optional[str] = None

    def to_dict(self) -> _Dict[str, _Any]:
        result = {
            "jobName": self.job_name,
            "priority": self.priority,
            "farmId": self.farm_id,
            "queueId": self.queue_id,
            "jobBundleDir": self.job_bundle_dir,
            "parameters": self.parameters,
            "submitterName": self.submitter_name,
            "assetReferences": self.asset_references,
            "submissionPayload": self.submission_payload,
        }
        if self.storage_profile_id:
            result["storageProfileId"] = self.storage_profile_id
        if self.job_id:
            result["jobId"] = self.job_id
        return result

    def to_json(self) -> str:
        return _json.dumps(self.to_dict(), indent=2)

    def to_environment_variables(self) -> _Dict[str, str]:
        env = {
            "DEADLINE_JOB_NAME": self.job_name,
            "DEADLINE_PRIORITY": str(self.priority),
            "DEADLINE_FARM_ID": self.farm_id,
            "DEADLINE_QUEUE_ID": self.queue_id,
            "DEADLINE_JOB_BUNDLE_DIR": self.job_bundle_dir,
        }
        if self.storage_profile_id:
            env["DEADLINE_STORAGE_PROFILE_ID"] = self.storage_profile_id
        if self.job_id:
            env["DEADLINE_JOB_ID"] = self.job_id
        return env


@_dataclass
class HookResult:
    """Result of hook execution."""

    exit_code: int
    stdout: str
    stderr: str
    execution_time: float
    timed_out: bool

    def is_success(self) -> bool:
        return self.exit_code == 0 and not self.timed_out
