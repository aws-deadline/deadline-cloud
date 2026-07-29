# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Harness launch-error handling (no real agent calls)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_evals.harness import HarnessError, run_agent  # noqa: E402


def test_run_agent_raises_harness_error_on_missing_binary(tmp_path: Path) -> None:
    # A missing agent binary must surface as a descriptive HarnessError, not a raw
    # FileNotFoundError that aborts the whole batch.
    with pytest.raises(HarnessError, match="could not launch"):
        run_agent(
            "hello",
            tmp_path,
            ["Bash"],
            claude_bin="definitely-not-a-real-binary-xyz",
        )
