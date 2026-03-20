# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Stack trace sanitizer for Deadline Cloud client telemetry.

Strips customer-specific file paths from Python stack traces while preserving
diagnostic value (module names, line numbers, function names, error messages).

Conforms to ADR 2024-02-19: "No customer content or other information provided
by the customer can be submitted, such as bucket names, file names, or similar."
"""

import re
import traceback
from typing import FrozenSet, List

# Packages we control — safe to include relative paths for
_KNOWN_PACKAGES: FrozenSet[str] = frozenset(
    {
        "deadline",
        "openjd",
        "boto3",
        "botocore",
    }
)

_FRAME_RE = re.compile(r'^(\s*File\s+)"([^"]+)",(\s+line\s+\d+,\s+in\s+.*)$')


def _sanitize_path(filepath: str) -> str:
    """Replace a full file path with the package-relative portion or bare filename."""
    if filepath.startswith("<"):
        return filepath

    parts = filepath.replace("\\", "/").split("/")

    for i, part in enumerate(parts):
        stem = part.split(".")[0]
        if stem in _KNOWN_PACKAGES:
            return "/".join(parts[i:])

    for i, part in enumerate(parts):
        if part == "site-packages" and i + 1 < len(parts):
            return "/".join(parts[i + 1 :])

    return parts[-1]


# Matches file paths in exception messages — paths are typically quoted with ' or "
_MSG_PATH_RE = re.compile(r"""(['"])(.*?[/\\].*?)\1""")


def sanitize_message(message: str) -> str:
    """Sanitize an exception message by replacing file paths with safe versions."""

    def _replace(m: re.Match) -> str:
        quote, path = m.group(1), m.group(2)
        return f"{quote}{_sanitize_path(path)}{quote}"

    return _MSG_PATH_RE.sub(_replace, message)


def sanitize_traceback_string(tb_string: str) -> str:
    """Sanitize a formatted traceback string, stripping customer paths."""
    lines = tb_string.splitlines()
    sanitized: List[str] = []
    for line in lines:
        m = _FRAME_RE.match(line)
        if m:
            prefix, filepath, suffix = m.groups()
            sanitized.append(f'{prefix}"{_sanitize_path(filepath)}",{suffix}')
        else:
            sanitized.append(line)
    return "\n".join(sanitized)


def sanitize_exception(exc: BaseException) -> str:
    """Format and sanitize a live exception's full traceback."""
    raw = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return sanitize_traceback_string(raw)
