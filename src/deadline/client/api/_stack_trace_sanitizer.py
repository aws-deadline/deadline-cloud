# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Stack trace sanitizer for Deadline Cloud client telemetry.

Uses an allowlist approach: only explicitly chosen fields (sanitized filename,
line number, function name, exception type) are emitted. Source code context
and exception messages are intentionally omitted as they could contain
customer data.

Conforms to ADR 2024-02-19: "No customer content or other information provided
by the customer can be submitted, such as bucket names, file names, or similar."
"""

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


def _sanitize_traceback(te: traceback.TracebackException) -> List[str]:
    """Recursively format a TracebackException chain using only allowlisted fields."""
    lines: List[str] = []

    # Handle chained exceptions (cause or context)
    if te.__cause__ is not None:
        lines.extend(_sanitize_traceback(te.__cause__))
        lines.append("\nThe above exception was the direct cause of the following exception:\n")
    elif te.__context__ is not None and not te.__suppress_context__:
        lines.extend(_sanitize_traceback(te.__context__))
        lines.append("\nDuring handling of the above exception, another exception occurred:\n")

    lines.append("Traceback (most recent call last):")
    for frame in te.stack:
        safe_path = _sanitize_path(frame.filename)
        lines.append(f'  File "{safe_path}", line {frame.lineno}, in {frame.name}')
        # Intentionally omit frame.line — source code context could
        # contain credentials, customer data, or other sensitive values

    # Only emit the exception type, not the message
    exc_name = te.exc_type.__qualname__ if te.exc_type else "UnknownException"
    lines.append(exc_name)

    return lines


def sanitize_exception(exc: BaseException) -> str:
    """Format and sanitize a live exception using only allowlisted fields."""
    te = traceback.TracebackException.from_exception(exc)
    return "\n".join(_sanitize_traceback(te))
