# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Stack trace sanitizer for Deadline Cloud client telemetry.

**Design tenet:** no customer-provided content (file paths, bucket names,
exception message text, source code lines, local variable values, etc.) may
appear in telemetry. Only an allowlist of structured fields is emitted:
sanitized filename, line number, function name, and exception type. The
exception's message string and source-line context are dropped entirely
because we have no control over what third-party libraries put in them.
"""

import traceback
from typing import FrozenSet, List

# Packages we author or vendor — emitting paths relative to these is safe
# because the path itself only reveals which of our own modules raised.
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
    # Synthetic frame sources like "<string>", "<stdin>", or
    # "<frozen importlib._bootstrap>" don't reference the filesystem and are
    # already non-identifying, so pass them through unchanged.
    if filepath.startswith("<"):
        return filepath

    # Normalize Windows separators so the rest of the function only deals
    # with forward slashes.
    parts = filepath.replace("\\", "/").split("/")

    # If any path segment names one of our known packages, return everything
    # from that segment onward. Scan right-to-left and match the *rightmost*
    # occurrence: the actually-installed package is always at the tail of
    # the path, so a customer directory that happens to share a name with
    # one of our packages (e.g. ~/deadline/...) earlier in the path can't
    # cause us to keep the customer-named segments between them. `stem`
    # strips a trailing extension so paths like ".../deadline.egg-info/..."
    # still match "deadline".
    for i in range(len(parts) - 1, -1, -1):
        stem = parts[i].split(".")[0]
        if stem in _KNOWN_PACKAGES:
            return "/".join(parts[i:])

    # Unknown third-party library installed into a venv: keep the
    # library-relative subpath but drop everything above site-packages
    # (which would otherwise leak the customer's home / venv layout).
    # Same right-to-left rationale as above — the real site-packages
    # directory is always at the tail.
    for i in range(len(parts) - 1, -1, -1):
        if parts[i] == "site-packages" and i + 1 < len(parts):
            return "/".join(parts[i + 1 :])

    # Anything else (customer scripts, project trees) — keep only the
    # bare filename.
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
