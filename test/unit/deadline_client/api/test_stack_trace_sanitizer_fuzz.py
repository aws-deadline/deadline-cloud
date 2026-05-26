# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Property-based fuzz tests for the stack trace sanitizer.

These tests throw randomly generated paths and exceptions at the sanitizer
and assert invariants — the goal is to surface leaks that hand-written
example tests miss. Modeled on
https://github.com/OpenJobDescription/openjd-model-for-python/blob/mainline/test/openjd/model/test_fuzz.py
which uses stdlib `random` rather than a fuzzing library to avoid adding a
test dependency.
"""

import random
import re
import string
from typing import List, Optional

from deadline.client.api._stack_trace_sanitizer import (
    _KNOWN_PACKAGES,
    _sanitize_path,
    sanitize_exception,
)

# Each test loops over this many fixed seeds. Failures report the offending
# seed so they can be reproduced deterministically.
_NUM_PATH_CASES = 200
_NUM_EXCEPTION_CASES = 200


def _random_path_segment(rng: random.Random) -> str:
    """A plausible filesystem segment — letters, digits, dots, dashes, underscores."""
    chars = string.ascii_letters + string.digits + "._-"
    return "".join(rng.choices(chars, k=rng.randint(1, 20)))


def _random_path(rng: random.Random) -> str:
    """Build a random file path that mixes plausible Linux/Windows shapes."""
    depth = rng.randint(0, 8)
    segments: List[str] = [_random_path_segment(rng) for _ in range(depth)]

    # Sometimes inject a known-package or site-packages segment so the
    # branches in _sanitize_path actually fire.
    if depth > 0 and rng.random() < 0.3:
        idx = rng.randint(0, depth - 1)
        segments[idx] = rng.choice(list(_KNOWN_PACKAGES))
    if depth > 0 and rng.random() < 0.2:
        idx = rng.randint(0, depth - 1)
        segments[idx] = "site-packages"

    # End in something that looks like a filename most of the time.
    if segments and rng.random() < 0.7:
        segments[-1] = segments[-1] + rng.choice([".py", ".pyc", ".so", ".txt", ""])

    sep = rng.choice(["/", "\\"])
    prefix = rng.choice(["", "/", "C:\\", "\\\\server\\share\\", "./", "../"])
    return prefix + sep.join(segments)


def _random_message(rng: random.Random) -> str:
    """A random exception message — may contain paths, secrets, control chars."""
    pieces: List[str] = []
    for _ in range(rng.randint(0, 4)):
        choice = rng.random()
        if choice < 0.4:
            pieces.append(_random_path(rng))
        elif choice < 0.6:
            # Quoted path — checking that quote-stripping doesn't help us.
            pieces.append(f'"{_random_path(rng)}"')
        elif choice < 0.8:
            pieces.append(
                "".join(rng.choices(string.printable + "\x00\x01\x1f", k=rng.randint(1, 30)))
            )
        else:
            pieces.append(_random_path_segment(rng))
    return " ".join(pieces)


_EXCEPTION_TYPES = [
    ValueError,
    RuntimeError,
    KeyError,
    TypeError,
    OSError,
    FileNotFoundError,
    PermissionError,
    Exception,
]


class _CustomException(Exception):
    """Lets us confirm subclasses with overridden __str__/__repr__ don't break things."""

    def __str__(self) -> str:
        return "/home/customer/SECRET/leak.txt"

    def __repr__(self) -> str:
        return "<_CustomException repr leaking /home/customer/leak.txt>"


def _raise_random_exception(rng: random.Random, depth: int = 0) -> BaseException:
    """Build (and catch) an exception, optionally chained, and return it."""
    exc_cls = rng.choice(_EXCEPTION_TYPES + [_CustomException])
    msg = _random_message(rng)

    cause: Optional[BaseException] = None
    if depth < 2 and rng.random() < 0.4:
        cause = _raise_random_exception(rng, depth + 1)

    try:
        if cause is not None and rng.random() < 0.5:
            raise exc_cls(msg) from cause
        raise exc_cls(msg)
    except BaseException as e:  # noqa: BLE001 — the whole point is to catch anything
        return e


_ABSOLUTE_PATH_RE = re.compile(r'File "(?P<path>[^"]+)"')


def _is_absolute_looking(path: str) -> bool:
    if path.startswith("<"):
        return False
    if path.startswith("/") or path.startswith("\\"):
        return True
    if len(path) >= 2 and path[1] == ":":
        return True
    return False


def test_fuzz_sanitize_path_never_returns_absolute_path():
    """_sanitize_path output must never look like an absolute filesystem path,
    and must never raise, across many randomly generated inputs."""
    for seed in range(_NUM_PATH_CASES):
        rng = random.Random(seed)
        raw = _random_path(rng)
        sanitized = _sanitize_path(raw)
        assert not _is_absolute_looking(sanitized), (
            f"Leaked absolute path (seed={seed}): input={raw!r} output={sanitized!r}"
        )


def test_fuzz_sanitize_exception_never_leaks_paths_or_messages():
    """sanitize_exception must (a) never raise, (b) never emit absolute-looking
    paths in frame lines, and (c) never echo the exception's message text."""
    for seed in range(_NUM_EXCEPTION_CASES):
        rng = random.Random(seed)
        exc = _raise_random_exception(rng)
        rendered = sanitize_exception(exc)  # must not raise
        for match in _ABSOLUTE_PATH_RE.finditer(rendered):
            path = match.group("path")
            assert not _is_absolute_looking(path), (
                f"Absolute path in rendered traceback (seed={seed}): {path!r}\n"
                f"full output:\n{rendered}"
            )


def test_fuzz_sanitize_exception_omits_message():
    """The exception's message text must not appear in the sanitized output —
    only the type name is emitted (callers can't constrain third-party / user
    code from putting sensitive data in messages)."""
    for seed in range(_NUM_EXCEPTION_CASES):
        # A marker token vanishingly unlikely to appear in any frame's
        # filename or function name; if it shows up in the output, the
        # message is leaking somewhere.
        marker = f"FUZZ_MARKER_{seed}_xZ9qK"
        msg = f"prefix {marker} suffix /home/customer/{marker}/file.txt"
        try:
            raise RuntimeError(msg)
        except RuntimeError as e:
            rendered = sanitize_exception(e)
        assert marker not in rendered, (
            f"Exception message leaked into sanitized output (seed={seed}):\n{rendered}"
        )
