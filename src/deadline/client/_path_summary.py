# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Summarizing a group of paths for display.

Kept out of ``_path_utils`` because this is presentation rather than a trust decision, and
it carries cases that only a displayed string cares about -- unresolved ``..`` runs and
preserving the caller's spelling -- which a reader auditing containment should not have to
read past.

Like the containment helpers, this is purely lexical and never raises.
"""

from __future__ import annotations

import os
from typing import Any, Sequence

from ._path_utils import _PARDIR, _UNC_ANCHOR, _split_anchored

__all__ = [
    "common_ancestor",
]


def _leading_pardir_count(parts: list[str]) -> int:
    """Count the leading '..' run that ``normpath`` could not resolve.

    Counted on parts rather than whole components because an anchor can precede the run
    ('C:..\\x' is the parent of the working directory on drive C:).
    """
    count = 0
    for part in parts:
        if part != _PARDIR:
            break
        count += 1
    return count


def common_ancestor(paths: Sequence[Any], *, path_module: Any = None) -> str:
    """Return the deepest directory containing every path in ``paths``.

    This is ``os.path.commonpath`` without the exceptions: paths in unrelated spaces return
    ``""`` rather than raising, and a UNC host is a valid answer for paths on different
    shares of one server. The result keeps the first path's spelling and, like
    ``commonpath``, is purely lexical.
    """
    # Resolved here, not in the signature: a default argument binds os.path when this
    # module is imported, which would silently ignore a test's patch of it and make a
    # caller that omits it untestable on another platform.
    path_module = path_module or os.path
    if not paths:
        return ""

    split = [_split_anchored(p, path_module, normalize_case=True) for p in paths]
    normalized = [([a] if a else []) + parts for a, parts in split]
    anchor, spelled_parts = _split_anchored(paths[0], path_module, normalize_case=False)
    spelled = ([anchor] if anchor else []) + spelled_parts

    # '..' and '../..' are rooted at different unknown places, so runs of differing depth
    # share nothing. Comparing them positionally would return the shallower path, which is
    # not an ancestor of the deeper one -- os.path.commonpath has that bug.
    if len({_leading_pardir_count(parts) for _, parts in split}) > 1:
        return ""

    shared = min(len(components) for components in normalized)
    while shared > 0 and any(other[:shared] != normalized[0][:shared] for other in normalized):
        shared -= 1
    if shared == 0:
        return ""
    # Matching only the bare anchor means different servers, so no shared directory.
    if shared == 1 and normalized[0][0] == _UNC_ANCHOR:
        return ""

    # The anchor carries its own separator, so it abuts the first part directly.
    if anchor:
        return anchor + path_module.sep.join(spelled[1:shared])
    return path_module.sep.join(spelled[:shared])
