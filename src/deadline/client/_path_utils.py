# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Path containment helpers that understand Windows UNC paths.

``os.path.commonpath`` raises ``ValueError`` rather than comparing a host-level UNC path
(``\\\\server``) with a path under one of its shares, and raises the same way for two shares
on one host. Which message it raises depends on the interpreter -- before 3.11
``splitdrive`` reported no drive for ``\\\\server``, so the pair read as mixing absolute
and relative; from 3.11 it reports one and they read as different drives -- but it raises
on every supported version. Callers that read that exception as "not contained" reject
valid paths.

These helpers compare paths component by component instead, so a UNC host is an ordinary
ancestor of its shares. Every function takes an optional ``path_module``
(``ntpath``/``posixpath``), resolved to ``os.path`` at call time rather than in the
signature, so Windows semantics stay testable on non-Windows hosts from any call site.

Comparisons are lexical -- pass ``realpath`` output in if symlinks must be resolved -- and
never raise. Anything unresolvable fails closed, since callers use containment to decide
whether a path is trusted.
"""

from __future__ import annotations

import os
import string
from typing import Any, Iterable

__all__ = [
    "is_absolute_path",
    "is_any_path_contained",
    "is_bare_unc_anchor",
    "is_path_contained",
    "normalized_path",
    "path_components",
]

# Anchors the UNC path space. It names no server on its own, so unlike POSIX '/' it is not
# a directory and contains nothing.
_UNC_ANCHOR = "\\\\"

# Spelled out rather than taken from os.* because these helpers parse Windows paths on any
# host, where os.sep is '/'.
_EXTENDED_PREFIX = "\\\\?\\"
_DEVICE_PREFIX = "\\\\.\\"
_EXTENDED_UNC_MARKER = "UNC"

_PARDIR = ".."


def _splitroot(text: str, path_module: Any) -> tuple[str, str, str]:
    """``path_module.splitroot``, backported for Python < 3.12.

    ``(drive, root)`` is what distinguishes one path space from another: ``('', '\\\\')``
    (rooted but driveless) and ``('\\\\\\\\', '')`` (UNC) both consist only of separators.
    """
    splitroot = getattr(path_module, "splitroot", None)
    if splitroot is not None:
        return splitroot(text)

    drive, rest = path_module.splitdrive(text)
    separators = path_module.sep + (getattr(path_module, "altsep", None) or "")
    if rest[:1] not in separators or not rest:
        return drive, "", rest
    leading = len(rest) - len(rest.lstrip(separators))
    # POSIX gives '//' its own root spelling, but collapses three or more.
    root_length = 2 if (leading == 2 and path_module.sep == "/") else 1
    return drive, rest[:root_length], rest[root_length:]


def _denotes_drive(text: str) -> bool:
    """True for a bare drive spelling such as ``'C:'``."""
    return len(text) == 2 and text[1] == ":" and text[0] in string.ascii_letters


def _fold_extended_length_prefix(text: str) -> str:
    """Rewrite an extended-length path as the plain path it denotes.

    ``\\\\?\\`` only turns off Win32 normalization; it names the same location as the plain
    spelling. Folding it keeps one location from having two sets of components, which would
    report a prefixed path outside a root that plainly contains it. Forms with no plain
    spelling (``Volume{GUID}``, ``GLOBALROOT``, and the ``\\\\.\\`` device namespace) are
    left alone, so they keep a path space of their own and alias nothing.
    """
    if not text.startswith(_EXTENDED_PREFIX):
        return text
    denoted = text[len(_EXTENDED_PREFIX) :]
    head = denoted.split("\\", 1)[0]
    if head.upper() == _EXTENDED_UNC_MARKER:
        # '\\?\UNC\server\share' is '\\server\share'. 'UNC' alone names no server, so it
        # folds to the bare anchor, which contains nothing.
        return _UNC_ANCHOR + denoted[len(_EXTENDED_UNC_MARKER) :].lstrip("\\")
    if _denotes_drive(head):
        return denoted
    return text


def _split_anchored(path: Any, path_module: Any, normalize_case: bool) -> tuple[str, list[str]]:
    """Return ``(anchor, parts)``, where ``anchor + sep.join(parts)`` reconstructs ``path``.

    The anchor names the path space and carries its own trailing separator. A UNC anchor is
    the bare ``\\\\`` marker, leaving the server and share as ordinary parts -- which is what
    lets a host-level root contain the shares beneath it.
    """
    text = str(path)
    windows = path_module.sep == "\\"
    if windows:
        text = text.replace("/", "\\")
        # Folded before normpath, which leaves '..' alone inside a '\\?\' path before 3.11
        # and collapses it after. Folding first makes the result the same on every
        # supported interpreter.
        text = _fold_extended_length_prefix(text)
    # Read off the text, not off splitdrive's drive: before Python 3.11 splitdrive reports
    # no drive at all for a UNC path that names no share, which would put a host-level root
    # in the rooted-driveless space and stop it containing its own shares -- the bug this
    # module exists to fix.
    in_unc_space = (
        windows
        and text.startswith(_UNC_ANCHOR)
        and not text.startswith(_EXTENDED_PREFIX)
        and not text.startswith(_DEVICE_PREFIX)
    )
    text = path_module.normpath(text)
    if in_unc_space and not text.startswith(_UNC_ANCHOR):
        # Those same versions collapse the leading pair itself ('\\host' -> '\host').
        text = _UNC_ANCHOR + text.lstrip(path_module.sep)
    if normalize_case:
        text = path_module.normcase(text)

    drive, root, tail = _splitroot(text, path_module)
    parts = [part for part in tail.split(path_module.sep) if part]

    if not windows:
        # '//foo' and '/foo' are the same file on the platforms this client targets.
        return (path_module.sep if root else ""), parts

    if drive.startswith(_EXTENDED_PREFIX) or drive.startswith(_DEVICE_PREFIX):
        # Whatever reaches here has no plain spelling to fold to, so the drive is a whole
        # anchor occupying its own space. The anchor carries its own trailing separator, so
        # a share root and the files under it -- which differ only by it -- still match.
        return drive + path_module.sep, parts
    if in_unc_space:
        # The server and share are ordinary parts beneath the bare anchor, which is what
        # lets a host-level root contain the shares under it.
        return _UNC_ANCHOR, [p for p in text[len(_UNC_ANCHOR) :].split(path_module.sep) if p]
    return drive + root, parts


def path_components(
    path: Any,
    *,
    path_module: Any = None,
    normalize_case: bool = True,
) -> list[str]:
    """Split ``path`` into the components used for ancestor comparisons.

    ``..`` segments are resolved first. The first component is the path space (``'/'``,
    ``'C:\\'``, ``'C:'`` for drive-relative, ``'\\\\'`` for UNC, absent when relative) and
    the rest are the path's parts, so comparing these lists component-wise confuses neither
    one path space for another nor a string prefix for a directory prefix.

    An extended-length prefix folds to the plain path it denotes, so ``\\\\?\\C:\\a`` and
    ``C:\\a`` yield the same components. Prefixed forms that denote no plain path keep a
    space of their own.

    ``normalize_case`` lowercases components on Windows to match the filesystem.
    """
    # Resolved here, not in the signature: a default argument binds os.path when this
    # module is imported, which would silently ignore a test's patch of it and make a
    # caller that omits it untestable on another platform.
    path_module = path_module or os.path
    anchor, parts = _split_anchored(path, path_module, normalize_case)
    return ([anchor] if anchor else []) + parts


def is_absolute_path(path: Any, *, path_module: Any = None) -> bool:
    """Return True iff ``path`` names a location without consulting the working directory.

    ``path_module.isabs`` cannot be used before Python 3.11: it tests what ``splitdrive``
    leaves behind, and for a UNC path that names a share ``splitdrive`` consumes the whole
    string, so ``isabs(r'\\\\host\\share')`` is False there. Callers use this to decide
    whether a path may be trusted as a root or accepted as a parameter value, so a UNC
    share silently reading as relative drops valid roots and rejects valid values.

    Only a fully anchored path counts. On Windows neither a drive-relative path (``C:x``,
    meaning ``x`` under the working directory on ``C:``) nor a rooted, driveless one
    (``\\x``, meaning ``x`` at the root of whichever drive the process is on) names a fixed
    location: resolving either consults the working directory, which is exactly what the
    known-root hardening must not let a caller supply implicitly.

    ``ntpath.isabs`` accepted ``\\x`` through 3.12 and rejects it from 3.13, so it cannot be
    the reference in either direction. Answering from the anchor keeps the verdict the same
    on every version.
    """
    path_module = path_module or os.path
    anchor, _ = _split_anchored(path, path_module, normalize_case=True)
    if not anchor:
        return False
    if path_module.sep != "\\":
        return True
    return not _denotes_drive(anchor) and anchor != path_module.sep


def is_bare_unc_anchor(path: Any, *, path_module: Any = None) -> bool:
    """True iff ``path`` is the bare ``\\\\`` marker, which names no server.

    It is fully qualified yet identifies no location, so it contains nothing --
    :func:`is_path_contained` enforces that. As a trusted root it is worse than useless: in a
    component trie it is a prefix of *every* real UNC root, so keeping it would filter them
    all out and leave only a root that matches nothing.

    ``'//'`` and ``'\\\\?\\UNC\\'`` both normalize to it, so it arrives from more spellings
    than it looks like.
    """
    return path_components(path, path_module=path_module) == [_UNC_ANCHOR]


def normalized_path(path: Any, *, path_module: Any = None) -> str:
    """Return ``path`` with ``..``, ``.``, repeated separators and separator style resolved.

    ``path_module.normpath`` with the version differences handled: before Python 3.11 it
    collapses the leading pair on a UNC path that names no share (``\\\\host`` -> ``\\host``),
    moving a host-level root out of the UNC space so it matches none of its own shares.
    Case is preserved, unlike the components used for comparison.
    """
    path_module = path_module or os.path
    anchor, parts = _split_anchored(path, path_module, normalize_case=False)
    return anchor + path_module.sep.join(parts)


def is_path_contained(
    path: Any,
    root: Any,
    *,
    path_module: Any = None,
) -> bool:
    """Return True iff ``path`` equals or is a descendant of ``root``.

    Containment is anchored on whole components, so a sibling that merely shares a string
    prefix (root ``/trusted/project`` vs path ``/trusted/project-secret``) is outside the
    root. Paths in unrelated spaces -- different drives, different UNC hosts, one relative
    and one absolute -- are not contained.
    """
    root_components = path_components(root, path_module=path_module)
    candidate_components = path_components(path, path_module=path_module)
    # A bare '\\' root names no server, so it is an ancestor of nothing -- otherwise it
    # would prefix, and so trust, every reachable share. ntpath.isabs lets it reach here.
    if root_components == [_UNC_ANCHOR]:
        return candidate_components == [_UNC_ANCHOR]
    if candidate_components[: len(root_components)] != root_components:
        return False
    # Shared leading '..' belongs to the root; one below it could climb back out.
    return _PARDIR not in candidate_components[len(root_components) :]


def is_any_path_contained(
    path: Any,
    roots: Iterable[Any],
    *,
    path_module: Any = None,
) -> bool:
    """Return True iff ``path`` is contained by any root in ``roots``."""
    return any(is_path_contained(path, root, path_module=path_module) for root in roots)
