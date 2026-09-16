# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""A faithful pre-3.11 ``ntpath`` for tests, so those code paths run on any interpreter.

Shared because more than one module needs it: the containment helpers are tested directly,
and the known-asset-root filter has to be tested through the same lens to prove it does not
lose a host-level UNC root on the interpreters where ``normpath`` collapses one.
"""

import ntpath


class PreThreeElevenNtpath:
    """``ntpath`` as it behaved before Python 3.11 for a UNC path that names no share.

    Both ``normpath`` and ``splitdrive`` stripped such a path down to a rooted, driveless
    one. Injecting this exercises that branch on any interpreter, rather than only on the
    3.9 and 3.10 jobs -- the same reason the tests inject ``ntpath`` to begin with.
    """

    # Forces the _splitroot backport, which is what those versions had.
    splitroot = None

    @staticmethod
    def _is_shareless_unc(text: str) -> bool:
        return text.startswith("\\\\") and "\\" not in text[2:]

    @staticmethod
    def normpath(text: str) -> str:
        result = ntpath.normpath(text)
        if PreThreeElevenNtpath._is_shareless_unc(result):
            return result[1:]
        return result

    @staticmethod
    def splitdrive(text: str):
        if PreThreeElevenNtpath._is_shareless_unc(text):
            return "", text
        return ntpath.splitdrive(text)

    def __getattr__(self, name):
        return getattr(ntpath, name)
