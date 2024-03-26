# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import ctypes
import ctypes.wintypes
import sys

# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
assert sys.platform == "win32"

kernel32 = ctypes.WinDLL("Kernel32")

# https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfinalpathnamebyhandlew
kernel32.GetFinalPathNameByHandleW.restype = ctypes.wintypes.DWORD
kernel32.GetFinalPathNameByHandleW.argtypes = [
    ctypes.wintypes.HANDLE,  # [in]  HANDLE hFile,
    ctypes.wintypes.LPWSTR,  # [out] LPWSTR lpszFilePath,
    ctypes.wintypes.DWORD,  # [in]  DWORD  cchFilePath,
    ctypes.wintypes.DWORD,  # [in]  DWORD  dwFlags
]
GetFinalPathNameByHandleW = kernel32.GetFinalPathNameByHandleW

VOLUME_NAME_DOS = 0
VOLUME_NAME_GUID = 1
VOLUME_NAME_NONE = 4
VOLUME_NAME_NT = 2
