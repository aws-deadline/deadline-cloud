# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import getpass
import sys


def is_windows_non_admin():
    return sys.platform == "win32" and getpass.getuser() != "Administrator"
