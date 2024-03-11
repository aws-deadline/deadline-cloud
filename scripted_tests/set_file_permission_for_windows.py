# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import argparse
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from typing import Tuple
import ntsecuritycon as con
import win32security

from deadline.job_attachments.os_file_permission import (
    WindowsFileSystemPermissionSettings,
    WindowsPermissionEnum,
    _set_fs_permission_for_windows,
)

"""
This script is to test a `_set_fs_permission_for_windows()` function in Job Attachment module,
which is for setting file permissions and ownership on Windows.

Prerequisites
-------------
Before running the test, prepare a target user and a disjoint user.

How to Run
----------
To execute this script, run the following command from the root location:
python ./scripted_tests/set_file_permission_for_windows.py \
    -n <the_number_of_files_to_create_for_test> \
    -f <file_permission> \
    -d <directory_permission> \
    -u <target_user_name> \
    -du <disjoint_user_name>

Note: The `-f` and `-d` flags are optional.

Then, the command will do the following:
1. Installs `pywin32`, which is a required package for the testing.
2. Creates a temporary directory and creates the specified number of files in it.
2. The script will add the given target user to the owner list for the specified files.
3. It will then verify (1) whether the target user has Read/Write access to these files,
   and (2) that the disjoint user does not have access.

Example Output
--------------
Created a temporary directory for the test: C:...
Creating temporary files...
Temporary files created.
Running test: Setting file permissions and group ownership...
File permissions and group ownership set.
Total running time for 10 files: 0.01644610000000002
Checking file permissions...
Verified that file permissions are correctly set.
Cleaned up the temporary directory: C:...
End of test execution.
"""


def run_test():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num_files", type=int, required=True)
    parser.add_argument("-f", "--file_permission", required=False, type=str, default="FULL_CONTROL")
    parser.add_argument("-d", "--dir_permission", required=False, type=str, default="FULL_CONTROL")
    parser.add_argument("-u", "--target_user", required=True, type=str)
    parser.add_argument("-du", "--disjoint_user", required=True, type=str)

    args = parser.parse_args()

    num_files = args.num_files
    file_permission = WindowsPermissionEnum(args.file_permission.upper())
    dir_permission = WindowsPermissionEnum(args.dir_permission.upper())

    with TemporaryDirectory() as temp_root_dir:
        print(f"Created a temporary directory for the test: {temp_root_dir}")

        print("Creating temporary files...")
        files = []
        for i in range(0, num_files):
            sub_dir = Path(temp_root_dir) / "sub_directory"
            sub_dir.mkdir(parents=True, exist_ok=True)
            if i < num_files / 2:
                file_path = Path(temp_root_dir) / f"test{i}.txt"
            else:
                file_path = sub_dir / f"test{i}.txt"
            if not os.path.exists(file_path):
                with file_path.open("w", encoding="utf-8") as f:
                    f.write(f"test: {i}")
            files.append(str(file_path))

        print("Temporary files created.")
        print("Running test: Setting file permissions...")
        start_time = time.perf_counter()

        fs_permission_settings = WindowsFileSystemPermissionSettings(
            os_user=args.target_user,
            dir_mode=dir_permission,
            file_mode=file_permission,
        )
        _set_fs_permission_for_windows(
            file_paths=files,
            local_root=temp_root_dir,
            fs_permission_settings=fs_permission_settings,
        )
        print("File permissions set.")
        print(f"Total running time for {num_files} files: {time.perf_counter() - start_time}")

        print("Checking file permissions...")
        for path in files:
            assert check_file_permission(path, args.target_user) == (True, True)
            assert check_file_permission(path, args.disjoint_user) == (False, False)
        print("Verified that file permissions are correctly set.")

    print(f"Cleaned up the temporary directory: {temp_root_dir}")


def check_file_permission(file_path, username) -> Tuple[bool, bool]:
    # Get the file's security information
    sd = win32security.GetFileSecurity(file_path, win32security.DACL_SECURITY_INFORMATION)

    # Get the discretionary access control list (DACL)
    dacl = sd.GetSecurityDescriptorDacl()

    # Lookup the user's SID (Security Identifier)
    sid, _, _ = win32security.LookupAccountName("", username)

    # Trustee
    trustee = {
        "TrusteeForm": win32security.TRUSTEE_IS_SID,
        "TrusteeType": win32security.TRUSTEE_IS_USER,
        "Identifier": sid,
    }

    # Get effective rights
    result = dacl.GetEffectiveRightsFromAcl(trustee)

    # Return a tuple of (has read access, has write access)
    return (bool(result & con.FILE_GENERIC_READ), bool(result & con.FILE_GENERIC_WRITE))


if __name__ == "__main__":
    run_test()
    print("End of test execution.")
