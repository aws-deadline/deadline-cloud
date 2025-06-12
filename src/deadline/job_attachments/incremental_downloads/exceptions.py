# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Exceptions for the AWS Deadline Cloud Incremental Download Operations
"""


class PidLockAlreadyHeld(Exception):
    """Error for when the pid lock is already by a process"""
