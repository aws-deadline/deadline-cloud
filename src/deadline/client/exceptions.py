# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Exceptions for the AWS Deadline Cloud Client Library.
"""


class DeadlineOperationError(Exception):
    """Error whose message gets printed verbatim by the cli handler"""


class DeadlineOperationCanceled(DeadlineOperationError):
    """DeadlineOperationError for when an operation was canceled"""

    def __init__(self, message: str = "Operation canceled"):
        super().__init__(message)


class DeadlineOperationTimedOut(DeadlineOperationError):
    """DeadlineOperationError for when an operation timed out"""

    def __init__(self, message: str = "Operation timed out"):
        super().__init__(message)


class CreateJobWaiterCanceled(DeadlineOperationCanceled):
    """Error for when the waiter after CreateJob is interrupted"""

    def __init__(self, message: str = "Operation canceled while waiting for CreateJob to finish"):
        super().__init__(message)


class UserInitiatedCancel(DeadlineOperationCanceled):
    """Error for when the user requests cancelation"""

    def __init__(self, message: str = "Operation canceled by user"):
        super().__init__(message)


class NonValidInputError(Exception):
    """Error for when the user input is nonvalid"""


class ManifestOutdatedError(Exception):
    """Error for when local files are different from version captured in manifest"""
