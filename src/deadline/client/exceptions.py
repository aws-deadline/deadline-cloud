# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Exceptions for the AWS Deadline Cloud Client Library.
"""


class DeadlineOperationError(Exception):
    """Error whose message gets printed verbatim by the cli handler"""


class CreateJobWaiterCanceled(Exception):
    """Error for when the waiter on CreateJob is interrupted"""


class UserInitiatedCancel(Exception):
    """Error for when the user requests cancelation"""


class NonValidInputError(Exception):
    """Error for when the user input is nonvalid"""
