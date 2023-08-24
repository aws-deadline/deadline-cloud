# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""AWS configuration."""

# S3 related
S3_CONNECT_TIMEOUT_IN_SECS: int = 30
S3_READ_TIMEOUT_IN_SECS: int = 30

# TODO: This is currently set to our closed-beta endpoint. We need to update this for GA.
DEADLINE_ENDPOINT: str = "https://btpdb6qczg.execute-api.us-west-2.amazonaws.com"
VENDOR_CODE: str = "deadline"
