# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""The AWS Deadline Cloud client library for Python. This library provides functions that support
integrating your application or integrating your pipeline with a Deadline Cloud farm. The
`deadline` CLI command is implemented using this library, and it provides modules for processing
and submitting job bundles, monitoring jobs, and downloading job output.
"""

from ._version import __version__ as version  # noqa

__all__ = [
    "version",
]
