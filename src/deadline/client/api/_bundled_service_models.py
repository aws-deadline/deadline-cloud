# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
botocore stopped releasing for Python 3.9 on 2026-04-29, so on 3.9 its Deadline Cloud
model is frozen and lacks newer APIs. On those Pythons, clients load a copy of the
model from the latest botocore instead (see scripts/update_bundled_service_models.py).
"""

from __future__ import annotations

import os
import sys

import boto3  # type: ignore[import]

_USE_BUNDLED_MODELS = sys.version_info < (3, 10)

_BUNDLED_DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_botocore_data")


def _use_bundled_service_models(session: boto3.Session) -> boto3.Session:
    """
    Makes clients created from ``session`` load the bundled service models. Must run
    before the session creates a client for a bundled service, since botocore caches
    the models it loads.
    """
    if not _USE_BUNDLED_MODELS:
        return session

    loader = session._loader
    search_paths = loader.search_paths
    if _BUNDLED_DATA_PATH in search_paths:
        return session
    # Ahead of botocore's own models but after ~/.aws/models and AWS_DATA_PATH, so
    # models a user provides still take precedence.
    if loader.BUILTIN_DATA_PATH in search_paths:
        search_paths.insert(search_paths.index(loader.BUILTIN_DATA_PATH), _BUNDLED_DATA_PATH)
    else:
        search_paths.append(_BUNDLED_DATA_PATH)
    return session
