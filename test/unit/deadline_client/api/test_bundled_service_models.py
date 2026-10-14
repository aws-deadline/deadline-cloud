# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the Deadline Cloud service model bundled for Python versions that
botocore no longer releases for.
"""

import gzip
import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import boto3  # type: ignore[import]
import botocore  # type: ignore[import]
import pytest
from botocore.stub import Stubber  # type: ignore[import]

from deadline.client import api
from deadline.client.api import _bundled_service_models
from deadline.client.api._session import get_session_client, invalidate_boto3_session_cache

FARM_ID = "farm-0123456789abcdef0123456789abcdef"
FLEET_ID = "fleet-0123456789abcdef0123456789abcdef"

requires_bundled_models = pytest.mark.skipif(
    not _bundled_service_models._USE_BUNDLED_MODELS,
    reason="The bundled model is only used on Python versions botocore no longer supports",
)


@pytest.fixture(autouse=True)
def clear_client_caches():
    invalidate_boto3_session_cache()
    get_session_client.cache_clear()
    yield
    invalidate_boto3_session_cache()
    get_session_client.cache_clear()


def _assert_can_list_volumes(deadline) -> None:
    # ListVolumes was added to the Deadline Cloud model after botocore stopped
    # releasing for Python 3.9, so it is only present via the bundled model there.
    assert deadline.can_paginate("list_volumes")
    with Stubber(deadline) as stubber:
        stubber.add_response(
            "list_volumes",
            {"volumes": []},
            expected_params={"farmId": FARM_ID, "fleetId": FLEET_ID},
        )
        assert deadline.list_volumes(farmId=FARM_ID, fleetId=FLEET_ID)["volumes"] == []


def test_get_boto3_client_supports_recently_added_operation(fresh_deadline_config):
    deadline = api.get_boto3_client("deadline", region="us-west-2")

    _assert_can_list_volumes(deadline)


def test_get_session_client_supports_recently_added_operation_on_caller_session():
    deadline = get_session_client(boto3.Session(region_name="us-west-2"), "deadline")

    _assert_can_list_volumes(deadline)


def test_queue_user_session_supports_recently_added_operation(fresh_deadline_config):
    queue_role_deadline = MagicMock()
    queue_role_deadline.assume_queue_role_for_user.return_value = {
        "credentials": {
            "accessKeyId": "AKIAEXAMPLE",
            "secretAccessKey": "secret",
            "sessionToken": "token",
            "expiration": datetime.now(timezone.utc) + timedelta(hours=1),
        }
    }
    base_session = api.get_boto3_session()
    queue_session = api._session._get_queue_user_boto3_session(
        deadline=queue_role_deadline,
        base_session=base_session,
        farm_id=FARM_ID,
        queue_id="queue-0123456789abcdef0123456789abcdef",
        region="us-west-2",
    )

    _assert_can_list_volumes(queue_session.client("deadline"))


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="Python 3.9 only")
def test_bundled_models_are_used_on_python_3_9():
    assert _bundled_service_models._USE_BUNDLED_MODELS


@pytest.mark.skipif(sys.version_info < (3, 10), reason="Python 3.10+ only")
def test_bundled_models_are_not_used_on_supported_python():
    session = boto3.Session(region_name="us-west-2")
    search_paths_before = list(session._loader.search_paths)

    _bundled_service_models._use_bundled_service_models(session)

    assert session._loader.search_paths == search_paths_before


@requires_bundled_models
def test_customer_models_take_precedence_over_bundled_models():
    session = boto3.Session(region_name="us-west-2")
    loader = session._loader

    _bundled_service_models._use_bundled_service_models(session)

    search_paths = loader.search_paths
    bundled_index = search_paths.index(_bundled_service_models._BUNDLED_DATA_PATH)
    assert search_paths.index(loader.CUSTOMER_DATA_PATH) < bundled_index
    assert bundled_index < search_paths.index(loader.BUILTIN_DATA_PATH)


@requires_bundled_models
def test_use_bundled_service_models_is_idempotent():
    session = boto3.Session(region_name="us-west-2")

    _bundled_service_models._use_bundled_service_models(session)
    _bundled_service_models._use_bundled_service_models(session)

    assert session._loader.search_paths.count(_bundled_service_models._BUNDLED_DATA_PATH) == 1


def _read_model_file(path: str) -> bytes:
    # Compare decompressed bytes: gzip headers embed a timestamp.
    with open(path, "rb") as f:
        data = f.read()
    return gzip.decompress(data) if path.endswith(".gz") else data


def _model_files(root: str) -> dict:
    return {
        os.path.relpath(os.path.join(dirpath, name), root): _read_model_file(
            os.path.join(dirpath, name)
        )
        for dirpath, _, filenames in os.walk(root)
        for name in filenames
        if name.endswith((".json", ".json.gz"))
    }


def _version_tuple(version: str) -> tuple:
    return tuple(int(part) for part in version.strip().split("."))


@pytest.mark.skipif(
    sys.version_info < (3, 10), reason="botocore no longer releases for this Python"
)
def test_bundled_models_match_installed_botocore():
    bundled_root = _bundled_service_models._BUNDLED_DATA_PATH
    with open(os.path.join(bundled_root, "BOTOCORE_VERSION")) as f:
        bundled_version = f.read().strip()
    if _version_tuple(botocore.__version__) < _version_tuple(bundled_version):
        pytest.skip(
            f"Installed botocore {botocore.__version__} is older than the bundled {bundled_version}"
        )

    botocore_root = os.path.join(os.path.dirname(botocore.__file__), "data")
    service_names = [
        name for name in os.listdir(bundled_root) if os.path.isdir(os.path.join(bundled_root, name))
    ]
    assert service_names
    for service_name in service_names:
        bundled = _model_files(os.path.join(bundled_root, service_name))
        installed = _model_files(os.path.join(botocore_root, service_name))
        # Report only file names: the decompressed models are too large for a useful diff.
        differing = sorted(
            name
            for name in bundled.keys() | installed.keys()
            if bundled.get(name) != installed.get(name)
        )
        assert not differing, (
            f"The bundled {service_name} model differs from botocore {botocore.__version__} "
            f"in {differing}. Run `python scripts/update_bundled_service_models.py` and "
            "commit the result."
        )
