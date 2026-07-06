# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the multi-region fan-out behavior of deadline.client.api list functions:
the ``list_farms`` aggregation/failure semantics, the ``_iter_farms_by_region``
streaming generator, and region pass-through on the per-farm list APIs.
"""

import os
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from deadline.client import api
from deadline.client.api import _list_apis
from deadline.client.exceptions import DeadlineOperationError


@pytest.fixture(autouse=True)
def _clear_endpoint_override_env(monkeypatch):
    """
    Ensure each test in this module controls the AWS_ENDPOINT_URL_DEADLINE /
    DEADLINE_CLOUD_REGIONS env vars itself.

    Some sibling CLI test modules set AWS_ENDPOINT_URL_DEADLINE at import time without
    cleanup; under xdist that can leak into this worker and flip list_farms into its
    single-region endpoint-override path, breaking the fan-out tests here. Clearing the
    vars up front makes the fan-out tests deterministic; tests that exercise the override
    path set it explicitly.
    """
    monkeypatch.delenv("AWS_ENDPOINT_URL_DEADLINE", raising=False)
    monkeypatch.delenv("DEADLINE_CLOUD_REGIONS", raising=False)


def _make_farm(farm_id, display_name="Farm"):
    return {"farmId": farm_id, "displayName": display_name, "description": ""}


def _make_cmk_farm(farm_id, display_name="Farm", kms_key_arn="arn:aws:kms:::key/abc"):
    """A farm encrypted with a customer-managed KMS key (has a ``kmsKeyArn``)."""
    farm = _make_farm(farm_id, display_name)
    farm["kmsKeyArn"] = kms_key_arn
    return farm


def _client_returning(farms):
    """Builds a mock deadline client whose list_farms returns a single (unpaginated) page."""
    client = MagicMock()
    # Return fresh dicts each call so the region-tagging mutation doesn't leak across regions.
    client.list_farms.return_value = {"farms": [dict(f) for f in farms]}
    return client


def _client_raising(exc):
    client = MagicMock()
    client.list_farms.side_effect = exc
    return client


def test_list_farms_multi_region_happy_path(fresh_deadline_config):
    """Farms from every region are concatenated, each tagged with its region."""
    regions = ["us-west-2", "us-east-1", "eu-west-1"]
    clients = {
        "us-west-2": _client_returning([_make_farm("farm-west2")]),
        "us-east-1": _client_returning([_make_farm("farm-east1a"), _make_farm("farm-east1b")]),
        "eu-west-1": _client_returning([_make_farm("farm-euwest1")]),
    }

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    farms = result["farms"]
    assert len(farms) == 4
    # Every farm carries the region it came from.
    by_id = {f["farmId"]: f["region"] for f in farms}
    assert by_id == {
        "farm-west2": "us-west-2",
        "farm-east1a": "us-east-1",
        "farm-east1b": "us-east-1",
        "farm-euwest1": "eu-west-1",
    }


def test_list_farms_partial_failure_returns_survivors(fresh_deadline_config, caplog):
    """One region failing emits a warning but farms from surviving regions are returned."""
    regions = ["us-west-2", "us-east-1"]
    boom = RuntimeError("region is opted out")
    clients = {
        "us-west-2": _client_returning([_make_farm("farm-west2")]),
        "us-east-1": _client_raising(boom),
    }

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with (
        caplog.at_level("WARNING"),
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    farms = result["farms"]
    assert [f["farmId"] for f in farms] == ["farm-west2"]
    assert farms[0]["region"] == "us-west-2"
    # Warning mentions the failing region and the cause.
    assert any(
        "us-east-1" in rec.message and "region is opted out" in rec.message
        for rec in caplog.records
    )


def test_list_farms_total_failure_raises(fresh_deadline_config):
    """When every region fails, DeadlineOperationError surfaces each region's cause."""
    regions = ["us-west-2", "us-east-1"]
    clients = {
        "us-west-2": _client_raising(RuntimeError("west2 boom")),
        "us-east-1": _client_raising(RuntimeError("east1 boom")),
    }

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        with pytest.raises(DeadlineOperationError) as excinfo:
            api.list_farms()

    message = str(excinfo.value)
    assert "us-west-2" in message and "west2 boom" in message
    assert "us-east-1" in message and "east1 boom" in message


def test_list_farms_explicit_region_single_region(fresh_deadline_config):
    """Passing region= scopes the call to exactly that region and tags farms with it."""
    captured = {}

    def fake_get_client(service_name, config=None, region=None):
        captured["region"] = region
        return _client_returning([_make_farm("farm-only")])

    # If fan-out were used, get_deadline_regions would be consulted; assert it is NOT.
    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms(region="ap-south-1")

    regions_mock.assert_not_called()
    assert captured["region"] == "ap-south-1"
    farms = result["farms"]
    assert len(farms) == 1
    assert farms[0]["region"] == "ap-south-1"


def test_iter_farms_by_region_yields_per_region_including_failure(fresh_deadline_config):
    """The generator yields (region, farms, None) on success and (region, None, exc) on failure."""
    regions = ["us-west-2", "us-east-1"]
    boom = RuntimeError("kaboom")
    clients = {
        "us-west-2": _client_returning([_make_farm("farm-west2")]),
        "us-east-1": _client_raising(boom),
    }

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        results = list(_list_apis._iter_farms_by_region(regions=regions))

    # Out-of-order completion is allowed, so index by region.
    by_region = {region: (farms, exc) for region, farms, exc in results}
    assert set(by_region) == {"us-west-2", "us-east-1"}

    west_farms, west_exc = by_region["us-west-2"]
    assert west_exc is None
    assert west_farms is not None
    assert [f["farmId"] for f in west_farms] == ["farm-west2"]
    assert west_farms[0]["region"] == "us-west-2"

    east_farms, east_exc = by_region["us-east-1"]
    assert east_farms is None
    assert east_exc is boom


def test_iter_farms_by_region_does_not_swallow_base_exceptions(fresh_deadline_config):
    """
    Per-region errors are reported as (region, None, exc), but control-flow exceptions
    like KeyboardInterrupt/SystemExit must NOT be captured as a per-region "failure" --
    they should propagate so the operation can actually be interrupted.
    """
    regions = ["us-west-2", "us-east-1"]
    clients = {
        "us-west-2": _client_returning([_make_farm("farm-west2")]),
        "us-east-1": _client_raising(KeyboardInterrupt()),
    }

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        with pytest.raises(KeyboardInterrupt):
            list(_list_apis._iter_farms_by_region(regions=regions))


def test_iter_farms_by_region_out_of_order_completion(fresh_deadline_config):
    """A slow region completes after a fast one; both results still arrive."""
    regions = ["slow-region", "fast-region"]

    def slow_list_farms(**kwargs):
        time.sleep(0.2)
        return {"farms": [_make_farm("farm-slow")]}

    slow_client = MagicMock()
    slow_client.list_farms.side_effect = slow_list_farms
    fast_client = _client_returning([_make_farm("farm-fast")])
    clients = {"slow-region": slow_client, "fast-region": fast_client}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        order = [region for region, _, _ in _list_apis._iter_farms_by_region(regions=regions)]

    # Fast region should complete (and thus be yielded) before the slow one.
    assert order == ["fast-region", "slow-region"]


def test_iter_farms_by_region_pagination_per_region(fresh_deadline_config):
    """nextToken pagination is honored within each region's call."""
    regions = ["us-west-2"]
    client = MagicMock()
    client.list_farms.side_effect = [
        {"farms": [_make_farm("farm-a")], "nextToken": "t1"},
        {"farms": [_make_farm("farm-b")]},
    ]

    def fake_get_client(service_name, config=None, region=None):
        return client

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        results = list(_list_apis._iter_farms_by_region(regions=regions))

    assert len(results) == 1
    region, farms, exc = results[0]
    assert exc is None
    assert farms is not None
    assert [f["farmId"] for f in farms] == ["farm-a", "farm-b"]
    assert all(f["region"] == "us-west-2" for f in farms)


@pytest.mark.parametrize(
    "api_func, list_method, list_property",
    [
        ("list_queues", "list_queues", "queues"),
        ("list_jobs", "list_jobs", "jobs"),
        ("list_fleets", "list_fleets", "fleets"),
        ("list_storage_profiles_for_queue", "list_storage_profiles_for_queue", "storageProfiles"),
    ],
)
def test_per_farm_list_apis_pass_region_through(
    fresh_deadline_config, api_func, list_method, list_property
):
    """list_queues/list_jobs/list_fleets/list_storage_profiles_for_queue forward region."""
    captured = {}
    client = MagicMock()
    getattr(client, list_method).return_value = {list_property: []}

    def fake_get_client(service_name, config=None, region=None):
        captured["service_name"] = service_name
        captured["region"] = region
        return client

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        getattr(api, api_func)(region="eu-central-1")

    assert captured["service_name"] == "deadline"
    assert captured["region"] == "eu-central-1"


def test_per_farm_list_api_default_region_is_none(fresh_deadline_config):
    """With no region argument, region=None flows to get_boto3_client (unchanged behavior)."""
    captured = {}
    client = MagicMock()
    client.list_queues.return_value = {"queues": []}

    def fake_get_client(service_name, config=None, region=None):
        captured["region"] = region
        return client

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        api.list_queues()

    assert captured["region"] is None


def test_list_farms_explicit_region_ignores_configured_farm_region(fresh_deadline_config):
    """
    an explicit region= argument wins over a configured defaults.farm_region.
    """
    from deadline.client import config

    config.set_setting("defaults.farm_region", "us-west-2")

    captured = {}

    def fake_get_client(service_name, config=None, region=None):
        captured["region"] = region
        return _client_returning([_make_farm("farm-explicit")])

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms(region="ap-northeast-1")

    # Fan-out was never consulted, and the explicit region (not the configured one) was used.
    regions_mock.assert_not_called()
    assert captured["region"] == "ap-northeast-1"
    assert result["farms"][0]["region"] == "ap-northeast-1"


def test_list_farms_no_regions_returns_empty_no_error(fresh_deadline_config):
    """
    when get_deadline_regions() returns [], list_farms returns {"farms": []}
    without crashing and without raising the all-regions-failed error (there were no
    regions to fail).
    """
    with (
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=[]),
        patch.object(_list_apis, "get_boto3_client") as get_client_mock,
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    assert result == {"farms": []}
    # No region => no client was ever built.
    get_client_mock.assert_not_called()


def test_list_farms_explicit_region_does_not_fan_out(fresh_deadline_config):
    """
    list_farms(region=...) does a single-region call - it neither consults
    get_deadline_regions nor builds more than one client.
    """
    calls = []

    def fake_get_client(service_name, config=None, region=None):
        calls.append(region)
        return _client_returning([_make_farm("farm-single")])

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        api.list_farms(region="us-east-2")

    regions_mock.assert_not_called()
    # Exactly one client built, for the requested region only.
    assert calls == ["us-east-2"]


def test_list_farms_multi_region_concatenation_contract(fresh_deadline_config):
    """
    multi-region results are concatenated. Because regions complete via
    as_completed, ordering is non-deterministic; the pinned contract is that EVERY
    region's farms are present (as a set) with correct region tags.
    """
    regions = ["us-west-2", "us-east-1", "eu-west-1", "ap-south-1"]
    clients = {r: _client_returning([_make_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    farms = result["farms"]
    # All regions are represented exactly once (set contract; order is not guaranteed).
    assert {f["region"] for f in farms} == set(regions)
    assert {f["farmId"] for f in farms} == {f"farm-{r}" for r in regions}
    # Each farm is tagged with its own origin region (no cross-region mislabeling).
    assert all(f["farmId"] == f"farm-{f['region']}" for f in farms)


@pytest.mark.parametrize(
    "api_func, list_method, list_property",
    [
        ("list_queues", "list_queues", "queues"),
        ("list_jobs", "list_jobs", "jobs"),
        ("list_fleets", "list_fleets", "fleets"),
        ("list_storage_profiles_for_queue", "list_storage_profiles_for_queue", "storageProfiles"),
    ],
)
def test_per_farm_list_apis_explicit_region_overrides_config(
    fresh_deadline_config, api_func, list_method, list_property
):
    """
    per-farm list APIs forward an explicit region= even when
    defaults.farm_region is configured (explicit arg wins). Resolution to the configured
    value happens later inside get_boto3_client, which here is mocked - so we assert the
    explicit region is the one passed through.
    """
    from deadline.client import config

    config.set_setting("defaults.farm_region", "us-west-2")

    captured = {}
    client = MagicMock()
    getattr(client, list_method).return_value = {list_property: []}

    def fake_get_client(service_name, config=None, region=None):
        captured["region"] = region
        return client

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        getattr(api, api_func)(region="eu-north-1")

    assert captured["region"] == "eu-north-1"


def test_list_farms_endpoint_override_scans_single_session_region(fresh_deadline_config):
    """
    With AWS_ENDPOINT_URL_DEADLINE set and region=None, list_farms does NOT fan out: it
    scans only the session/profile default region (a single client), and tags the farms
    with the session region. get_deadline_regions must not be consulted.
    """
    calls = []

    def fake_get_client(service_name, config=None, region=None):
        calls.append(region)
        return _client_returning([_make_farm("farm-single")])

    mock_session = MagicMock()
    mock_session.region_name = "us-east-1"

    with patch.dict(os.environ, {"AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline"}):
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
            patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            result = api.list_farms()

    regions_mock.assert_not_called()
    assert calls == ["us-east-1"]
    assert [f["region"] for f in result["farms"]] == ["us-east-1"]


def test_list_farms_endpoint_override_session_region_none_does_not_crash(fresh_deadline_config):
    """
    With the override set and a session that has no region (region_name is None), the
    single-region scan still works; farms carry a None region tag rather than crashing.
    """

    def fake_get_client(service_name, config=None, region=None):
        return _client_returning([_make_farm("farm-noregion")])

    mock_session = MagicMock()
    mock_session.region_name = None

    with patch.dict(os.environ, {"AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline"}):
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
            patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            result = api.list_farms()

    regions_mock.assert_not_called()
    assert [f["farmId"] for f in result["farms"]] == ["farm-noregion"]
    assert result["farms"][0]["region"] is None


def test_list_farms_endpoint_override_with_explicit_regions_still_fans_out(fresh_deadline_config):
    """
    User intent wins: when the override is set AND the user explicitly lists regions via
    DEADLINE_CLOUD_REGIONS, list_farms still fans out across those explicit regions (the
    user is being deliberate). The single-region short-circuit is suppressed.
    """
    regions = ["us-west-2", "eu-west-1"]
    clients = {r: _client_returning([_make_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with patch.dict(
        os.environ,
        {
            "AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline",
            "DEADLINE_CLOUD_REGIONS": ",".join(regions),
        },
    ):
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(
                _list_apis.config_file, "get_deadline_regions", return_value=regions
            ) as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            result = api.list_farms()

    # Fan-out path was taken (explicit region list honored).
    regions_mock.assert_called()
    assert {f["region"] for f in result["farms"]} == set(regions)


def test_list_farms_endpoint_override_explicit_region_arg_still_single(fresh_deadline_config):
    """
    An explicit region= argument is always honored (single region) regardless of the
    endpoint override, unchanged from before.
    """
    calls = []

    def fake_get_client(service_name, config=None, region=None):
        calls.append(region)
        return _client_returning([_make_farm("farm-eu")])

    with patch.dict(os.environ, {"AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline"}):
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            result = api.list_farms(region="eu-west-1")

    regions_mock.assert_not_called()
    assert calls == ["eu-west-1"]
    assert result["farms"][0]["region"] == "eu-west-1"


def test_list_farms_no_override_still_fans_out(fresh_deadline_config):
    """
    Regression guard: with NO endpoint override set, list_farms still fans out across all
    Deadline regions (the multi-region default behavior is preserved).
    """
    regions = ["us-west-2", "us-east-1", "eu-west-1"]
    clients = {r: _client_returning([_make_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AWS_ENDPOINT_URL_DEADLINE", None)
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(
                _list_apis.config_file, "get_deadline_regions", return_value=regions
            ) as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            result = api.list_farms()

    regions_mock.assert_called()
    assert {f["region"] for f in result["farms"]} == set(regions)


def test_iter_farms_by_region_endpoint_override_scans_single_session_region(fresh_deadline_config):
    """
    The shared generator (the chokepoint the UI uses directly) honors the endpoint
    override: with AWS_ENDPOINT_URL_DEADLINE set and regions=None it yields exactly ONE
    tuple for the session region, builds only one client, and never consults
    get_deadline_regions.
    """
    calls = []

    def fake_get_client(service_name, config=None, region=None):
        calls.append(region)
        return _client_returning([_make_farm("farm-single")])

    mock_session = MagicMock()
    mock_session.region_name = "us-east-1"

    with patch.dict(os.environ, {"AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline"}):
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
            patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            results = list(_list_apis._iter_farms_by_region())

    regions_mock.assert_not_called()
    assert calls == ["us-east-1"]
    assert len(results) == 1
    region, farms, exc = results[0]
    assert region == "us-east-1"
    assert exc is None
    assert farms is not None
    assert [f["region"] for f in farms] == ["us-east-1"]


def test_iter_farms_by_region_endpoint_override_with_explicit_regions_still_fans_out(
    fresh_deadline_config,
):
    """
    With the override set AND an explicit DEADLINE_CLOUD_REGIONS list, the generator does
    NOT short-circuit: it fans out across the explicit region list (user intent wins).
    """
    regions = ["us-west-2", "eu-west-1"]
    clients = {r: _client_returning([_make_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with patch.dict(
        os.environ,
        {
            "AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline",
            "DEADLINE_CLOUD_REGIONS": ",".join(regions),
        },
    ):
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(
                _list_apis.config_file, "get_deadline_regions", return_value=regions
            ) as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            results = list(_list_apis._iter_farms_by_region())

    regions_mock.assert_called()
    by_region = {region: (farms, exc) for region, farms, exc in results}
    assert set(by_region) == set(regions)


def test_iter_farms_by_region_explicit_regions_arg_ignores_override(fresh_deadline_config):
    """
    When the caller passes an explicit regions= argument, the override logic is skipped
    entirely and exactly those regions are scanned (caller is being deliberate), even with
    AWS_ENDPOINT_URL_DEADLINE set. get_deadline_regions / get_boto3_session are not used.
    """
    regions = ["us-west-2", "ap-south-1"]
    clients = {r: _client_returning([_make_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with patch.dict(os.environ, {"AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline"}):
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
            patch.object(_list_apis, "get_boto3_session") as session_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            results = list(_list_apis._iter_farms_by_region(regions=regions))

    regions_mock.assert_not_called()
    session_mock.assert_not_called()
    by_region = {region: (farms, exc) for region, farms, exc in results}
    assert set(by_region) == set(regions)


def test_iter_farms_by_region_no_override_fans_out_via_get_deadline_regions(fresh_deadline_config):
    """
    Regression guard for the generator: with NO override and regions=None, the generator
    resolves the region set via get_deadline_regions() and fans out across it.
    """
    regions = ["us-west-2", "us-east-1", "eu-west-1"]
    clients = {r: _client_returning([_make_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AWS_ENDPOINT_URL_DEADLINE", None)
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(
                _list_apis.config_file, "get_deadline_regions", return_value=regions
            ) as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            results = list(_list_apis._iter_farms_by_region())

    regions_mock.assert_called()
    by_region = {region: (farms, exc) for region, farms, exc in results}
    assert set(by_region) == set(regions)


def test_iter_farms_by_region_endpoint_override_session_region_none(fresh_deadline_config):
    """
    Single-endpoint case with a session that has no region (region_name is None): the
    generator yields exactly one tuple whose region is None, without crashing in the
    fan-out machinery (a None region key is fine).
    """

    def fake_get_client(service_name, config=None, region=None):
        return _client_returning([_make_farm("farm-noregion")])

    mock_session = MagicMock()
    mock_session.region_name = None

    with patch.dict(os.environ, {"AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline"}):
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
            patch.object(_list_apis.config_file, "get_deadline_regions") as regions_mock,
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            results = list(_list_apis._iter_farms_by_region())

    regions_mock.assert_not_called()
    assert len(results) == 1
    region, farms, exc = results[0]
    assert region is None
    assert exc is None
    assert farms is not None
    assert [f["region"] for f in farms] == [None]


def test_list_farms_endpoint_override_single_region_failure_raises(fresh_deadline_config):
    """
    Failure semantics in the single-endpoint case: the generator yields one tuple, and if
    that one call fails there are zero successes, so list_farms raises
    DeadlineOperationError (all regions failed = the one region failed).
    """

    def fake_get_client(service_name, config=None, region=None):
        return _client_raising(RuntimeError("override boom"))

    mock_session = MagicMock()
    mock_session.region_name = "us-east-1"

    with patch.dict(os.environ, {"AWS_ENDPOINT_URL_DEADLINE": "https://override.test/deadline"}):
        os.environ.pop("DEADLINE_CLOUD_REGIONS", None)
        with (
            patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
            patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
            patch.object(_list_apis, "_apply_principal_id_filter"),
        ):
            with pytest.raises(DeadlineOperationError) as excinfo:
                api.list_farms()

    assert "override boom" in str(excinfo.value)


def test_list_farms_does_not_mutate_caller_farm_dicts(fresh_deadline_config):
    """
    region tagging uses a shallow copy ({**farm, "region": r}); the original farm
    dicts returned by the SDK/caller must be left unmodified (no "region" key added).
    """
    original_farms = [
        {"farmId": "farm-a", "displayName": "A"},
        {"farmId": "farm-b", "displayName": "B"},
    ]

    client = MagicMock()
    # Return the SAME dict objects the caller would own (no defensive copy here).
    client.list_farms.return_value = {"farms": original_farms}

    def fake_get_client(service_name, config=None, region=None):
        return client

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms(region="us-west-2")

    # The returned farms are region-tagged...
    assert all(f["region"] == "us-west-2" for f in result["farms"])
    # ...but the original dicts are untouched.
    assert original_farms == [
        {"farmId": "farm-a", "displayName": "A"},
        {"farmId": "farm-b", "displayName": "B"},
    ]
    assert all("region" not in f for f in original_farms)


# ---------------------------------------------------------------------------
# Thread-safety of the fan-out
#
# The fan-out shares one boto3 Session across regions. boto3 Sessions are not safe to
# build clients from concurrently, so every client (and the one-time principal-id DCM
# lookup) is built up front on the calling thread; the worker threads only make the
# network ListFarms call against a ready client. These tests pin that contract.
# ---------------------------------------------------------------------------


def test_iter_farms_by_region_builds_clients_on_calling_thread(fresh_deadline_config):
    """
    get_boto3_client (which touches the shared Session) must run on the thread that
    drives the generator, never inside a worker thread. This is what makes the fan-out
    thread-safe by construction rather than by timing.
    """
    regions = ["us-west-2", "us-east-1", "eu-west-1"]
    driver_thread = threading.get_ident()
    client_build_threads = []
    list_farms_threads = []

    def fake_get_client(service_name, config=None, region=None):
        client_build_threads.append(threading.get_ident())
        client = MagicMock()

        def _list_farms(**kwargs):
            list_farms_threads.append(threading.get_ident())
            return {"farms": [_make_farm(f"farm-{region}")]}

        client.list_farms.side_effect = _list_farms
        return client

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        results = list(_list_apis._iter_farms_by_region())

    assert len(results) == len(regions)
    # Every client was constructed on the driver thread (no Session access off-thread).
    assert client_build_threads == [driver_thread] * len(regions)
    # The actual ListFarms calls did run off the driver thread (the fan-out is real).
    assert list_farms_threads, "expected ListFarms to be called"
    assert all(tid != driver_thread for tid in list_farms_threads)


def test_iter_farms_by_region_principal_filter_applied_once_on_calling_thread(
    fresh_deadline_config,
):
    """
    The principal-id filter does a DCM lookup; it must run exactly once, up front, on the
    calling thread -- not once per worker thread.
    """
    regions = ["us-west-2", "us-east-1", "eu-west-1", "ap-south-1"]
    driver_thread = threading.get_ident()
    filter_threads = []

    def fake_apply_principal(kwargs, config=None):
        filter_threads.append(threading.get_ident())

    def fake_get_client(service_name, config=None, region=None):
        return _client_returning([_make_farm(f"farm-{region}")])

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter", side_effect=fake_apply_principal),
    ):
        list(_list_apis._iter_farms_by_region())

    assert filter_threads == [driver_thread], "principal filter must run once, on the driver thread"


def test_iter_farms_by_region_high_concurrency_stress(fresh_deadline_config):
    """
    Stress the fan-out across many regions with a client factory that is deliberately NOT
    safe to call concurrently: it asserts it is only ever entered from one thread at a
    time. Because clients are built serially up front, this never trips; if a future change
    moved client construction back into the workers, this test would flake/fail.

    The per-region ListFarms calls themselves run concurrently (verified by observing more
    than one distinct worker thread), so this exercises real parallelism on the call path.
    """
    regions = [f"region-{i:02d}" for i in range(30)]
    build_lock = threading.Lock()
    concurrent_builds = 0
    max_concurrent_builds = 0
    call_threads = set()
    call_threads_lock = threading.Lock()

    def make_list_farms(region):
        def _list_farms(**kwargs):
            with call_threads_lock:
                call_threads.add(threading.get_ident())
            time.sleep(0.002)  # let calls overlap across the pool
            return {"farms": [_make_farm(f"farm-{region}")]}

        return _list_farms

    def fake_get_client(service_name, config=None, region=None):
        # Detect any overlapping client construction: if two threads were ever inside this
        # factory at once, current would exceed 1 and we'd fail right here.
        nonlocal concurrent_builds, max_concurrent_builds
        with build_lock:
            concurrent_builds += 1
            max_concurrent_builds = max(max_concurrent_builds, concurrent_builds)
            current = concurrent_builds
        assert current == 1, "client construction overlapped across threads"
        try:
            time.sleep(0.001)  # hold the "build" open so any overlap would be observed
            client = MagicMock()
            client.list_farms.side_effect = make_list_farms(region)
            return client
        finally:
            with build_lock:
                concurrent_builds -= 1

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        results = list(_list_apis._iter_farms_by_region())

    # All regions returned, each correctly tagged, no exceptions captured.
    assert len(results) == len(regions)
    by_region = {region: (farms, exc) for region, farms, exc in results}
    assert set(by_region) == set(regions)
    for region, (farms, exc) in by_region.items():
        assert exc is None
        assert farms is not None and farms[0]["region"] == region
    # Client construction never overlapped (serial, up front)...
    assert max_concurrent_builds == 1
    # ...while the network calls genuinely ran in parallel across the worker pool.
    assert len(call_threads) > 1


# ---------------------------------------------------------------------------
# Cross-region customer-managed-key (CMK) filtering
#
# Farms encrypted with a customer-managed KMS key (kmsKeyArn) are not yet supported
# across regions -- the backend can't serve their detail/follow-up calls from a region
# other than the one the farm lives in. So the fan-out hides a CMK farm when it lives
# outside the monitor's own region (the boto3 session/profile region). A farm in the
# monitor's own region is ALWAYS kept, CMK or not; the only farms ever removed are
# cross-region CMK farms. Filtering happens in _iter_farms_by_region -- the shared
# chokepoint -- so the CLI (list_farms) and the GUI streaming path stay in lockstep.
# ---------------------------------------------------------------------------


def _patch_monitor_creds(is_monitor=True):
    """
    Patches get_credentials_source so the fan-out believes it is (or isn't) running with
    Deadline Cloud Monitor credentials. The CMK filter only applies to monitor creds.
    """
    from deadline.client.api._session import AwsCredentialsSource

    source = (
        AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
        if is_monitor
        else AwsCredentialsSource.HOST_PROVIDED
    )
    return patch.object(_list_apis, "get_credentials_source", return_value=source)


class TestIsFarmVisibleAcrossRegions:
    """Unit tests for the predicate itself, with emphasis on current-region safety."""

    MONITOR_REGION = "us-west-2"
    OTHER_REGION = "eu-west-1"

    def test_keeps_current_region_farm_without_cmk(self):
        farm = {"region": self.MONITOR_REGION}
        assert _list_apis._is_farm_visible_across_regions(farm, self.MONITOR_REGION) is True

    def test_keeps_current_region_farm_with_cmk(self):
        # A CMK farm in the monitor's own region is fully supported; the CMK must not
        # cause it to be filtered out.
        farm = {"region": self.MONITOR_REGION, "kmsKeyArn": "arn:aws:kms:us-west-2:1234:key/x"}
        assert _list_apis._is_farm_visible_across_regions(farm, self.MONITOR_REGION) is True

    def test_keeps_cross_region_farm_without_cmk(self):
        farm = {"region": self.OTHER_REGION}
        assert _list_apis._is_farm_visible_across_regions(farm, self.MONITOR_REGION) is True

    def test_hides_cross_region_farm_with_cmk(self):
        # The only case the filter removes.
        farm = {"region": self.OTHER_REGION, "kmsKeyArn": "arn:aws:kms:eu-west-1:1234:key/x"}
        assert _list_apis._is_farm_visible_across_regions(farm, self.MONITOR_REGION) is False

    def test_keeps_cross_region_farm_with_empty_kms_arn(self):
        # An empty arn is not a real customer-managed key, so the farm stays.
        farm = {"region": self.OTHER_REGION, "kmsKeyArn": ""}
        assert _list_apis._is_farm_visible_across_regions(farm, self.MONITOR_REGION) is True

    def test_keeps_farm_with_missing_kms_arn_key(self):
        # kmsKeyArn absent entirely (service-managed key) -> kept anywhere.
        farm = {"region": self.OTHER_REGION}
        assert _list_apis._is_farm_visible_across_regions(farm, self.MONITOR_REGION) is True

    def test_none_monitor_region_treated_as_its_own_region(self):
        # The endpoint-override path tags farms with region=None; a None monitor region
        # must match so those farms are never spuriously hidden.
        farm = {"region": None, "kmsKeyArn": "arn:aws:kms:::key/x"}
        assert _list_apis._is_farm_visible_across_regions(farm, None) is True


def test_list_farms_hides_cross_region_cmk_farm_keeps_current_region(fresh_deadline_config):
    """
    The same CMK farm surfaced by every region survives only in the monitor's own region;
    its cross-region copies are hidden. Proves the current region is never affected.
    """
    regions = ["us-west-2", "us-east-1", "eu-west-1"]
    monitor_region = "us-west-2"
    # Every region reports the identical CMK farm (tagged with that region downstream).
    clients = {r: _client_returning([_make_cmk_farm("farm-cmk")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    mock_session = MagicMock()
    mock_session.region_name = monitor_region

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
        _patch_monitor_creds(),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    farms = result["farms"]
    # Kept exactly once -- for the monitor's own region only.
    assert [f["region"] for f in farms] == [monitor_region]
    assert farms[0]["farmId"] == "farm-cmk"


def test_list_farms_keeps_non_cmk_farm_in_every_region(fresh_deadline_config):
    """A farm without a customer-managed key is surfaced from every region (nothing hidden)."""
    regions = ["us-west-2", "us-east-1", "eu-west-1"]
    clients = {r: _client_returning([_make_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    mock_session = MagicMock()
    mock_session.region_name = "us-west-2"

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
        _patch_monitor_creds(),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    assert {f["region"] for f in result["farms"]} == set(regions)


def test_list_farms_mixed_cmk_and_non_cmk_in_cross_region(fresh_deadline_config):
    """
    In a non-monitor region, a CMK farm is hidden while a sibling non-CMK farm from the
    same region is kept -- filtering is per-farm, not per-region.
    """
    regions = ["us-west-2", "eu-west-1"]
    monitor_region = "us-west-2"
    clients = {
        "us-west-2": _client_returning([_make_farm("farm-home")]),
        "eu-west-1": _client_returning([_make_cmk_farm("farm-eu-cmk"), _make_farm("farm-eu-open")]),
    }

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    mock_session = MagicMock()
    mock_session.region_name = monitor_region

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
        _patch_monitor_creds(),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    by_id = {f["farmId"] for f in result["farms"]}
    assert by_id == {"farm-home", "farm-eu-open"}
    assert "farm-eu-cmk" not in by_id


def test_iter_farms_by_region_hides_cross_region_cmk_farm(fresh_deadline_config):
    """
    The shared generator (used directly by the GUI) applies the same CMK filter: the
    cross-region CMK copy is dropped, the monitor-region copy is yielded.
    """
    regions = ["us-west-2", "eu-west-1"]
    monitor_region = "us-west-2"
    clients = {r: _client_returning([_make_cmk_farm("farm-cmk")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    mock_session = MagicMock()
    mock_session.region_name = monitor_region

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
        _patch_monitor_creds(),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        results = list(_list_apis._iter_farms_by_region(regions=regions))

    by_region = {region: farms for region, farms, exc in results}
    # Monitor region keeps its CMK farm; the other region's copy is filtered to empty.
    west_farms = by_region["us-west-2"]
    assert west_farms is not None
    assert [f["farmId"] for f in west_farms] == ["farm-cmk"]
    assert by_region["eu-west-1"] == []


def test_list_farms_does_not_filter_cmk_farms_for_non_monitor_creds(fresh_deadline_config):
    """
    The CMK filter is gated on Deadline Cloud Monitor credentials. With host-provided
    (BYO) credentials, a cross-region CMK farm must NOT be hidden -- those callers are
    responsible for their own region scoping and we must never silently drop their farms.
    """
    regions = ["us-west-2", "eu-west-1"]
    # A CMK farm lives in a region other than the session region; under monitor creds this
    # would be hidden, but here the creds are host-provided so it must survive.
    clients = {r: _client_returning([_make_cmk_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    mock_session = MagicMock()
    mock_session.region_name = "us-west-2"

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
        _patch_monitor_creds(is_monitor=False),
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    # Nothing filtered: every region's CMK farm is present.
    assert {f["region"] for f in result["farms"]} == set(regions)


def test_list_farms_monitor_creds_no_session_region_fails_open(fresh_deadline_config):
    """
    Fail-open guard: under monitor credentials but with no resolved session region, we
    can't tell which farms are cross-region, so NOTHING is filtered. Without this, a
    None monitor region would match no concrete region tag and hide EVERY CMK farm --
    including the user's home region.
    """
    regions = ["us-west-2", "eu-west-1"]
    clients = {r: _client_returning([_make_cmk_farm(f"farm-{r}")]) for r in regions}

    def fake_get_client(service_name, config=None, region=None):
        return clients[region]

    mock_session = MagicMock()
    mock_session.region_name = None  # no resolved default region

    with (
        patch.object(_list_apis, "get_boto3_client", side_effect=fake_get_client),
        patch.object(_list_apis, "get_boto3_session", return_value=mock_session),
        _patch_monitor_creds(),  # monitor creds, but region is unknown
        patch.object(_list_apis.config_file, "get_deadline_regions", return_value=regions),
        patch.object(_list_apis, "_apply_principal_id_filter"),
    ):
        result = api.list_farms()

    # Every CMK farm survives despite being "cross-region" -- we failed open.
    assert {f["region"] for f in result["farms"]} == set(regions)
