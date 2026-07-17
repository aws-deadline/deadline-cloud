# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import concurrent.futures
import logging
import os
from configparser import ConfigParser
from typing import Iterator, List, Optional, Tuple

from ._session import (
    AwsCredentialsSource,
    get_boto3_client,
    get_boto3_session,
    get_credentials_source,
    get_user_and_identity_store_id,
)
from .. import api
from ..config import config_file
from ..exceptions import DeadlineOperationError

logger = logging.getLogger(__name__)

# Upper bound on the number of regions queried concurrently during a fan-out.
_MAX_FANOUT_WORKERS = 10


def _apply_principal_id_filter(kwargs, config=None):
    """Injects ``principalId`` into *kwargs* when the active profile has a DCM user_id."""
    if "principalId" not in kwargs:
        user_id, _ = get_user_and_identity_store_id(config=config)
        if user_id:
            kwargs["principalId"] = user_id


def _call_paginated_deadline_list_api(list_api, list_property_name, **kwargs):
    """
    Calls a deadline:List* API repeatedly to concatenate all pages.

    Example:
        deadline = get_boto3_client("deadline")
        return _call_paginated_deadline_list_api(deadline.list_farms, "farms", **kwargs)

    Args:
      list_api (callable): The List* API function to call, from the boto3 client.
      list_property_name (str): The name of the property in the response that contains
                                the list.
    """
    response = list_api(**kwargs)
    result = {list_property_name: response[list_property_name]}

    while "nextToken" in response:
        response = list_api(nextToken=response["nextToken"], **kwargs)
        result[list_property_name].extend(response[list_property_name])

    return result


def _has_explicit_region_list(config: Optional[ConfigParser] = None) -> bool:
    """
    Returns True when the user has explicitly listed the Deadline regions to scan via
    the ``DEADLINE_CLOUD_REGIONS`` env var or the ``settings.deadline_regions`` config
    setting.

    This deliberate user intent takes precedence over the endpoint-override
    single-region short-circuit in :func:`list_farms`.

    Args:
        config (ConfigParser, optional): The config to read the ``settings.deadline_regions``
            override from; threaded through so an in-memory config (e.g. a ``--profile``
            override) is honored rather than the global on-disk config.
    """
    env_value = os.getenv(config_file.DEADLINE_REGIONS_ENV_VAR)
    if env_value and env_value.strip():
        return True
    config_value = config_file.get_setting("settings.deadline_regions", config=config)
    return bool(config_value and config_value.strip())


def _is_farm_visible_across_regions(farm: dict, monitor_region: Optional[str]) -> bool:
    """
    Whether a farm should be shown by the all-farms fan-out.

    Farms with a customer-managed KMS key (``kmsKeyArn``) aren't yet supported across
    regions, so a CMK farm is hidden when it lives outside the monitor's own region. A
    farm in the monitor's region is always kept -- CMK farms are only ever hidden
    cross-region.
    """
    return farm.get("region") == monitor_region or not farm.get("kmsKeyArn")


def _list_farms_with_client(
    region: Optional[str],
    deadline,
    **call_kwargs,
) -> List[dict]:
    """
    Runs ``deadline:ListFarms`` against an already-built client and tags each farm.

    This is the fan-out's per-region unit of work. It takes a ready-made ``deadline``
    client rather than building one, so it touches **no** shared boto3 ``Session`` state
    and is therefore safe to run from a worker thread by construction -- the only thing it
    does is make the (network) ListFarms call and shallow-copy/tag the results. All session
    and client construction happens up front on the calling thread (see
    :func:`_iter_farms_by_region`).

    Args:
        region (str, optional): The region this client is scoped to; used only to tag the
            returned farms (``None`` for the endpoint-override single-region path).
        deadline: A boto3 ``deadline`` client already scoped to ``region``.

    Returns:
        The list of farm dicts for ``region``, each annotated with ``farm["region"]``.
    """
    result = _call_paginated_deadline_list_api(deadline.list_farms, "farms", **call_kwargs)
    # Tag a shallow copy of each farm so we don't mutate dicts owned by the caller/SDK.
    return [{**farm, "region": region} for farm in result["farms"]]


def _list_farms_for_region(
    region: Optional[str],
    config: Optional[ConfigParser] = None,
    **kwargs,
) -> List[dict]:
    """
    Single-region ``deadline:ListFarms`` (client built here, then delegated).

    Used by the single-region path of :func:`list_farms`, where there is no concurrency.
    Builds a region-scoped client, applies the principal-id filter, and delegates to
    :func:`_list_farms_with_client`.

    Args:
        region (str, optional): The AWS region to list farms in. When ``None``, the
            session/profile default region is used and the ``region`` tag on each farm
            is ``None`` (used by the endpoint-override single-region path).
        config (ConfigParser, optional): The AWS Deadline Cloud config to use.

    Returns:
        The list of farm dicts for ``region``, each annotated with ``farm["region"]``.
    """
    call_kwargs = dict(kwargs)
    _apply_principal_id_filter(call_kwargs, config=config)
    deadline = get_boto3_client("deadline", config=config, region=region)
    return _list_farms_with_client(region, deadline, **call_kwargs)


def _iter_farms_by_region(
    config: Optional[ConfigParser] = None,
    regions: Optional[List[str]] = None,
    **kwargs,
) -> Iterator[Tuple[str, Optional[List[dict]], Optional[BaseException]]]:
    """
    Fans ``deadline:ListFarms`` out across Deadline Cloud regions concurrently, yielding
    each region's result *as it completes* (out of order) so consumers like the UI can
    render partial results without waiting on the slowest region.

    Thread-safety: every per-region boto3 client (and the principal-id filter, which does a
    one-time DCM lookup) is built **up front on this thread**, before the executor starts.
    The worker threads receive a ready client and only make the network ListFarms call --
    they never touch the shared boto3 ``Session`` (which is not thread-safe to build clients
    from concurrently). This makes the fan-out safe by construction rather than by timing.

    This is the shared chokepoint for the region-set decision, so the CLI
    (:func:`list_farms`) and the GUI streaming path behave identically. When ``regions`` is
    passed, exactly those are scanned. When ``None``, regions come from
    :func:`config_file.get_deadline_regions` -- except that when ``AWS_ENDPOINT_URL_DEADLINE``
    is set (a single explicit endpoint makes a fan-out meaningless, since every client would
    hit the same endpoint) only the session region is scanned, unless the user has also
    explicitly listed regions (see :func:`_has_explicit_region_list`), which wins.

    Yields ``(region, farms, None)`` on success (each farm carries a ``region`` key) or
    ``(region, None, exception)`` on failure.
    """
    if regions is None:
        if os.getenv(
            config_file.AWS_ENDPOINT_URL_DEADLINE_ENV_VAR
        ) and not _has_explicit_region_list(config=config):
            # Single explicit endpoint: scan only the session region. It may be None
            # (the client builder accepts region=None), scanned as a 1-element list so
            # the fan-out machinery below handles it uniformly.
            try:
                session_region = get_boto3_session(config=config).region_name
            except Exception:
                session_region = None
            regions = [session_region]
        else:
            regions = config_file.get_deadline_regions(config=config)

    if not regions:
        return

    # Build everything that touches the shared boto3 Session up front, on this thread:
    # the principal-id filter (one DCM lookup, region-independent) and one region-scoped
    # client per region. The worker threads below only make the network call against a
    # ready client, so they never construct clients off the shared Session concurrently.
    call_kwargs = dict(kwargs)
    _apply_principal_id_filter(call_kwargs, config=config)
    clients_by_region = {
        region: get_boto3_client("deadline", config=config, region=region) for region in regions
    }

    # The CMK filter applies only to Deadline Cloud Monitor credentials; BYO credentials
    # are single-region, so there's nothing cross-region to hide. Resolved up front here
    # (the calling thread), not in the workers.
    monitor_region = (
        get_boto3_session(config=config).region_name
        if get_credentials_source(config=config)
        == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
        else None
    )
    # Fail open: only filter when we know BOTH that these are monitor credentials AND the
    # monitor's own region. Without a resolved region we can't tell which farms are
    # cross-region, so we hide nothing rather than risk hiding home-region CMK farms.
    filter_cmk_farms = monitor_region is not None

    max_workers = min(len(regions), _MAX_FANOUT_WORKERS)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_region = {
            executor.submit(
                _list_farms_with_client, region, clients_by_region[region], **call_kwargs
            ): region
            for region in regions
        }
        for future in concurrent.futures.as_completed(future_to_region):
            region = future_to_region[future]
            try:
                farms = future.result()
            except Exception as exc:
                # A per-region failure (auth, opt-in, throttling, timeout, etc.) is
                # reported and skipped. Control-flow exceptions (KeyboardInterrupt,
                # SystemExit) are NOT Exception subclasses, so they propagate and the
                # operation can be interrupted rather than silently swallowed.
                yield (region, None, exc)
            else:
                # Filter at this shared chokepoint so the CLI and GUI paths stay in sync.
                if filter_cmk_farms:
                    farms = [f for f in farms if _is_farm_visible_across_regions(f, monitor_region)]
                yield (region, farms, None)


@api.record_function_latency_telemetry_event()
def list_farms(config=None, region=None, **kwargs):
    """
    Calls the [deadline:ListFarms] API, paginating to return all farms. Each farm dict is
    annotated with an additive ``region`` key.

    When ``region`` is given, only that region is queried. When ``None``, ListFarms is
    fanned out across regions via :func:`_iter_farms_by_region` (which owns the region-set
    decision, including the ``AWS_ENDPOINT_URL_DEADLINE`` single-region short-circuit) and
    the results are concatenated.

    Fan-out failure semantics: a failing region is skipped with a ``logger.warning`` and
    does not block others; survivors are returned as long as one region succeeds; if every
    region fails a :class:`DeadlineOperationError` is raised (an empty list is never
    silently returned).

    [deadline:ListFarms]: https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_ListFarms.html
    """
    if region is not None:
        return {"farms": _list_farms_for_region(region, config=config, **kwargs)}

    all_farms: List[dict] = []
    failures: List[Tuple[str, BaseException]] = []
    succeeded = False

    for result_region, farms, exc in _iter_farms_by_region(config=config, **kwargs):
        if exc is not None:
            logger.warning("Failed to list farms in region %s: %s", result_region, exc)
            failures.append((result_region, exc))
        else:
            succeeded = True
            all_farms.extend(farms or [])

    if not succeeded and failures:
        summary = "; ".join(f"{failed_region}: {failure}" for failed_region, failure in failures)
        raise DeadlineOperationError(
            f"Failed to list farms in all {len(failures)} region(s): {summary}"
        )

    return {"farms": all_farms}


@api.record_function_latency_telemetry_event()
def list_queues(config=None, region=None, **kwargs):
    """
    Calls the [deadline:ListQueues] API call, applying the filter for user membership
    depending on the configuration. If the response is paginated, it repeated
    calls the API to get all the queues.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud config to use.
        region (str, optional): The AWS region the farm lives in. When omitted, the
            region is resolved from `defaults.farm_region`, otherwise the session/profile region.

    [deadline:ListQueues]: https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_ListQueues.html
    """
    _apply_principal_id_filter(kwargs, config=config)
    deadline = get_boto3_client("deadline", config=config, region=region)
    return _call_paginated_deadline_list_api(deadline.list_queues, "queues", **kwargs)


@api.record_function_latency_telemetry_event()
def list_jobs(config=None, region=None, **kwargs):
    """
    Calls the [deadline:ListJobs] API call, applying the filter for user membership
    depending on the configuration. If the response is paginated, it repeated
    calls the API to get all the jobs.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud config to use.
        region (str, optional): The AWS region the farm lives in. When omitted, the
            region is resolved from `defaults.farm_region`, otherwise the session/profile region.

    [deadline:ListJobs]: https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_ListJobs.html
    """
    _apply_principal_id_filter(kwargs, config=config)
    deadline = get_boto3_client("deadline", config=config, region=region)
    return _call_paginated_deadline_list_api(deadline.list_jobs, "jobs", **kwargs)


@api.record_function_latency_telemetry_event()
def list_fleets(config=None, region=None, **kwargs):
    """
    Calls the [deadline:ListFleets] API call, applying the filter for user membership
    depending on the configuration. If the response is paginated, it repeated
    calls the API to get all the fleets.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud config to use.
        region (str, optional): The AWS region the farm lives in. When omitted, the
            region is resolved from `defaults.farm_region`, otherwise the session/profile region.

    [deadline:ListFleets]: https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_ListFleets.html
    """
    _apply_principal_id_filter(kwargs, config=config)
    deadline = get_boto3_client("deadline", config=config, region=region)
    return _call_paginated_deadline_list_api(deadline.list_fleets, "fleets", **kwargs)


@api.record_function_latency_telemetry_event()
def list_storage_profiles_for_queue(config=None, region=None, **kwargs):
    """
    Calls the [deadline:ListStorageProfilesForQueue] API call. If the response is paginated, it repeated
    calls the API to get all the storage profiles.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud config to use.
        region (str, optional): The AWS region the farm lives in. When omitted, the
            region is resolved from `defaults.farm_region`, otherwise the session/profile region.

    [deadline:ListStorageProfilesForQueue]: https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_ListStorageProfilesForQueue.html
    """
    deadline = get_boto3_client("deadline", config=config, region=region)

    return _call_paginated_deadline_list_api(
        deadline.list_storage_profiles_for_queue, "storageProfiles", **kwargs
    )
