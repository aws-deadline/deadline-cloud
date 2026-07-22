# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Helpers for constructing AWS Deadline Cloud monitor (web console) URLs.

The primary entry point, :func:`build_monitor_url`, is a pure URL *formatter*: it
takes the pieces of a resource path and returns the corresponding monitor URL.
It never makes network calls -- the caller must supply the monitor ``subdomain``
(available from the ``deadline:GetMonitor`` API's ``subdomain``/``url`` fields,
or the Deadline Cloud console under Monitor details).

The full host of a monitor is ``subdomain.Region.deadlinecloud.amazonaws.com``.
Resource paths mirror the routes the monitor web app itself uses:

- All farms:  ``/farms``
- Farm:       ``/<region>/farms/<farm_id>``
- Queue:      ``/<region>/farms/<farm_id>/queues/<queue_id>``
- A job/step/task is selected on the queue page via ``jobId``/``stepId``/``taskId``
  query parameters.
"""

from __future__ import annotations

from configparser import ConfigParser
from typing import Optional
from urllib.parse import quote, urlencode, urlsplit

from botocore.client import BaseClient  # type: ignore[import]

from ._session import (
    AwsCredentialsSource,
    _resolve_region,
    get_boto3_session,
    get_credentials_source,
    get_monitor_id,
    get_session_client,
)

__all__ = ["build_monitor_url"]

# The parent domain shared by every Deadline Cloud monitor. The full monitor
# host is "<subdomain>.<region>.<_MONITOR_DOMAIN>".
_MONITOR_DOMAIN = "deadlinecloud.amazonaws.com"


def build_monitor_url(
    subdomain: str,
    region: Optional[str] = None,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    job_id: Optional[str] = None,
    step_id: Optional[str] = None,
    task_id: Optional[str] = None,
    *,
    monitor_region: Optional[str] = None,
    host: Optional[str] = None,
    config: Optional[ConfigParser] = None,
) -> str:
    """
    Builds the AWS Deadline Cloud monitor URL for a resource.

    This is a pure formatter and makes no network calls. The ``subdomain`` is
    required; it is the monitor-specific label in the host
    ``<subdomain>.<region>.deadlinecloud.amazonaws.com`` and can be read from the
    ``deadline:GetMonitor`` API (its ``subdomain`` field) or the Deadline Cloud
    console.

    Two regions are involved and may differ for a cross-region farm: the
    *monitor* region appears in the host, while the *farm* (resource) region
    appears in the path. ``monitor_region`` controls the host; ``region``
    controls the path. When ``monitor_region`` is omitted it defaults to
    ``region`` -- the common single-region case, where both are the same.

    The resource identifiers form a hierarchy -- ``queue_id`` requires ``farm_id``,
    ``job_id`` requires ``queue_id``, ``step_id`` requires ``job_id``, and
    ``task_id`` requires ``step_id``. Supplying a lower-level id without its
    parent (for example a ``queue_id`` with no ``farm_id``) raises ``ValueError``.
    With ``farm_id`` omitted the URL points at the "all farms" list.

    Example:
        ```python
        from deadline.client.api import build_monitor_url

        # Queue URL
        build_monitor_url("mymonitor", "us-east-1", farm_id="farm-1234", queue_id="queue-5678")
        # 'https://mymonitor.us-east-1.deadlinecloud.amazonaws.com/us-east-1/farms/farm-1234/queues/queue-5678'

        # Task URL (job + step + task selected on the queue page)
        build_monitor_url(
            "mymonitor", "us-east-1",
            farm_id="farm-1234", queue_id="queue-5678",
            job_id="job-9abc", step_id="step-def0", task_id="task-def0-2",
        )

        # Cross-region: a monitor in us-east-1 linking to a farm in eu-west-1
        build_monitor_url(
            "mymonitor", "eu-west-1", farm_id="farm-1234", monitor_region="us-east-1"
        )
        # 'https://mymonitor.us-east-1.deadlinecloud.amazonaws.com/eu-west-1/farms/farm-1234'
        ```

    Args:
        subdomain (str): The monitor subdomain (required).
        region (str, optional): The AWS region of the resource (farm). Used in the
            URL path. When omitted, it is resolved the same way as elsewhere in the
            client: an explicit value wins, otherwise the ``defaults.farm_region``
            setting is used.
        farm_id (str, optional): The farm to link to.
        queue_id (str, optional): The queue to link to. Requires ``farm_id``.
        job_id (str, optional): The job to select. Requires ``queue_id``.
        step_id (str, optional): The step to select. Requires ``job_id``.
        task_id (str, optional): The task to select. Requires ``step_id``.
        monitor_region (str, optional): The region the monitor itself lives in, used
            in the host. Defaults to ``region`` (single-region case). Pass this when
            the farm is in a different region than the monitor. Ignored when ``host``
            is supplied.
        host (str, optional): The monitor host to use verbatim (scheme + netloc, e.g.
            ``https://mymonitor.us-east-1.deadlinecloud.amazonaws.com``), such as the
            ``url`` returned by ``deadline:GetMonitor``. When given, it is used as-is
            instead of reconstructing the host from ``subdomain`` + region + the
            commercial domain -- necessary for non-commercial partitions (aws-us-gov,
            aws-cn) whose hosts differ. A trailing slash is stripped.
        config (ConfigParser, optional): The AWS Deadline Cloud configuration object
            to use when resolving the region.

    Returns:
        str: The fully-qualified monitor URL.

    Raises:
        ValueError: If ``subdomain`` is empty, the resource hierarchy is violated,
            or no region can be resolved.
    """
    if not subdomain:
        raise ValueError("A monitor subdomain is required to build a monitor URL.")

    # Validate the resource hierarchy: each id requires its parent.
    if queue_id and not farm_id:
        raise ValueError("queue_id requires farm_id to build a monitor URL.")
    if job_id and not queue_id:
        raise ValueError("job_id requires queue_id to build a monitor URL.")
    if step_id and not job_id:
        raise ValueError("step_id requires job_id to build a monitor URL.")
    if task_id and not step_id:
        raise ValueError("task_id requires step_id to build a monitor URL.")

    resolved_region = _resolve_region(config=config, region=region)
    if not resolved_region:
        raise ValueError(
            "A region is required to build a monitor URL. Pass region= explicitly or "
            "configure defaults.farm_region."
        )
    # Prefer an authoritative host when the caller has one (the ``url`` from
    # ``deadline:GetMonitor``). Reconstructing from the hardcoded commercial domain
    # would produce a wrong host in other partitions (aws-us-gov, aws-cn), where the
    # real monitor host does not end in ``deadlinecloud.amazonaws.com``. When no host
    # is supplied, fall back to building one from the monitor's own region -- which
    # may differ from the farm's (cross-region) -- defaulting to the farm region for
    # the common single-region case.
    if host:
        host = host.rstrip("/")
    else:
        host_region = monitor_region or resolved_region
        host = f"https://{subdomain}.{host_region}.{_MONITOR_DOMAIN}"

    # No farm -> the "all farms" list, which is not region-scoped in the path.
    if not farm_id:
        return f"{host}/farms"

    path = f"/{resolved_region}/farms/{quote(farm_id)}"
    if queue_id:
        path += f"/queues/{quote(queue_id)}"
        # A job/step/task is selected on the queue page via query parameters.
        query = [
            (key, value)
            for key, value in (("jobId", job_id), ("stepId", step_id), ("taskId", task_id))
            if value
        ]
        if query:
            path += "?" + urlencode(query)

    return host + path


def _parse_monitor_host(url: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Parse ``(subdomain, region)`` out of a monitor URL of the form
    ``https://<subdomain>.<region>.deadlinecloud.amazonaws.com``.

    Returns ``(None, None)`` if the URL is missing or doesn't match that shape.
    """
    if not url:
        return None, None
    host = urlsplit(url).netloc or url
    suffix = "." + _MONITOR_DOMAIN
    if not host.endswith(suffix):
        return None, None
    label = host[: -len(suffix)]
    # label is "<subdomain>.<region>"
    subdomain, _, region = label.partition(".")
    if not subdomain or not region:
        return None, None
    return subdomain, region


def _monitor_host_of(url: Optional[str]) -> Optional[str]:
    """
    Extract the ``scheme://netloc`` host from a monitor ``url``, or ``None`` when the
    url is missing or has no host. Unlike :func:`_parse_monitor_host` this makes no
    assumptions about the domain, so it works in every partition (aws-us-gov, aws-cn).
    """
    if not url:
        return None
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        return None
    return f"{parts.scheme}://{parts.netloc}"


def _get_monitor_client(config: Optional[ConfigParser] = None) -> BaseClient:
    """
    A deadline client for calling ``deadline:GetMonitor``, scoped to the profile's own
    region -- where Deadline Cloud monitor wrote the profile, i.e. the monitor's home
    region. Deliberately not ``get_boto3_client``, whose region resolution prefers
    ``defaults.farm_region``: a monitor is a regional resource, and in a cross-region
    setup a farm-region endpoint would report the monitor as not found.
    """
    session = get_boto3_session(config=config)
    return get_session_client(session=session, service_name="deadline")


def _get_monitor_subdomain(
    config: Optional[ConfigParser] = None,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Best-effort lookup of the monitor ``(subdomain, monitor_region, host)`` for the
    current profile.

    Returns these only when the credentials were written by Deadline Cloud monitor (so
    a monitor exists to link to) and the ``deadline:GetMonitor`` call returns a usable
    ``subdomain``. Returns ``(None, None, None)`` otherwise. ``host`` is the
    authoritative ``scheme://netloc`` from the monitor's ``url`` (preferred over a
    reconstructed host); ``monitor_region`` is parsed from that same url. Either may be
    ``None`` if the ``url`` field is absent or malformed. Never raises.
    """
    if get_credentials_source(config=config) != AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
        return None, None, None
    monitor_id = get_monitor_id(config=config)
    if not monitor_id:
        return None, None, None
    monitor = _get_monitor_client(config=config).get_monitor(monitorId=monitor_id)
    subdomain = monitor.get("subdomain")
    if not subdomain:
        return None, None, None
    url = monitor.get("url")
    _, monitor_region = _parse_monitor_host(url)
    return subdomain, monitor_region, _monitor_host_of(url)


def _get_job_monitor_url(
    config: Optional[ConfigParser] = None,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    job_id: Optional[str] = None,
) -> Optional[str]:
    """
    Best-effort monitor URL for a just-submitted job.

    Returns ``None`` (rather than raising) whenever the URL can't be built -- for
    example when the credentials didn't come from Deadline Cloud monitor, the
    ``deadline:GetMonitor`` call fails or returns no subdomain, or a region can't be
    resolved. This keeps URL generation from ever interfering with job submission
    itself.

    Note there is deliberately no way to supply a deadline client: the ``GetMonitor``
    lookup must run against the monitor's home region (the profile region), while
    clients used for submission are scoped to the farm's region, which can differ.
    """
    try:
        subdomain, monitor_region, monitor_host = _get_monitor_subdomain(config=config)
        if not subdomain:
            return None
        # Resource (path) region: the configured farm region if set, otherwise fall
        # back to the monitor's own region (correct for the common single-region
        # case, where farm_region is often not configured at all).
        resource_region = _resolve_region(config=config) or monitor_region
        # Prefer the authoritative host from GetMonitor so the URL is correct in every
        # partition (aws-us-gov, aws-cn); build_monitor_url falls back to reconstructing
        # it from subdomain + region when the monitor url was absent/malformed.
        return build_monitor_url(
            subdomain,
            region=resource_region,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id=job_id,
            monitor_region=monitor_region,
            host=monitor_host,
            config=config,
        )
    except Exception:
        return None
