# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides functionality for boto3 Sessions, Clients, and properties
of the Deadline-configured IAM credentials.
"""

from __future__ import annotations

import logging
import os
from configparser import ConfigParser
from contextlib import contextmanager
from enum import Enum
from functools import lru_cache
from typing import Optional, Tuple

import boto3  # type: ignore[import]
import botocore
import botocore.config
from botocore.client import BaseClient  # type: ignore[import]
from botocore.credentials import CredentialProvider, RefreshableCredentials
from botocore.exceptions import (  # type: ignore[import]
    ClientError,
    ProfileNotFound,
)
from botocore.session import get_session as get_botocore_session

from .. import version
from ._agent_detection import detect_invoking_agent
from ..config import get_setting, set_setting, config_file
from ..exceptions import DeadlineOperationError
from ...job_attachments._aws.aws_clients import get_s3_client


class AwsCredentialsSource(Enum):
    NOT_VALID = 0
    HOST_PROVIDED = 2
    DEADLINE_CLOUD_MONITOR_LOGIN = 3


class AwsAuthenticationStatus(Enum):
    CONFIGURATION_ERROR = 1
    AUTHENTICATED = 2
    NEEDS_LOGIN = 3


# Place for stashing context to be attached to boto clients.
session_context: dict[str, Optional[str]] = {
    "submitter-name": None,
    "submitter-version": None,
    "cli-command-name": None,
}


def get_boto3_session(
    force_refresh: bool = False,
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
) -> boto3.Session:
    """
    Gets a boto3 session for the AWS Deadline Cloud aws profile from the local
    configuration `~/.deadline/config`. This may either use a named profile
    or the default credentials provider chain.

    This implementation caches the session object for use across multiple calls
    unless `force_refresh` is set to True.

    Args:
        force_refresh (bool, optional): If set to True, forces a cache refresh.
        config (ConfigParser, optional): If provided, the AWS Deadline Cloud config to use.
        region (str, optional): If provided, returns a session scoped to this region (so
            boto3 resolves the regional Deadline endpoint itself). When omitted, the
            profile's default region is used (today's behavior).
    """

    profile_name: Optional[str] = get_setting("defaults.aws_profile_name", config)

    # If the default AWS profile name is either not set, or set to "default",
    # use the default credentials provider chain instead of a named profile.
    if profile_name in ("(default)", "default", ""):
        profile_name = None

    if force_refresh:
        invalidate_boto3_session_cache()

    session = _get_boto3_session_for_profile(profile_name, region)
    # Apply proxy / CA-bundle settings once per cached session. Idempotent: a later
    # call with a different config that resolves to the same cached session keeps the
    # proxy/CA first applied (these are machine-level, not per-config, settings).
    return apply_proxy_settings(session, config=config)


@lru_cache
def _get_boto3_session_for_profile(profile_name: str, region: Optional[str] = None):
    session = boto3.Session(profile_name=profile_name, region_name=region)

    # By default, DCM returns creds that expire after 15 minutes, and boto3's RefreshableCredentials
    # class refreshes creds that are within 15 minutes of expiring, so credentials would never be reused.
    # Also DCM credentials currently take several seconds to refresh. Lower the refresh timeouts so creds
    # are reused between API calls to save time.
    # See https://github.com/boto/botocore/blob/develop/botocore/credentials.py#L342-L362

    try:
        credentials = session.get_credentials()
        if (
            isinstance(credentials, RefreshableCredentials)
            and credentials.method == "custom-process"
        ):
            credentials._advisory_refresh_timeout = 5 * 60  # 5 minutes
            credentials._mandatory_refresh_timeout = 2.5 * 60  # 2.5 minutes
    except:  # noqa: E722
        # Attempt to patch the timeouts but ignore any errors. These patched proeprties are internal and could change
        # without notice. Creds are functional without patching timeouts.
        pass

    return session


def invalidate_boto3_session_cache() -> None:
    _get_boto3_session_for_profile.cache_clear()
    _get_queue_user_boto3_session.cache_clear()


def get_default_client_config(**kwargs) -> botocore.config.Config:
    """
    Gets the default botocore Config object to use with `boto3 clients`.
    This method adds user agent version and submitter context into botocore calls.
    Additional arguments are forwarded to the Config constructor.
    """
    user_agent_extra = f"app/deadline-client#{version}"
    if session_context.get("submitter-name"):
        submitter_extra = f" submitter/{session_context['submitter-name']}"
        if session_context.get("submitter-version"):
            submitter_extra += f"#{session_context['submitter-version']}"
        user_agent_extra += submitter_extra
    if session_context.get("cli-command-name"):
        user_agent_extra += f" cli-command/{session_context['cli-command-name']}"
    # Attribute AI-agent-driven invocations on the service-side User-Agent so they
    # are distinguishable in service logs (complements the RUM telemetry tagging).
    agent_name = detect_invoking_agent()
    if agent_name:
        user_agent_extra += f" invoked-by/{agent_name}"

    client_config = botocore.config.Config(user_agent_extra=user_agent_extra, **kwargs)
    return client_config


def _resolve_https_proxy(config: Optional[ConfigParser] = None) -> Optional[str]:
    """
    Return the configured HTTPS proxy URL (``settings.https_proxy``), or ``None``
    when it is unset.
    """
    https_proxy = config_file.get_setting("settings.https_proxy", config=config)
    if https_proxy and https_proxy.strip():
        return https_proxy.strip()
    return None


def _resolve_ca_bundle(config: Optional[ConfigParser] = None) -> Optional[str]:
    """
    Return the configured CA certificate bundle path (``settings.ca_bundle``),
    or ``None`` when it is unset.
    """
    ca_bundle = config_file.get_setting("settings.ca_bundle", config=config)
    if ca_bundle and ca_bundle.strip():
        # ``settings.ca_bundle`` is declared ``is_path: True``, but ``get_setting``
        # only normalizes slashes -- it does not expand ``~``. Expand it here so a
        # value like ``~/certs/ca.pem`` resolves before being handed to boto3's
        # ``verify`` (botocore does not expand ``~`` either).
        return os.path.expanduser(ca_bundle.strip())
    return None


# Attribute stashing the (https_proxy, ca_bundle) tuple that was first applied to a
# boto3.Session, so apply_proxy_settings stays idempotent (sessions are cached and
# reused across many client builds) and can detect when a later config disagrees with
# the settings already baked into the session and its already-built clients.
_PROXY_SETTINGS_APPLIED_ATTR = "_deadline_applied_proxy_settings"


def apply_proxy_settings(
    session: boto3.Session, config: Optional[ConfigParser] = None
) -> boto3.Session:
    """
    Apply the ``settings.https_proxy`` and ``settings.ca_bundle`` config settings to
    ``session`` so that *every* client built from it -- the Deadline Cloud clients
    created here as well as the S3 / Deadline clients job_attachments builds from the
    same session -- routes through the configured proxy and verifies TLS against the
    configured CA bundle.

    Both settings are applied once, at the session level, rather than threaded through
    every client-creation call: botocore merges a session's default client config into
    every per-client ``botocore.config.Config`` (so ``proxies`` is inherited) and reads
    the session's ``ca_bundle`` config variable for the client's ``verify`` value. This
    gives uniform proxy/CA coverage with no per-client plumbing.

    Proxy and CA bundle are process-global, machine-level settings (a host has one
    corporate proxy / trust store), so -- unlike region, which is resolved per farm --
    they are taken from the *first* ``config`` that configures a given cached session.
    Sessions are cached on ``(profile, region)`` (see ``get_boto3_session``), so a later
    call with a different ``config`` that resolves to the same cached session does not
    re-apply or override the proxy/CA already set on it -- clients already built from the
    session captured the original values at build time, so silently changing the session
    now would split-brain old and new clients. A later config that *disagrees* with the
    already-applied settings is therefore ignored with a warning. Pass
    ``force_refresh=True`` to ``get_boto3_session`` to drop the cached session and pick up
    changed settings cleanly.

    Args:
        session: The boto3 session to configure, modified in place.
        config: An optional AWS Deadline Cloud ConfigParser to resolve the settings
            from. When ``None``, the on-disk default config is read.

    Returns:
        The same ``session``, for convenient chaining.
    """
    resolved = (_resolve_https_proxy(config), _resolve_ca_bundle(config))

    applied = getattr(session, _PROXY_SETTINGS_APPLIED_ATTR, None)
    if applied is not None:
        # Already configured (first config wins). Warn if a later config disagrees,
        # since the session and its already-built clients keep the original values.
        if resolved != applied:
            # Log only *which* setting changed, not the values: a proxy URL can embed
            # basic-auth credentials (http://user:pass@host), and writing those to a
            # WARNING-level log risks leaking them to shared/aggregated log stores.
            changed = []
            if resolved[0] != applied[0]:
                changed.append("https_proxy")
            if resolved[1] != applied[1]:
                changed.append("ca_bundle")
            logging.getLogger(__name__).warning(
                "Ignoring changed %s for an already-configured boto3 session; keeping the "
                "values applied first. Proxy and CA bundle are machine-level settings fixed "
                "per session -- use force_refresh to apply changed settings.",
                " and ".join(changed),
            )
        return session
    setattr(session, _PROXY_SETTINGS_APPLIED_ATTR, resolved)

    https_proxy, ca_bundle = resolved
    botocore_session = session._session
    if https_proxy:
        existing = botocore_session.get_default_client_config() or botocore.config.Config()
        # Set both schemes so the proxy is honored regardless of the endpoint's scheme.
        botocore_session.set_default_client_config(
            existing.merge(
                botocore.config.Config(proxies={"http": https_proxy, "https": https_proxy})
            )
        )

    if ca_bundle:
        # botocore feeds the session's ``ca_bundle`` config variable into each client's
        # ``verify``. (_resolve_ca_bundle has already expanded ``~``.)
        botocore_session.set_config_variable("ca_bundle", ca_bundle)

    return session


@lru_cache
def get_session_client(session: boto3.Session, service_name: str, region: Optional[str] = None):
    """
    Create and cache a boto3 client for the given session, service name, and region.

    This function is decorated with @lru_cache to ensure that repeated calls
    with the same session, service name, and region return the cached client to
    avoid repeating initialization where possible. The ``region`` argument is part
    of the cache key so clients for different regions are never reused for each other.

    The ``settings.https_proxy`` / ``settings.ca_bundle`` config settings are applied
    to the session (see ``apply_proxy_settings``), not here, so the created client
    inherits them automatically via botocore's config merge.

    When a profile has a non-standard endpoint override (e.g. via the ``[services ...]``
    section or ``AWS_ENDPOINT_URL*`` env vars), boto3 applies it regardless of the
    ``region_name`` passed to ``.client()``. This causes SigV4 credential-scope mismatches
    for cross-region calls. We detect this by inspecting the resolved endpoint and
    re-creating the client with the regionalized URL if needed.

    Args:
        session: The boto3 Session to use for creating the client
        service_name: The name of the AWS service (e.g., 'deadline', 's3')
        region: The AWS region to create the client for. If None, the session's
            default region is used.

    Returns:
        A boto3 client for the specified service
    """
    if region is None:
        return session.client(service_name, config=get_default_client_config())

    client = session.client(service_name, config=get_default_client_config(), region_name=region)
    resolved = client.meta.endpoint_url
    session_region = session.region_name
    # An override leaked the session's region into a cross-region endpoint.
    if region not in resolved and session_region and session_region in resolved:
        return session.client(
            service_name,
            config=get_default_client_config(),
            region_name=region,
            endpoint_url=resolved.replace(session_region, region, 1),
        )
    return client


def _resolve_region(
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
    farm_id: Optional[str] = None,
) -> Optional[str]:
    """
    Resolves which AWS region to use, following this precedence:
    1. An explicit ``region`` argument.
    2. The ``defaults.farm_region`` config setting, if set (non-empty).
    3. ``None`` (let boto3/the session decide, preserving single-region behavior).

    Contract / region-value convention: this normalizes "no region" to ``None`` and never
    returns ``""``. Both an explicit ``region=""`` and an empty ``defaults.farm_region`` are
    treated as "not set" (the falsy ``if region:`` / ``if farm_region:`` checks below).
    Callers downstream of resolution therefore test ``if region is not None:`` -- a
    resolved region is either ``None`` (use the session/profile default) or a real region.
    Raw, pre-resolution inputs (CLI args, config reads) may legitimately be ``""`` and use a
    truthy ``if region:`` check instead; the two conventions are intentional, not arbitrary.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud config to use.
        region (str, optional): An explicit region override.
        farm_id (str, optional): The farm the region is being resolved for. When given,
            the per-farm ``defaults.farm_region`` is looked up for *this* farm (which may
            differ from the default farm). When ``None``, the default farm's region is used.

    Returns:
        The resolved region name, or ``None`` if nothing is configured (never ``""``).
    """
    # Truthy (not "is not None"): an explicit region="" means "not provided", so fall through.
    if region:
        return region

    farm_region = _get_farm_region_setting(config=config, farm_id=farm_id)
    if farm_region:
        return farm_region

    return None


def _get_farm_region_setting(
    config: Optional[ConfigParser] = None,
    farm_id: Optional[str] = None,
) -> str:
    """
    Reads ``defaults.farm_region`` for a specific farm.

    ``defaults.farm_region`` is keyed per farm (it depends on ``defaults.farm_id``), so to
    read a non-default farm's region we read it against a config whose ``defaults.farm_id``
    is set to that farm. When ``farm_id`` is ``None`` (or matches the default), the live
    config is read directly. Returns ``""`` when no region is stored.
    """
    if farm_id is None:
        return get_setting("defaults.farm_region", config=config)

    # Read against a copy with defaults.farm_id overridden to the target farm, so the
    # per-farm section lookup resolves to that farm rather than the default one.
    if config is None:
        config = config_file.read_config()
    farm_scoped_config = ConfigParser()
    farm_scoped_config.read_dict(config)
    set_setting("defaults.farm_id", farm_id, config=farm_scoped_config)
    return get_setting("defaults.farm_region", config=farm_scoped_config)


def get_boto3_client(
    service_name: str,
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
) -> BaseClient:
    """
    Gets a client from the boto3 session returned by `get_boto3_session`.

    Args:
        service_name (str): The AWS service to get the client for, e.g. "deadline".
        config (ConfigParser, optional): If provided, the AWS Deadline Cloud config to use.
        region (str, optional): The AWS region to create the client for. When omitted,
            the region is resolved from `defaults.farm_region` (if set), otherwise the
            session/profile region is used.
    """

    session = get_boto3_session(config=config)
    resolved_region = _resolve_region(config=config, region=region)
    return get_session_client(session=session, service_name=service_name, region=resolved_region)


def get_credentials_source(
    config: Optional[ConfigParser] = None,
) -> AwsCredentialsSource:
    """
    Returns DEADLINE_CLOUD_MONITOR_LOGIN if Deadline Cloud monitor wrote the credentials, HOST_PROVIDED otherwise.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    try:
        session = get_boto3_session(config=config)
        profile_config = session._session.get_scoped_config()
    except ProfileNotFound:
        return AwsCredentialsSource.NOT_VALID
    if "monitor_id" in profile_config:
        # Deadline Cloud monitor Desktop adds the "monitor_id" key
        return AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
    else:
        return AwsCredentialsSource.HOST_PROVIDED


def get_user_and_identity_store_id(
    config: Optional[ConfigParser] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    If logged in with Deadline Cloud monitor Desktop, returns a tuple
    (user_id, identity_store_id), otherwise returns None.
    """
    session = get_boto3_session(config=config)
    profile_config = session._session.get_scoped_config()

    if "monitor_id" in profile_config:
        return (profile_config["user_id"], profile_config["identity_store_id"])
    else:
        return None, None


def get_monitor_id(
    config: Optional[ConfigParser] = None,
) -> Optional[str]:
    """
    If logged in with Deadline Cloud Monitor to a Deadline Monitor, returns Monitor Id, otherwise returns None.
    """
    session = get_boto3_session(config=config)
    profile_config = session._session.get_scoped_config()

    return profile_config.get("monitor_id", None)


def get_queue_user_boto3_session(
    deadline: BaseClient,
    config: Optional[ConfigParser] = None,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    queue_display_name: Optional[str] = None,
    force_refresh: bool = False,
) -> boto3.Session:
    """
    Calls the AssumeQueueRoleForUser API to obtain the role configured in a Queue,
    and then creates and returns a boto3 session with those credentials.

    Args:
        deadline (BaseClient): A Deadline client.
        config (ConfigParser, optional): If provided, the AWS Deadline Cloud config to use.
        farm_id (str, optional): The ID of the farm to use.
        queue_id (str, optional): The ID of the queue to use.
        queue_display_name (str, optional): The display name of the queue.
        force_refresh (bool, optional): If True, forces a cache refresh.
    """

    base_session = get_boto3_session(config=config, force_refresh=force_refresh)

    if farm_id is None:
        farm_id = get_setting("defaults.farm_id")
    if queue_id is None:
        queue_id = get_setting("defaults.queue_id")

    # Resolve the region to scope the queue-user session to, for this specific farm.
    # When nothing is configured, _resolve_region returns None and we fall back to the
    # base session's region.
    region = _resolve_region(config=config, farm_id=farm_id)

    queue_user_session = _get_queue_user_boto3_session(
        deadline, base_session, farm_id, queue_id, queue_display_name, region
    )
    # Apply proxy / CA-bundle to the queue-user session too, so clients job_attachments
    # builds from it (S3 uploads/downloads) route through the proxy and trust the CA
    # bundle. Idempotent per cached session.
    return apply_proxy_settings(queue_user_session, config=config)


@lru_cache
def _get_queue_user_boto3_session(
    deadline: BaseClient,
    base_session: boto3.Session,
    farm_id: str,
    queue_id: str,
    queue_display_name: Optional[str] = None,
    region: Optional[str] = None,
):
    queue_credential_provider = QueueUserCredentialProvider(
        deadline,
        farm_id,
        queue_id,
        queue_display_name,
    )

    botocore_session = get_botocore_session()
    credential_provider = botocore_session.get_component("credential_provider")
    credential_provider.insert_before("env", queue_credential_provider)
    aws_profile_name: Optional[str] = None
    if base_session.profile_name != "default":
        aws_profile_name = base_session.profile_name

    return boto3.Session(
        botocore_session=botocore_session,
        profile_name=aws_profile_name,
        region_name=region if region is not None else base_session.region_name,
    )


@contextmanager
def _modified_logging_level(logger, level):
    old_level = logger.getEffectiveLevel()
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old_level)


def _list_farms_for_auth_probe(config: Optional[ConfigParser] = None) -> None:
    """
    Makes a minimal ``deadline:ListFarms`` call used as an auth/reachability
    probe by :func:`check_authentication_status` and
    :func:`check_deadline_api_available`.

    For Deadline Cloud monitor profiles, injects ``principalId`` so the call
    is scoped to the caller's IdC user (matching the :func:`api.list_farms`
    wrapper). Without it, IdC-issued sessions get denied by the service and
    the auth-login poll loop never resolves to AUTHENTICATED.

    Raises whatever exception the underlying boto3 call raises; callers are
    responsible for exception handling.
    """
    from ._list_apis import _apply_principal_id_filter

    list_farm_params: dict = {"maxResults": 1}
    _apply_principal_id_filter(list_farm_params, config=config)
    get_boto3_client("deadline", config=config).list_farms(**list_farm_params)


def check_authentication_status(
    config: Optional[ConfigParser] = None,
) -> AwsAuthenticationStatus:
    """
    Checks the status of the provided session by making a small ``deadline:ListFarms``
    API call. This validates both that credentials are usable and that the session can
    reach the Deadline Cloud API.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.

    Returns AwsAuthenticationStatus enum value:
      - CONFIGURATION_ERROR if there is an unexpected error accessing credentials
      - AUTHENTICATED if they are fine
      - NEEDS_LOGIN if a Deadline Cloud monitor login is required.
    """

    with _modified_logging_level(logging.getLogger("botocore.credentials"), logging.ERROR):
        try:
            _list_farms_for_auth_probe(config=config)
            return AwsAuthenticationStatus.AUTHENTICATED
        except Exception:
            # We assume that the presence of a Deadline Cloud monitor profile
            # means we will know everything necessary to start it and login.

            if get_credentials_source(config) == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
                return AwsAuthenticationStatus.NEEDS_LOGIN
            return AwsAuthenticationStatus.CONFIGURATION_ERROR


def precache_clients(
    deadline: BaseClient = None,
    config: Optional[ConfigParser] = None,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    queue_display_name: Optional[str] = None,
) -> Tuple[BaseClient, BaseClient]:
    """
    Initialize an S3 client (and optionally a Deadline client) with queue user credentials
    to pre-warm the client cache.

    This function creates an S3 client using queue user credentials, which triggers
    the expensive service discovery process once. Subsequent client creations using
    the same session object should then use the cached client, improving performance.
    This function is designed to be called in a background thread at application startup.

    Args:
        deadline: An existing deadline client. If None, one will be created.
        config: Optional configuration parser. If None, the default configuration will be used.
        farm_id: The farm ID. If None, it will be retrieved from settings.
        queue_id: The queue ID. If None, it will be retrieved from settings.
        queue_display_name: The queue display name. If None, it will be retrieved from the queue.

    Returns:
        Created (or current) s3 client for the given queue_role_session

    Example:
        ```
        # Fire and forget initialization in a background thread
        import threading
        threading.Thread(
            target=initialize_queue_user_s3_client,
            daemon=True,
            name="S3ClientInit"
        ).start()
        ```
    """
    if not deadline:
        deadline = get_boto3_client("deadline", config=config)
    if not queue_id:
        queue_id = get_setting("defaults.queue_id", config=config)
    if not farm_id:
        farm_id = get_setting("defaults.farm_id", config=config)

    if not queue_display_name:
        queue = deadline.get_queue(
            farmId=farm_id,
            queueId=queue_id,
        )
        queue_display_name = queue["displayName"]

    queue_role_session = get_queue_user_boto3_session(
        deadline=deadline,
        config=config,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue_display_name,
    )
    # Initialize the S3 client to populate the cache
    s3_max_pool_connections = int(config_file.get_setting("settings.s3_max_pool_connections"))
    return deadline, get_s3_client(
        queue_role_session, s3_max_pool_connections=s3_max_pool_connections
    )


class QueueUserCredentialProvider(CredentialProvider):
    """A custom botocore CredentialProvider for handling AssumeQueueRoleForUser API
    credentials. If the credentials expire, the provider will automatically refresh
    them using the _get_queue_credentials method.
    """

    # The following two constants are part of botocore's CredentialProvider interface

    # A short name to identify the provider within botocore.
    METHOD = "queue-credential-provider"
    # A name to identify the provider for use in cross-sdk features. The AWS SDKs
    # require that providers outside of botocore are prefixed with "custom"
    CANONICAL_NAME = "custom-queue-credential-provider"

    deadline: BaseClient
    farm_id: str
    queue_id: str
    queue_display_name_or_id: Optional[str]

    def __init__(
        self,
        deadline: BaseClient,
        farm_id: str,
        queue_id: str,
        queue_display_name: Optional[str] = None,
    ):
        self.deadline = deadline
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.queue_display_name_or_id = queue_display_name or queue_id

    def load(self):
        credentials = self._get_queue_credentials()
        return RefreshableCredentials.create_from_metadata(
            metadata=credentials,
            refresh_using=self._get_queue_credentials,
            method=self.METHOD,
        )

    def _get_queue_credentials(self):
        """
        Fetches or refreshes the credentials using the AssumeQueueRoleForUser API
        for the specified Farm ID and Queue ID.
        """
        try:
            queue_credentials = self.deadline.assume_queue_role_for_user(
                farmId=self.farm_id, queueId=self.queue_id
            ).get("credentials", None)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", None)
            if code == "ThrottlingException":
                raise DeadlineOperationError(
                    f"Throttled while attempting to assume Queue role for user on Queue '{self.queue_display_name_or_id}': {exc}\n"
                    "Please retry the operation later, or contact your administrator to increase the API's rate limit."
                ) from exc
            elif code == "InternalServerException":
                raise DeadlineOperationError(
                    f"An internal server error occurred while attempting to assume Queue role for user on "
                    f"Queue '{self.queue_display_name_or_id}': {exc}\n"
                ) from exc
            else:
                raise DeadlineOperationError(
                    f"Failed to assume Queue role for user on Queue '{self.queue_display_name_or_id}': {exc}\nPlease contact your "
                    "administrator to ensure a Queue role exists and that you have permissions to access this Queue."
                ) from exc
        if not queue_credentials:
            raise DeadlineOperationError(
                f"Failed to get credentials for '{self.queue_display_name_or_id}': Empty credentials received."
            )
        return {
            "access_key": queue_credentials["accessKeyId"],
            "secret_key": queue_credentials["secretAccessKey"],
            "token": queue_credentials["sessionToken"],
            "expiry_time": queue_credentials["expiration"].isoformat(),
        }
