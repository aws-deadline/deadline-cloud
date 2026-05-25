# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import atexit
from functools import lru_cache, wraps
import json
import logging
import os
import platform
import uuid
import random
import sys
import time

from botocore.config import Config as BotocoreConfig
from configparser import ConfigParser
from dataclasses import asdict, dataclass, field
from datetime import datetime
from queue import Queue, Full
from threading import Thread
from typing import Any, Callable, Dict, Optional, TypeVar, cast
from urllib import request, error

from ...job_attachments.progress_tracker import SummaryStatistics

from ._session import (
    get_monitor_id,
    get_user_and_identity_store_id,
    get_boto3_client,
    get_boto3_session,
)
from ._stack_trace_sanitizer import sanitize_exception
from ..config import config_file
from .. import version

__cached_telemetry_clients: Dict[str, "TelemetryClient"] = {}

logger = logging.getLogger(__name__)


# Generic function return type.
F = TypeVar("F", bound=Callable[..., Any])


def _swallow_exceptions(func: F) -> F:
    """Decorator that catches all exceptions in telemetry functions to prevent
    telemetry issues from affecting the main application flow."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.debug(
                "Swallowed exception in telemetry function %s", func.__name__, exc_info=True
            )
            return None

    return cast(F, wrapper)


def get_deadline_endpoint_url(
    config: Optional[ConfigParser] = None,
) -> str:
    # Use boto3's built-in logic to get the correct endpoint URL
    client = get_boto3_client("deadline", config=config)
    return client.meta.endpoint_url


@dataclass
class TelemetryEvent:
    """Base class for telemetry events"""

    event_type: str = "com.amazon.rum.deadline.uncategorized"
    event_details: Dict[str, Any] = field(default_factory=dict)


class TelemetryClient:
    """
    Sends telemetry events periodically to the Deadline Cloud telemetry service.

    This client holds a queue of events which is written to synchronously, and processed
    asynchronously, where events are sent in the background, so that it does not slow
    down user interactivity.

    Telemetry events contain non-personally-identifiable information that helps us
    understand how users interact with our software so we know what features our
    customers use, and/or what existing pain points are.

    Data is aggregated across a session ID (a UUID created at runtime), used to mark every
    telemetry event for the lifetime of the application), and a 'telemetry identifier' (a
    UUID recorded in the configuration file), to aggregate data across multiple application
    lifetimes on the same machine.

    Telemetry collection can be opted-out of by running:
    'deadline config set "telemetry.opt_out" true' or setting the environment variable
    'DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true'
    """

    # Used for backing off requests if we encounter errors from the service.
    # See https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    MAX_QUEUE_SIZE = 25
    BASE_TIME = 0.5
    MAX_BACKOFF_SECONDS = 10  # The maximum amount of time to wait between retries
    MAX_RETRY_ATTEMPTS = 4

    ENDPOINT_PREFIX = "management."

    def __init__(
        self,
        package_name: str,
        package_ver: str,
        config: Optional[ConfigParser] = None,
    ):
        # Instance-level dicts so every TelemetryClient has its own state (avoid
        # the mutable-class-attribute pitfall where updates would be shared by
        # all instances).
        self._common_details: Dict[str, Any] = {}
        self._system_metadata: Dict[str, Any] = {}

        self._initialized: bool = False
        self.package_name = package_name
        self.package_ver = ".".join(package_ver.split(".")[:3])

        # IDs for this session
        self.session_id: str = str(uuid.uuid4())
        try:
            self.telemetry_id: str = self._get_telemetry_identifier(config=config)
        except Exception:
            logger.debug("Swallowed exception in telemetry __init__", exc_info=True)
            self.telemetry_id = str(uuid.uuid4())
        # If a different base package is provided, include info from this library as supplementary info
        if package_name != "deadline-cloud-library":
            self._common_details["deadline-cloud-version"] = version
        try:
            self._system_metadata = self._get_system_metadata(config=config)
        except Exception:
            logger.debug("Swallowed exception in telemetry __init__", exc_info=True)
            self._system_metadata = {}
        self.set_opt_out(config=config)
        self.initialize(config=config)

    @_swallow_exceptions
    def set_opt_out(self, config: Optional[ConfigParser] = None) -> None:
        """
        Checks whether telemetry has been opted out by checking the DEADLINE_CLOUD_TELEMETRY_OPT_OUT
        environment variable and the 'telemetry.opt_out' config file setting.
        Note the environment variable supersedes the config file setting.
        """
        env_var_value = os.environ.get("DEADLINE_CLOUD_TELEMETRY_OPT_OUT")
        if env_var_value:
            self.telemetry_opted_out = env_var_value in config_file._TRUE_VALUES
        else:
            self.telemetry_opted_out = config_file.str2bool(
                config_file.get_setting("telemetry.opt_out", config=config)
            )
        logger.info(
            "Deadline Cloud telemetry is "
            + ("not enabled." if self.telemetry_opted_out else "enabled.")
        )

    @_swallow_exceptions
    def initialize(self, config: Optional[ConfigParser] = None) -> None:
        """
        Starts up the telemetry background thread after getting settings from the boto3 client.
        Note that if this is called before boto3 is successfully configured / initialized,
        an error can be raised. In that case we silently fail and don't mark the client as
        initialized.
        """
        if self.telemetry_opted_out:
            return

        self.endpoint: str = self._get_prefixed_endpoint(
            f"{get_deadline_endpoint_url(config=config)}/2023-10-12/telemetry",
            TelemetryClient.ENDPOINT_PREFIX,
        )

        # Some environments might not have SSL, so we'll use the vendored botocore SSL context
        from botocore.httpsession import create_urllib3_context, get_cert_path

        self._urllib3_context = create_urllib3_context()
        self._urllib3_context.load_verify_locations(cafile=get_cert_path(True))

        user_id, _ = get_user_and_identity_store_id(config=config)
        if user_id:
            self._system_metadata["user_id"] = user_id

        monitor_id: Optional[str] = get_monitor_id(config=config)
        if monitor_id:
            self._system_metadata["monitor_id"] = monitor_id

        self._initialized = True
        self._start_threads()

    def record_error_with_trace(
        self,
        exc: BaseException,
        exception_scope: str,
        extra_details: Optional[dict] = None,
        from_gui: bool = False,
    ) -> None:
        event_details: dict = {
            "exception_type": type(exc).__qualname__,
            "exception_scope": exception_scope,
            "stack_trace": sanitize_exception(exc),
        }
        if extra_details:
            event_details.update(extra_details)

        self.record_event(
            event_type="com.amazon.rum.deadline.error",
            event_details=event_details,
            from_gui=from_gui,
        )

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    def _get_prefixed_endpoint(self, endpoint: str, prefix: str) -> str:
        """Insert the prefix right after 'https://'"""
        if endpoint.startswith("https://"):
            prefixed_endpoint = endpoint[:8] + prefix + endpoint[8:]
            return prefixed_endpoint
        return endpoint

    def _get_telemetry_identifier(self, config: Optional[ConfigParser] = None):
        identifier = config_file.get_setting("telemetry.identifier", config=config)
        try:
            uuid.UUID(identifier, version=4)
        except ValueError:  # Thrown if the user_id isn't in UUID4 format
            identifier = str(uuid.uuid4())
            config_file.set_setting("telemetry.identifier", identifier)
        return identifier

    def _start_threads(self) -> None:
        """Set up background threads for shutdown checking and request sending"""
        self.event_queue: Queue[Optional[TelemetryEvent]] = Queue(
            maxsize=TelemetryClient.MAX_QUEUE_SIZE
        )
        atexit.register(self._exit_cleanly)
        self.processing_thread: Thread = Thread(
            target=self._process_event_queue_thread, daemon=True
        )
        self.processing_thread.start()

    def _get_system_metadata(self, config: Optional[ConfigParser]) -> Dict[str, Any]:
        """
        Builds up a dict of non-identifiable metadata about the system environment.

        This will be used in the Rum event metadata, which has a limit of 10 unique values.
        """
        platform_info = platform.uname()
        metadata: Dict[str, Any] = {
            "service": self.package_name,
            "version": self.package_ver,
            "python_version": platform.python_version(),
            "osName": "macOS" if platform_info.system == "Darwin" else platform_info.system,
            "osVersion": platform_info.release,
        }

        return metadata

    @_swallow_exceptions
    def _exit_cleanly(self):
        try:
            self.event_queue.put_nowait(None)
        except Full:
            # If the queue is full, it may mean the telemetry processing thread has already joined
            # since it is daemon and the Python runtime will shut it down on exit.
            # Ignore the error, since this is a best-effort cleanup.
            pass
        self.processing_thread.join()

    def _send_request(self, req: request.Request) -> None:
        attempts = 0
        success = False
        while not success:
            try:
                with request.urlopen(req, context=self._urllib3_context):
                    logger.debug("Successfully sent telemetry.")
                    success = True
            except error.HTTPError as httpe:
                if httpe.code == 429 or httpe.code == 500:
                    logger.debug(f"Error received from service. Waiting to retry: {str(httpe)}")

                    attempts += 1
                    if attempts >= TelemetryClient.MAX_RETRY_ATTEMPTS:
                        raise Exception("Max retries reached sending telemetry")

                    backoff_sleep = random.uniform(
                        0,
                        min(
                            TelemetryClient.MAX_BACKOFF_SECONDS,
                            TelemetryClient.BASE_TIME * 2**attempts,
                        ),
                    )
                    time.sleep(backoff_sleep)
                else:  # Reraise any exceptions we didn't expect
                    raise

    def _process_event_queue_thread(self):
        """Background thread for processing the telemetry event data queue and sending telemetry requests."""
        # Resolve the AWS account ID once on this background thread so callers
        # of record_event() are never blocked (e.g. by a slow STS timeout on a
        # restricted network). The resolved value is stored on _common_details
        # and merged into every event's payload below, so events enqueued
        # before resolution completes still include the account ID when sent.
        account_id = self.get_account_id(get_boto3_session())
        if account_id:
            self.update_common_details({"accountId": account_id})

        while True:
            # Blocks until we get a new entry in the queue
            event_data: Optional[TelemetryEvent] = self.event_queue.get()
            # We've received the shutdown signal
            if event_data is None:
                return

            headers = {"Accept": "application-json", "Content-Type": "application-json"}
            try:
                # Merge _common_details into the per-event details at send
                # time (not enqueue time) so late-resolved fields like
                # accountId are included.
                details = {**event_data.event_details, **self._common_details}
                request_body = {
                    "BatchId": str(uuid.uuid4()),
                    "RumEvents": [
                        {
                            "details": str(json.dumps(details)),
                            "id": str(uuid.uuid4()),
                            "metadata": str(json.dumps(self._system_metadata)),
                            "timestamp": int(datetime.now().timestamp()),
                            "type": event_data.event_type,
                        },
                    ],
                    "UserDetails": {"sessionId": self.session_id, "userId": self.telemetry_id},
                }
                request_body_encoded = str(json.dumps(request_body)).encode("utf-8")
            except Exception as exc:
                logger.debug(f"Failed to serialize telemetry data. {str(exc)}")
                continue

            req = request.Request(url=self.endpoint, data=request_body_encoded, headers=headers)
            try:
                logger.debug("Sending telemetry data: %s", request_body)
                self._send_request(req)
            except Exception as exc:
                # Swallow any kind of uncaught exception and stop sending telemetry
                logger.debug(f"Error received from service. {str(exc)}")
                return
            self.event_queue.task_done()

    def _put_telemetry_record(self, event: TelemetryEvent) -> None:
        if not self._initialized or self.telemetry_opted_out:
            return
        try:
            self.event_queue.put_nowait(event)
        except Full:
            # Silently swallow the error if the event queue is full (due to throttling of the service)
            pass

    def record_vfs_mounting(self, successfully_mounted: bool):
        details: Dict[str, Any] = {"successfully_mounted": successfully_mounted}
        event_type = "com.amazon.rum.deadline.job_attachments.vfs_mount"
        self.record_event(event_type=event_type, event_details=details, from_gui=False)

    def _record_summary_statistics(
        self, event_type: str, summary: SummaryStatistics, from_gui: bool
    ):
        details: Dict[str, Any] = asdict(summary)
        self.record_event(event_type=event_type, event_details=details, from_gui=from_gui)

    def record_hashing_summary(self, summary: SummaryStatistics, *, from_gui: bool = False):
        self._record_summary_statistics(
            "com.amazon.rum.deadline.job_attachments.hashing_summary", summary, from_gui
        )

    def record_upload_summary(self, summary: SummaryStatistics, *, from_gui: bool = False):
        self._record_summary_statistics(
            "com.amazon.rum.deadline.job_attachments.upload_summary", summary, from_gui
        )

    def record_error(
        self, event_details: Dict[str, Any], exception_type: str, from_gui: bool = False
    ):
        event_details["exception_type"] = exception_type
        # Possibility to add stack trace here
        self.record_event("com.amazon.rum.deadline.error", event_details, from_gui=from_gui)

    @_swallow_exceptions
    def record_event(
        self, event_type: str, event_details: Dict[str, Any], *, from_gui: bool = False
    ):
        event_details["usage_mode"] = "GUI" if from_gui else "CLI"
        self._put_telemetry_record(
            TelemetryEvent(
                event_type=event_type,
                event_details=event_details,
            )
        )

    @lru_cache
    def get_account_id(self, boto3_session) -> Optional[str]:
        """Best-effort AWS account ID lookup for telemetry, cached per
        (client, session) so it runs at most once per telemetry client
        instance.

        Prefers ``session.get_credentials().account_id`` (populated for free
        by SSO, AssumeRole, IMDS/ECS, or a ``credential_process`` that emits
        ``AccountId``). Deadline Cloud monitor delivers its credentials
        through ``credential_process`` (see ``_get_boto3_session_for_profile``
        in ``_session.py``), so if DCM's process output includes
        ``AccountId`` this fast path covers the common monitor user flow
        without an STS call. Falls back to ``sts:GetCallerIdentity`` with a
        short timeout, returning ``None`` on any failure so users on
        restricted networks without STS access can still run the CLI.
        """
        try:
            credentials = boto3_session.get_credentials()
            account_id = getattr(credentials, "account_id", None) if credentials else None
            if account_id:
                return account_id
            # Short-timeout best-effort fallback; runs on the telemetry background
            # thread so blocking is fine.
            sts = boto3_session.client(
                "sts",
                config=BotocoreConfig(
                    connect_timeout=2, read_timeout=2, retries={"max_attempts": 1}
                ),
            )
            return sts.get_caller_identity()["Account"]
        except Exception:
            logger.debug("Could not resolve account ID for telemetry", exc_info=True)
            return None

    def update_common_details(self, details: Dict[str, Any]):
        """Updates the dict of common data that is included in every telemetry request."""
        self._common_details.update(details)


def get_telemetry_client(
    package_name: str, package_ver: str, config: Optional[ConfigParser] = None
) -> TelemetryClient:
    """
    Retrieves the cached telemetry client, lazy-loading the first time this is called.
    :param package_name: Base package name to associate data by.
    :param package_ver: Base package version to associate data by.
    :param config: Optional configuration to use for the client. Loads defaults if not given.
    :return: Telemetry client to make requests with.
    """
    global __cached_telemetry_clients
    cached = __cached_telemetry_clients.get(package_name)
    if not cached:
        cached = TelemetryClient(
            package_name=package_name,
            package_ver=package_ver,
            config=config,
        )
        __cached_telemetry_clients[package_name] = cached
    elif not cached.is_initialized:
        cached.initialize(config=config)

    return cached


def get_deadline_cloud_library_telemetry_client(
    config: Optional[ConfigParser] = None,
) -> TelemetryClient:
    """
    Retrieves the cached telemetry client, specifying the Deadline Cloud Client Library's package information.
    :param config: Optional configuration to use for the client. Loads defaults if not given.
    :return: Telemetry client to make requests with.
    """
    return get_telemetry_client("deadline-cloud-library", version, config=config)


def record_success_fail_telemetry_event(**decorator_kwargs: Any) -> Callable[[F], F]:
    """
    Decorator to try catch a function. Sends a success / fail telemetry event.
    :param ** Python variable arguments. See https://docs.python.org/3/glossary.html#term-parameter.
    """

    def inner(function: F) -> F:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            Wrapper to try-catch a function for telemetry
            :param * Python variable argument. See https://docs.python.org/3/glossary.html#term-parameter
            :param ** Python variable argument. See https://docs.python.org/3/glossary.html#term-parameter
            """
            success: bool = False
            try:
                result = function(*args, **kwargs)
                success = True
                return result
            finally:
                event_name = decorator_kwargs.get("metric_name", function.__name__)

                event_details: dict = decorator_kwargs.get("event_details", {})
                event_details["is_success"] = success
                raised_exception = sys.exc_info()[1]
                if raised_exception is not None:
                    event_details["exception_type"] = type(raised_exception).__name__

                get_deadline_cloud_library_telemetry_client().record_event(
                    event_type=f"com.amazon.rum.deadline.{event_name}",
                    event_details=event_details,
                )

        wrapper.__doc__ = function.__doc__
        return cast(F, wrapper)

    return inner


def record_function_latency_telemetry_event(**decorator_kwargs: Any) -> Callable[[F], F]:
    """
    Decorator to time a function. Sends a latency telemetry event.
    :param ** Python variable arguments. See https://docs.python.org/3/glossary.html#term-parameter.
    """

    def inner(function: F) -> F:
        @wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_t = time.perf_counter_ns()
            ret_val = function(*args, **kwargs)
            end_t = time.perf_counter_ns()

            latency = end_t - start_t

            event_name = decorator_kwargs.get("metric_name", function.__name__)
            get_deadline_cloud_library_telemetry_client().record_event(
                event_type="com.amazon.rum.deadline.latency",
                event_details={"latency": latency, "function_call": event_name},
            )

            return ret_val

        return cast(F, wrapper)

    return inner
