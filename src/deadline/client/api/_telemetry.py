# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import atexit
import json
import logging
import os
import platform
import uuid
import random
import time

from configparser import ConfigParser
from dataclasses import asdict, dataclass, field
from datetime import datetime
from queue import Queue, Full
from threading import Thread
from typing import Any, Dict, Optional
from urllib import request, error

from ...job_attachments.progress_tracker import SummaryStatistics

from ._session import (
    get_monitor_id,
    get_user_and_identity_store_id,
    get_boto3_client,
)
from ..config import config_file
from .. import version

__cached_telemetry_client = None

logger = logging.getLogger(__name__)


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

    _common_details: Dict[str, Any] = {}
    _system_metadata: Dict[str, Any] = {}

    def __init__(
        self,
        package_name: str,
        package_ver: str,
        config: Optional[ConfigParser] = None,
    ):
        self._initialized: bool = False
        self.package_name = package_name
        self.package_ver = ".".join(package_ver.split(".")[:3])

        # IDs for this session
        self.session_id: str = str(uuid.uuid4())
        self.telemetry_id: str = self._get_telemetry_identifier(config=config)
        # If a different base package is provided, include info from this library as supplementary info
        if package_name != "deadline-cloud-library":
            self._common_details["deadline-cloud-version"] = version
        self._system_metadata = self._get_system_metadata(config=config)
        self.set_opt_out(config=config)
        self.initialize(config=config)

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

    def initialize(self, config: Optional[ConfigParser] = None) -> None:
        """
        Starts up the telemetry background thread after getting settings from the boto3 client.
        Note that if this is called before boto3 is successfully configured / initialized,
        an error can be raised. In that case we silently fail and don't mark the client as
        initialized.
        """
        if self.telemetry_opted_out:
            return

        try:
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
        except Exception:
            # Silently swallow any exceptions
            return

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

    def _exit_cleanly(self):
        self.event_queue.put(None)
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
        while True:
            # Blocks until we get a new entry in the queue
            event_data: Optional[TelemetryEvent] = self.event_queue.get()
            # We've received the shutdown signal
            if event_data is None:
                return

            headers = {"Accept": "application-json", "Content-Type": "application-json"}
            request_body = {
                "BatchId": str(uuid.uuid4()),
                "RumEvents": [
                    {
                        "details": str(json.dumps(event_data.event_details)),
                        "id": str(uuid.uuid4()),
                        "metadata": str(json.dumps(self._system_metadata)),
                        "timestamp": int(datetime.now().timestamp()),
                        "type": event_data.event_type,
                    },
                ],
                "UserDetails": {"sessionId": self.session_id, "userId": self.telemetry_id},
            }
            request_body_encoded = str(json.dumps(request_body)).encode("utf-8")
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

    def record_event(
        self, event_type: str, event_details: Dict[str, Any], *, from_gui: bool = False
    ):
        event_details.update(self._common_details)
        event_details["usage_mode"] = "GUI" if from_gui else "CLI"
        self._put_telemetry_record(
            TelemetryEvent(
                event_type=event_type,
                event_details=event_details,
            )
        )

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
    global __cached_telemetry_client
    if not __cached_telemetry_client:
        __cached_telemetry_client = TelemetryClient(
            package_name=package_name,
            package_ver=package_ver,
            config=config,
        )
    elif not __cached_telemetry_client.is_initialized:
        __cached_telemetry_client.initialize(config=config)

    return __cached_telemetry_client


def get_deadline_cloud_library_telemetry_client(
    config: Optional[ConfigParser] = None,
) -> TelemetryClient:
    """
    Retrieves the cached telemetry client, specifying the Deadline Cloud Client Library's package information.
    :param config: Optional configuration to use for the client. Loads defaults if not given.
    :return: Telemetry client to make requests with.
    """
    return get_telemetry_client("deadline-cloud-library", version, config=config)
