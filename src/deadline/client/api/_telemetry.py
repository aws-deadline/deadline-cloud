# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import atexit
import json
import logging
import platform
import uuid

from configparser import ConfigParser
from dataclasses import asdict, dataclass, field
from datetime import datetime
from queue import Queue
from threading import Thread
from typing import Any, Dict, Optional
from urllib import request, error

from ...job_attachments.progress_tracker import SummaryStatistics

from ._session import get_studio_id, get_user_and_identity_store_id
from ..config import config_file
from .. import version

__cached_telemetry_client = None

logger = logging.getLogger(__name__)


@dataclass
class TelemetryEvent:
    """Base class for telemetry events"""

    event_type: str = "com.amazon.rum.uncategorized"
    event_body: Dict[str, Any] = field(default_factory=dict)


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
    'deadline config set "telemetry.opt_out" true'
    """

    def __init__(self, config: Optional[ConfigParser] = None):
        self.telemetry_opted_out = config_file.str2bool(
            config_file.get_setting("telemetry.opt_out", config=config)
        )
        if self.telemetry_opted_out:
            return
        self.endpoint: str = f"{config_file.get_setting('settings.deadline_endpoint_url', config=config)}/2023-10-12/telemetry"

        # IDs for this session
        self.session_id: str = str(uuid.uuid4())
        self.telemetry_id: str = self._get_telemetry_identifier(config=config)
        # Get common data we'll include in each request
        self.studio_id: Optional[str] = get_studio_id(config=config)
        self.user_id, _ = get_user_and_identity_store_id(config=config)
        self.env_info: Dict[str, Any] = self._get_env_summary()
        self.system_info: Dict[str, Any] = self._get_system_info()

        self._start_threads()

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
        self.event_queue: Queue[Optional[TelemetryEvent]] = Queue()
        atexit.register(self._exit_cleanly)
        self.processing_thread: Thread = Thread(
            target=self._process_event_queue_thread, daemon=True
        )
        self.processing_thread.start()

    def _get_env_summary(self) -> Dict[str, Any]:
        """Builds up a dict of non-identifiable information the environment."""
        return {
            "service": "deadline-cloud-library",
            "version": ".".join(version.split(".")[:3]),
            "pythonVersion": platform.python_version(),
        }

    def _get_system_info(self) -> Dict[str, Any]:
        """Builds up a dict of non-identifiable information about this machine."""
        platform_info = platform.uname()
        return {
            "osName": "macOS" if platform_info.system == "Darwin" else platform_info.system,
            "osVersion": platform_info.release,
            "cpuType": platform_info.machine,
            "cpuName": platform_info.processor,
        }

    def _exit_cleanly(self):
        self.event_queue.put(None)
        self.processing_thread.join()

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
                        "details": "{}",
                        "id": str(uuid.uuid4()),
                        "metadata": str(json.dumps(event_data.event_body)),
                        "timestamp": int(datetime.now().timestamp()),
                        "type": event_data.event_type,
                    },
                ],
                "UserDetails": {"sessionId": self.session_id, "userId": self.telemetry_id},
            }
            request_body_encoded = str(json.dumps(request_body)).encode("utf-8")
            req = request.Request(url=self.endpoint, data=request_body_encoded, headers=headers)
            try:
                logger.debug(f"Sending telemetry data: {request_body}")
                with request.urlopen(req):
                    logger.debug("Successfully sent telemetry.")
            except error.HTTPError as httpe:
                logger.debug(f"HTTPError sending telemetry: {str(httpe)}")
            except Exception as ex:
                logger.debug(f"Exception sending telemetry: {str(ex)}")
            self.event_queue.task_done()

    def _record_summary_statistics(
        self, event_type: str, summary: SummaryStatistics, from_gui: bool
    ):
        if self.telemetry_opted_out:
            return
        data_body: Dict[str, Any] = asdict(summary)
        data_body.update(self.env_info)
        data_body.update(self.system_info)
        if self.user_id:
            data_body["userId"] = self.user_id
        if self.studio_id:
            data_body["studioId"] = self.studio_id
        data_body["usageMode"] = "GUI" if from_gui else "CLI"
        self.event_queue.put_nowait(TelemetryEvent(event_type=event_type, event_body=data_body))

    def record_hashing_summary(self, summary: SummaryStatistics, from_gui: bool = False):
        self._record_summary_statistics(
            "com.amazon.rum.job_attachments.hashing_summary", summary, from_gui
        )

    def record_upload_summary(self, summary: SummaryStatistics, from_gui: bool = False):
        self._record_summary_statistics(
            "com.amazon.rum.job_attachments.upload_summary", summary, from_gui
        )


def get_telemetry_client(config: Optional[ConfigParser] = None) -> TelemetryClient:
    global __cached_telemetry_client
    if not __cached_telemetry_client:
        __cached_telemetry_client = TelemetryClient(config)

    return __cached_telemetry_client
