# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides an object that can be used to track current status of AWS Deadline Cloud
authentication.

The object emits the following Qt Signals:
   aws_creds_changed: The AWS credentials in ~/.aws changed.
   deadline_config_changed: The AWS Deadline Cloud configuration in ~/.deadline changed.
   creds_source_changed: triggered when credential source changes
   auth_status_changed: triggered when authentication status changes
   api_availability_changed: triggered when api availability changes

The status includes three parts:
  1. Are credentials configured and available for use?
     This is checked with an sts:GetCallerIdentity AWS API call.
  2. Do the credentials grant access to AWS Deadline Cloud APIs?
     This is checked with a simplified deadline:ListFarms AWS API call.
  3. Do the credentials use Deadline Cloud monitor?
     This is checked by looking for the relevant properties
     in the AWS profile configuration.
"""

import os
from configparser import ConfigParser
from functools import partial
from logging import getLogger
from typing import Optional

from qtpy.QtCore import QObject, QFileSystemWatcher, QTimer, Qt, Signal
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QWidget,
)
from .. import api
from ..config import config_file
from .controllers import AsyncTaskRunner

logger = getLogger(__name__)

_deadline_authentication_status = None

# How often (in milliseconds) to re-probe the authentication status while a GUI is
# open. Credentials can expire in place, or be changed out-of-process (e.g. a
# `deadline auth logout`/`login` from a terminal). Neither of those reliably trips
# the QFileSystemWatcher, so we poll as a backstop to keep the UI in sync.
_AUTH_STATUS_POLL_INTERVAL_MS = 30 * 1000

# Number of consecutive quiet-poll probe failures required before a previously
# AUTHENTICATED state is downgraded in the UI. check_authentication_status maps
# any probe exception (including transient network/throttling errors) to a
# non-AUTHENTICATED status, so a single blip should not flip the UI; requiring a
# couple of consecutive failures debounces that without meaningfully delaying a
# real logout/expiry (which fails every poll).
_AUTH_STATUS_POLL_FAILURE_THRESHOLD = 2


class DeadlineAuthenticationStatus(QObject):
    """
    Holds status information about AWS Deadline Cloud credentials.
    Currently status values are available as properties:

       status.creds_source: result of api.get_credentials_source()
       status.auth_status: result of api.check_authentication_status()
       status.api_availability: True iff auth_status is AUTHENTICATED (derived
                                from the same api.check_authentication_status() probe)

    To initialize the status of a non-default AWS Deadline Cloud configuration, pass in
    an AWS Deadline Cloud configuration object to config, call set_config to change it.
    """

    # This signal is sent when an AWS credential changes (e.g. config file)
    aws_creds_changed = Signal()
    # This signal is sent when the AWS Deadline Cloud configuration changes
    deadline_config_changed = Signal()

    # This signal is sent when an AWS authentication type changes
    creds_source_changed = Signal()
    # This signal is sent when an AWS authentication status changes
    auth_status_changed = Signal()
    # This signal is sent when AWS Deadline Cloud API availability changes
    api_availability_changed = Signal()

    @staticmethod
    def getInstance():
        global _deadline_authentication_status
        if _deadline_authentication_status is None:
            _deadline_authentication_status = DeadlineAuthenticationStatus()
        return _deadline_authentication_status

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super(DeadlineAuthenticationStatus, self).__init__(parent)

        self.__creds_source: Optional[api.AwsCredentialsSource] = None
        self.__auth_status: Optional[api.AwsAuthenticationStatus] = None
        self.__api_availability: Optional[bool] = None

        # Count of consecutive *quiet poll* probe failures while otherwise
        # AUTHENTICATED. The probe reports CONFIGURATION_ERROR/NEEDS_LOGIN on any
        # exception, including transient network/throttling blips, so we require a
        # few consecutive failures before downgrading a steady AUTHENTICATED state
        # — otherwise a momentary blip would flip the UI to "Log in" and back
        # every poll interval. Reset on any success or on a user-driven refresh.
        self.__consecutive_poll_failures = 0

        # Use AsyncTaskRunner for background API calls
        self._runner = AsyncTaskRunner(self)
        self._runner.task_error.connect(self._handle_task_error, Qt.QueuedConnection)

        # Load the default config
        self.config = ConfigParser()
        self.config.read_dict(config_file.read_config())

        # Watch the ~/.aws path for any changes to config or credentials, and
        # the ~/.deadline path for any changes to the AWS Deadline Cloud config.
        self.aws_creds_file_watcher = QFileSystemWatcher()
        self.aws_creds_paths = [
            os.path.expanduser(os.path.join("~", ".aws")),
        ]
        self.deadline_config_paths = [
            os.path.expanduser(os.path.join("~", ".deadline")),
        ]
        failed_paths = self.aws_creds_file_watcher.addPaths(
            self.aws_creds_paths + self.deadline_config_paths
        )
        if failed_paths:
            logger.error(
                "Failed to watch these AWS Deadline Cloud configurations: %s", failed_paths
            )
        self.aws_creds_file_watcher.fileChanged.connect(self.files_changed)
        self.aws_creds_file_watcher.directoryChanged.connect(self.files_changed)

        # The file watcher only fires on direct add/remove/rename of entries in the
        # watched directories; it misses in-place credential rewrites, expiry, and
        # out-of-process changes. Poll periodically as a backstop so the UI reflects
        # the true auth state even when nothing on disk visibly changed.
        #
        # The timer is only run while at least one auth-status widget is visible
        # (see start_polling/stop_polling), so we never issue background AWS calls
        # when no GUI is shown, and the timer can't outlive the widgets that need
        # it. _poll_subscribers ref-counts those widgets.
        self._poll_subscribers = 0
        self._poll_timer = QTimer(self)
        self._poll_timer.setInterval(_AUTH_STATUS_POLL_INTERVAL_MS)
        self._poll_timer.timeout.connect(self._poll_auth_status)

        self.refresh_status()

    def _handle_task_error(self, operation_name: str, error: BaseException) -> None:
        """Handle errors from async tasks."""
        logger.exception(f"Error in {operation_name}: {error}")

        self.refresh_status()

    def set_config(self, config: Optional[ConfigParser]) -> None:
        """
        Changes the AWS Deadline Cloud configuration object used to display authentication
        status.

        Args:
            config (ConfigParser): The AWS Deadline Cloud configuration to use.
        """

        # Refresh the status if any setting that impacts authentication was changed
        if self.config:
            auth_config_changed = False
            for setting_name in [
                "defaults.aws_profile_name",
            ]:
                if config_file.get_setting(setting_name, self.config) != config_file.get_setting(
                    setting_name, config
                ):
                    auth_config_changed = True
        else:
            auth_config_changed = True

        # Make a copy of the config object
        self.config = ConfigParser()
        if config:
            self.config.read_dict(config)
        else:
            self.config.read_dict(config_file.read_config())

        if auth_config_changed:
            self.refresh_status()

    @property
    def creds_source(self) -> Optional[api.AwsCredentialsSource]:
        return self.__creds_source

    @property
    def auth_status(self) -> Optional[api.AwsAuthenticationStatus]:
        return self.__auth_status

    @property
    def api_availability(self) -> Optional[bool]:
        return self.__api_availability

    def files_changed(self, changed_path) -> None:
        # Force the cached boto3 session to refresh, since we don't check the creds
        # file
        if changed_path in self.aws_creds_paths:
            logger.info(f"Path {changed_path} changed, refreshing authentication status")
            # Use AsyncTaskRunner to avoid blocking the Qt event loop
            self._runner.run(
                operation_key="get_session",
                fn=self._get_session_background,
                on_success=lambda _: self._on_session_refreshed(changed_path),
                on_error=lambda e: logger.exception(f"Error refreshing session: {e}"),
            )
        else:
            logger.info(f"Path {changed_path} changed, does not affect authentication status")

    def _get_session_background(self):
        """Background task to refresh boto3 session."""
        api.get_boto3_session(force_refresh=True)
        return True

    def _on_session_refreshed(self, changed_path):
        """Called after session is refreshed."""
        self.refresh_status()

        if changed_path in self.aws_creds_paths:
            self.aws_creds_changed.emit()
        elif changed_path in self.deadline_config_paths:
            self.deadline_config_changed.emit()

    def _refresh_creds_source(self) -> api.AwsCredentialsSource:
        """Background task to get credentials source."""
        return api.get_credentials_source(config=self.config)

    def _on_creds_source_success(self, result: api.AwsCredentialsSource) -> None:
        """Handle successful credentials source fetch."""
        self._set_creds_source(result)

    def _on_creds_source_error(self, error: BaseException) -> None:
        """Handle credentials source fetch error."""
        logger.exception(error)
        self._set_creds_source(None)

    def _set_creds_source(self, value: Optional[api.AwsCredentialsSource]) -> None:
        """Update the cached creds source, emitting only when it actually changed.

        Emitting only on change lets the periodic poll re-probe silently: nothing
        is signalled (and so nothing re-renders) while the state is steady.
        """
        if self.__creds_source != value:
            self.__creds_source = value
            self.creds_source_changed.emit()

    def _refresh_auth_status(self, force_refresh: bool = False) -> api.AwsAuthenticationStatus:
        """Background task to check authentication status.

        When ``force_refresh`` is set (the periodic poll), the cached boto3
        session is invalidated first so out-of-process credential changes that
        the file watcher misses — an in-place rewrite of ``~/.aws/credentials``,
        or a logout — are actually re-read rather than validated against the
        stale, in-memory cached session. This runs on the background thread just
        before the probe, keeping the (potentially slow) refresh off the Qt loop.
        """
        if force_refresh:
            api.get_boto3_session(force_refresh=True, config=self.config)
        return api.check_authentication_status(config=self.config)

    def _on_auth_status_success(
        self, result: api.AwsAuthenticationStatus, quiet: bool = False
    ) -> None:
        """Handle successful authentication status check."""
        # A successful probe (of any status) clears the transient-failure streak.
        self.__consecutive_poll_failures = 0
        # API availability is equivalent to being AUTHENTICATED: both derive from
        # the same deadline:ListFarms probe. Compute it from the status result
        # rather than issuing a second, redundant probe.
        self._set_auth_status(result, result == api.AwsAuthenticationStatus.AUTHENTICATED)

    def _on_auth_status_error(self, error: BaseException, quiet: bool = False) -> None:
        """Handle authentication status check error."""
        logger.exception(error)
        # A quiet-poll failure while we are otherwise AUTHENTICATED may just be a
        # transient network/service blip (check_authentication_status maps any
        # probe exception to CONFIGURATION_ERROR). Debounce: only downgrade the UI
        # after N consecutive failures, so a momentary blip does not flip the
        # widget to "Log in"/"error" and back every poll interval. User-driven
        # refreshes (quiet=False) still surface the error immediately.
        if quiet and self.__auth_status == api.AwsAuthenticationStatus.AUTHENTICATED:
            self.__consecutive_poll_failures += 1
            if self.__consecutive_poll_failures < _AUTH_STATUS_POLL_FAILURE_THRESHOLD:
                logger.info(
                    "Quiet auth poll failed (%d/%d) while AUTHENTICATED; not downgrading yet",
                    self.__consecutive_poll_failures,
                    _AUTH_STATUS_POLL_FAILURE_THRESHOLD,
                )
                return
        self.__consecutive_poll_failures = 0
        self._set_auth_status(api.AwsAuthenticationStatus.CONFIGURATION_ERROR, False)

    def _set_auth_status(
        self,
        auth_status: Optional[api.AwsAuthenticationStatus],
        api_availability: Optional[bool],
    ) -> None:
        """Update the cached auth status / API availability, emitting only on change.

        Emitting only on change lets the periodic poll re-probe silently: a steady
        AUTHENTICATED state produces no signals, while a transition (e.g. to
        NEEDS_LOGIN after expiry or an external logout) flips the UI exactly once.
        """
        if self.__auth_status != auth_status:
            self.__auth_status = auth_status
            self.auth_status_changed.emit()
        if self.__api_availability != api_availability:
            self.__api_availability = api_availability
            self.api_availability_changed.emit()

    def _start_polling(self) -> None:
        """Register interest in periodic auth-status polling.

        Ref-counted: the timer runs while at least one caller (typically a live
        auth-status widget) has registered. Pairs with :meth:`_stop_polling`. This
        keeps polling — and the background AWS probes it triggers — scoped to when
        a GUI is actually present, rather than for the entire lifetime of the
        process-wide singleton (which would otherwise leak probes into a headless
        process or across unrelated tests).
        """
        self._poll_subscribers += 1
        if not self._poll_timer.isActive():
            self._poll_timer.start()

    def _stop_polling(self) -> None:
        """Release interest in periodic auth-status polling (see _start_polling).

        The timer is stopped once the last subscriber releases. Extra/unbalanced
        calls are clamped at zero so a stray stop can't drive the count negative.
        """
        if self._poll_subscribers > 0:
            self._poll_subscribers -= 1
        if self._poll_subscribers == 0 and self._poll_timer.isActive():
            self._poll_timer.stop()

    def _poll_auth_status(self) -> None:
        """Timer-driven background re-check of the authentication status.

        Runs a quiet refresh so a steady state produces no UI churn, and skips the
        tick entirely if a refresh is already in flight to avoid cancelling and
        restarting work on every interval.
        """
        if self._runner.is_running("auth_status") or self._runner.is_running("creds_source"):
            return
        self._refresh_status(quiet=True)

    def refresh_status(self) -> None:
        """
        Initiates an asynchronous status refresh.
        """
        self._refresh_status(quiet=False)

    def _refresh_status(self, quiet: bool) -> None:
        """
        Initiates an asynchronous status refresh.

        Args:
            quiet (bool): When False (used for user-initiated refreshes), the
                cached values are cleared first so widgets show a "Refreshing"
                state while the probes run. When True (used by the periodic
                poll), the cached values are left in place and signals fire only
                if the probe result differs, so a steady auth state produces no
                visible flicker.
        """
        if not quiet:
            # A user-driven refresh should reflect the probe result immediately,
            # so drop any in-progress transient-failure debounce.
            self.__consecutive_poll_failures = 0
            # Clear current values and emit signals to indicate refresh started
            self._set_creds_source(None)
            self._set_auth_status(None, None)

        # Start async tasks for each status check
        self._runner.run(
            operation_key="creds_source",
            fn=self._refresh_creds_source,
            on_success=self._on_creds_source_success,
            on_error=self._on_creds_source_error,
        )
        # The auth_status task also resolves api_availability (both rely on the
        # same deadline:ListFarms probe), so no separate api_availability task
        # is needed — see _on_auth_status_success / _on_auth_status_error.
        # On a quiet poll, force-refresh the boto3 session so out-of-process
        # credential changes are picked up, and pass ``quiet`` to the handlers so
        # transient failures are debounced rather than flipping the UI at once.
        self._runner.run(
            operation_key="auth_status",
            fn=partial(self._refresh_auth_status, force_refresh=quiet),
            on_success=partial(self._on_auth_status_success, quiet=quiet),
            on_error=partial(self._on_auth_status_error, quiet=quiet),
        )
