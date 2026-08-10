# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides functionality for logging in or out of the AWS Profile that is
configured for AWS Deadline Cloud to use on the local workstation.
"""

from configparser import ConfigParser
from logging import getLogger
from typing import Callable, Optional
import os
import subprocess
import sys

from ._session import (
    get_credentials_source,
    check_authentication_status,
    AwsCredentialsSource,
    AwsAuthenticationStatus,
)
from . import _session
from .. import api
from ..config import get_setting
from ..exceptions import DeadlineOperationError
import time

logger = getLogger(__name__)


class UnsupportedProfileTypeForLoginLogout(DeadlineOperationError):
    pass


def _check_console_login_dependency(profile_name: str) -> None:
    """
    Fails fast if botocore can't use the token a console sign-in would cache.

    botocore resolves ``login_session`` profiles with its LoginProvider, which needs the
    ``awscrt`` extra to sign the DPoP proofs the cached token is bound to. Without it every
    API call raises MissingDependencyException — including the authentication probe that the
    post-launch poll loop waits on. Since Deadline Cloud monitor is a long-running GUI, its
    process never exits either, so the loop would spin forever: the user would sign in
    successfully in the browser and the CLI would still hang. Check before launching anything.
    """
    # botocore.compat.EC is `awscrt.crypto.EC`, or None when awscrt is missing or older than
    # 0.28.4 -- it is the same symbol LoginProvider itself checks before refusing to load a
    # login_session profile, which is what makes it the right signal here. It is private, so
    # treat its absence as "can't tell" and let the login proceed: a spurious hang is a worse
    # outcome than an unhelpful traceback, but blocking a login that would have worked because
    # botocore renamed something is worse than both.
    try:
        from botocore.compat import EC
    except ImportError:
        logger.debug(
            "botocore.compat.EC is unavailable, so the awscrt pre-flight check was skipped."
        )
        return

    if EC is None:
        raise DeadlineOperationError(
            f"Signing in to the AWS Console sign-in profile {profile_name} requires an additional "
            'dependency. Install it with: pip install "deadline[console]"'
        )


def _login_aws_console(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
):
    """
    Logs in to an AWS Console sign-in profile by handing off to Deadline Cloud monitor.

    Starting a session needs an interactive browser handshake: the OAuth 2.0 authorization
    request is a browser endpoint, not an API this library can call. Deadline Cloud monitor
    already implements that handshake, and it recognises these profiles by the
    ``login_session`` key in ~/.aws/config, so we delegate rather than reimplement it.

    Keeping the session alive needs no help at all — botocore's LoginProvider refreshes the
    cached token in-process — so this is only reached once the session itself has run out.
    """
    # Deadline Cloud monitor writes the absolute path to itself when it creates the profile.
    # A profile created by `aws login` instead won't have it, so there's nothing to hand off to.
    deadline_cloud_monitor_path = get_setting("deadline-cloud-monitor.path", config=config)
    profile_name = get_setting("defaults.aws_profile_name", config=config)

    _check_console_login_dependency(profile_name)

    if not deadline_cloud_monitor_path:
        raise DeadlineOperationError(
            f"The profile {profile_name} was created by AWS Console sign-in, but Deadline Cloud "
            "monitor is not configured on this workstation, so there is no browser sign-in to "
            "start.\n\n"
            "To sign in, either:\n"
            "  - Install AWS Deadline Cloud monitor and create this profile with "
            "'Login with AWS Console', or\n"
            f"  - Run: aws login --profile {profile_name}\n\n"
            "Once signed in, credentials refresh automatically until the session expires."
        )

    return _login_deadline_cloud_monitor_process(
        deadline_cloud_monitor_path,
        profile_name,
        AwsCredentialsSource.AWS_CONSOLE_LOGIN,
        on_pending_authorization,
        on_cancellation_check,
        config,
    )


def _logout_aws_console(config: Optional[ConfigParser] = None) -> str:
    """
    Logs out of an AWS Console sign-in profile.

    Two things have to happen, and doing only the first leaves the user stuck. Deleting the
    cached token ends the session for anything resolving credentials: botocore caches it at
    ``<login cache dir>/<sha256 of the login_session ARN>.json``, so removing that file means
    the next resolution finds nothing to refresh.

    But Deadline Cloud monitor keeps its own idea of being signed in, and it does not watch
    that file. Left running, it would still show the profile as logged in — and worse, a
    later ``login`` would find the live instance, foreground it, and return without signing
    anyone in, because the monitor short-circuits when an instance is already up for the
    profile. So tell the monitor to log out too, the same way monitor profiles do.

    The monitor clears the cached token itself, so the deletion here is deliberately
    redundant: it is what makes logout work for a profile created by ``aws login`` on a
    workstation with no monitor installed. Whichever runs second finds nothing to do, which
    both paths treat as success.
    """
    from botocore.utils import generate_login_cache_key, get_login_token_cache_directory

    profile_name = get_setting("defaults.aws_profile_name", config=config)

    # The cache is keyed by the login session ARN, which is the profile's marker in
    # ~/.aws/config. Without it there is no session to end.
    login_session = _get_login_session_arn(config)
    if login_session is None:
        raise DeadlineOperationError(
            f"The profile {profile_name} has no login_session entry in the AWS config "
            "file, so there is no AWS Console sign-in session to log out of."
        )

    cache_file = os.path.join(
        get_login_token_cache_directory(), f"{generate_login_cache_key(login_session)}.json"
    )

    try:
        os.remove(cache_file)
    except FileNotFoundError:
        # Already signed out. Nothing cached means nothing to revoke, so this is a
        # success from the user's point of view.
        logger.debug("No cached token at %s; profile was already logged out.", cache_file)
    except OSError as e:
        raise DeadlineOperationError(
            f"Could not remove the cached credentials for profile {profile_name} at "
            f"{cache_file}: {e}"
        )

    _logout_deadline_cloud_monitor_instance(profile_name, config)

    # Force a refresh of the cached boto3 Session
    _session.invalidate_boto3_session_cache()
    return f"Successfully logged out of AWS Console sign-in profile: {profile_name}"


def _logout_deadline_cloud_monitor_instance(
    profile_name: str, config: Optional[ConfigParser] = None
) -> None:
    """
    Asks Deadline Cloud monitor to log out of a profile, if it is installed.

    Best-effort on purpose: the cached token is already gone by this point, so the session is
    over either way. A monitor that isn't installed, or that fails to log out, must not turn a
    successful logout into an error — but leaving it signed in would make the *next* login a
    silent no-op, so it is worth attempting and worth logging when it doesn't work.
    """
    deadline_cloud_monitor_path = get_setting("deadline-cloud-monitor.path", config=config)
    if not deadline_cloud_monitor_path:
        return

    try:
        subprocess.check_output(
            [deadline_cloud_monitor_path, "logout", "--profile", profile_name],
            stderr=subprocess.STDOUT,
        )
    except (OSError, subprocess.CalledProcessError) as e:
        logger.warning(
            "Deadline Cloud monitor could not log out of profile %s (%s). The cached "
            "credentials were removed, but the monitor may still show the profile as signed "
            "in; quit it before signing in again.",
            profile_name,
            e,
        )


def _get_login_session_arn(config: Optional[ConfigParser] = None) -> Optional[str]:
    """
    Returns the ``login_session`` ARN of the configured AWS profile, or None.

    Resolved through the boto3 session's scoped config rather than by indexing
    ``full_config["profiles"]`` by name. The two differ for the default profile:
    ``defaults.aws_profile_name`` ships as the sentinel ``"(default)"`` (and ``""`` also
    means "use the default credentials"), which `get_boto3_session` normalizes away, but
    ``full_config["profiles"]`` is keyed by the real name — ``"default"``. Indexing by the
    sentinel would miss a console sign-in profile that *is* the default profile, so logout
    would refuse a session that `get_credentials_source` had just classified as a console
    one. Going through the session, as `get_credentials_source` does, keeps the two
    agreeing on which profile is meant.

    A profile name that isn't in the AWS config at all makes botocore raise
    ``ProfileNotFound``. That means the same thing as a profile without the key — there is
    no session to end — so report it the same way rather than surfacing a different error
    from the one the caller is about to raise.
    """
    from botocore.exceptions import ProfileNotFound

    try:
        session = _session.get_boto3_session(config=config)
        return session._session.get_scoped_config().get("login_session")
    except ProfileNotFound:
        return None


def _login_deadline_cloud_monitor(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
):
    # Deadline Cloud monitor writes the absolute path to itself to the config file
    deadline_cloud_monitor_path = get_setting("deadline-cloud-monitor.path", config=config)
    profile_name = get_setting("defaults.aws_profile_name", config=config)

    return _login_deadline_cloud_monitor_process(
        deadline_cloud_monitor_path,
        profile_name,
        AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN,
        on_pending_authorization,
        on_cancellation_check,
        config,
    )


def _login_deadline_cloud_monitor_process(
    deadline_cloud_monitor_path: str,
    profile_name: str,
    credentials_source: AwsCredentialsSource,
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
):
    """
    Launches Deadline Cloud monitor to sign a profile in, and waits for it to take effect.

    Shared by the monitor and AWS Console sign-in profile types: both are signed in by the
    same ``login --profile`` subcommand and are both detected the same way, by polling until
    the profile authenticates. Only the reported credentials source and the name used in the
    success message differ.
    """
    # Name the profile type the user actually chose. `logout` reports it per-type too, so
    # branding a console sign-in as a monitor profile here would contradict it.
    profile_type_label = (
        "AWS Console sign-in profile"
        if credentials_source == AwsCredentialsSource.AWS_CONSOLE_LOGIN
        else "Deadline Cloud monitor profile"
    )
    args = [deadline_cloud_monitor_path, "login", "--profile", profile_name]

    # Open Deadline Cloud monitor, non-blocking the user will keep Deadline Cloud monitor running in the background.
    try:
        if sys.platform.startswith("win"):
            # We don't hookup to stdin but do this to avoid issues on windows
            # See https://docs.python.org/3/library/subprocess.html#subprocess.STARTUPINFO.lpAttributeList
            p = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.PIPE
            )
        else:
            p = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL
            )
    except FileNotFoundError:
        raise DeadlineOperationError(
            f"Could not find Deadline Cloud monitor at {deadline_cloud_monitor_path}. "
            f"Please ensure Deadline Cloud monitor is installed correctly and set up the {profile_name} profile again."
        )
    if on_pending_authorization:
        on_pending_authorization(credentials_source=credentials_source)
    # And wait for the user to complete login
    while True:
        # Deadline Cloud monitor is a GUI app that will keep on running
        # So we sit here and test that profile for validity until it works.
        # Force-refresh the session each iteration so the next probe picks up
        # profile keys DCM writes (user_id, identity_store_id, monitor_id) as
        # login completes — the GUI does the same on file-watch events in
        # DeadlineAuthenticationStatus.files_changed, but CLI has no watcher.
        _session.get_boto3_session(force_refresh=True, config=config)
        if check_authentication_status(config) == AwsAuthenticationStatus.AUTHENTICATED:
            return f"{profile_type_label}: {profile_name}"
        if on_cancellation_check:
            # Check if the UI has signaled a cancel
            if on_cancellation_check():
                p.kill()
                raise Exception()
        if p.poll():
            # Deadline Cloud monitor has stopped, we assume it returned us an error on one line on stderr
            # but let's be specific about Deadline Cloud monitor failing incase the error is non-obvious
            # and let's tack on stdout incase it helps
            err_prefix = (
                f"Deadline Cloud monitor was not able to log into the {profile_name} profile:"
            )
            out = p.stdout.read().decode("utf-8") if p.stdout else ""
            raise DeadlineOperationError(f"{err_prefix}\n{out}")

        time.sleep(0.5)


@api.record_function_latency_telemetry_event()
def login(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
) -> str:
    """
    For AWS profiles created by Deadline Cloud monitor or by AWS Console sign-in,
    logs in to provide access to Deadline Cloud.

    Args:
        on_pending_authorization (Callable): A callback that receives method-specific information to continue login.
            All methods: 'credentials_source' parameter of type AwsCredentialsSource
            For Deadline Cloud monitor: No additional parameters
            For AWS Console sign-in: No additional parameters
        on_cancellation_check (Callable): A callback that allows the operation to cancel before login completes
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    credentials_source = get_credentials_source(config)
    if credentials_source == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
        return _login_deadline_cloud_monitor(
            on_pending_authorization, on_cancellation_check, config
        )
    if credentials_source == AwsCredentialsSource.AWS_CONSOLE_LOGIN:
        return _login_aws_console(on_pending_authorization, on_cancellation_check, config)
    raise UnsupportedProfileTypeForLoginLogout(
        "Logging in is only supported for AWS Profiles created by Deadline Cloud monitor "
        "or by AWS Console sign-in."
    )


@api.record_function_latency_telemetry_event()
def logout(config: Optional[ConfigParser] = None) -> str:
    """
    For AWS profiles created by Deadline Cloud monitor or by AWS Console sign-in,
    logs out of Deadline Cloud.

     Args:
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    credentials_source = get_credentials_source(config)
    if credentials_source == AwsCredentialsSource.AWS_CONSOLE_LOGIN:
        return _logout_aws_console(config)
    if credentials_source == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
        # Deadline Cloud monitor writes the absolute path to itself to the config file
        deadline_cloud_monitor_path = get_setting("deadline-cloud-monitor.path", config=config)
        profile_name = get_setting("defaults.aws_profile_name", config=config)
        args = [deadline_cloud_monitor_path, "logout", "--profile", profile_name]

        # Open Deadline Cloud monitor, blocking
        # Unlike login, that opens the regular Deadline Cloud monitor GUI, logout is a CLI command that clears the profile
        # This makes it easier as we can execute and look at the return cdoe
        try:
            output = subprocess.check_output(args)
        except FileNotFoundError:
            raise DeadlineOperationError(
                f"Could not find Deadline Cloud monitor at {deadline_cloud_monitor_path}. "
                f"Please ensure Deadline Cloud monitor is installed correctly and set up the {profile_name} profile again."
            )
        except subprocess.CalledProcessError as e:
            raise DeadlineOperationError(
                f"Deadline Cloud monitor was unable to log out the profile {profile_name}."
                f"Return code {e.returncode}: {e.output}"
            )

        # Force a refresh of the cached boto3 Session
        _session.invalidate_boto3_session_cache()
        return output.decode("utf8")
    raise UnsupportedProfileTypeForLoginLogout(
        "Logging out is only supported for AWS Profiles created by Deadline Cloud monitor "
        "or by AWS Console sign-in."
    )
