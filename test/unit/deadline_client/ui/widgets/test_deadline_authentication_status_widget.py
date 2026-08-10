# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import MagicMock, patch

import pytest

try:
    from deadline.client.ui.widgets.deadline_authentication_status_widget import (
        AuthenticationState,
        DeadlineAuthenticationStatusWidget,
    )
except ImportError:
    # The tests in this file should be skipped if Qt UI related modules cannot be loaded
    pytest.importorskip("deadline.client.ui.widgets.deadline_authentication_status_widget")

from deadline.client import api

MODULE = "deadline.client.ui.widgets.deadline_authentication_status_widget"


@pytest.fixture
def mock_status():
    """A stand-in for the DeadlineAuthenticationStatus singleton the widget connects to."""
    status = MagicMock()
    status.creds_source = api.AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
    status.auth_status = api.AwsAuthenticationStatus.NEEDS_LOGIN
    status.config = MagicMock()
    return status


def _make_widget(qtbot, mock_status, profile_name="my-profile"):
    with (
        patch(f"{MODULE}.DeadlineAuthenticationStatus.getInstance", return_value=mock_status),
        patch(f"{MODULE}.config_file.get_setting", return_value=profile_name),
    ):
        widget = DeadlineAuthenticationStatusWidget()
        qtbot.addWidget(widget)
        return widget


def test_needs_login_shows_only_profile_name(qtbot, mock_status):
    """Regression test for issue #769.

    When logged out, the profile button must show only the profile name and not the
    longer "You are logged out." message, which was clipped behind the switch profile
    and log in buttons at default scaling / narrow window widths. The warning icon and
    the visible "Log in" button already communicate the logged-out state.
    """
    mock_status.auth_status = api.AwsAuthenticationStatus.NEEDS_LOGIN

    widget = _make_widget(qtbot, mock_status, profile_name="my-profile")

    assert widget._get_current_auth_state_key() == AuthenticationState.NEEDS_LOGIN
    # Only the profile name is shown -- no "logged out" text to be clipped.
    assert widget._profile_button.text() == "my-profile"
    assert "logged out" not in widget._profile_button.text().lower()
    # The next step is obvious: the warning icon plus the visible Log in button.
    # isVisibleTo avoids depending on the top-level widget being shown on screen.
    assert widget._login_button.isVisibleTo(widget)
    assert widget._switch_profile_button.isVisibleTo(widget)


def test_authenticated_shows_profile_name(qtbot, mock_status):
    mock_status.auth_status = api.AwsAuthenticationStatus.AUTHENTICATED

    widget = _make_widget(qtbot, mock_status, profile_name="my-profile")

    assert widget._get_current_auth_state_key() == AuthenticationState.AUTHENTICATED_READY
    assert widget._profile_button.text() == "my-profile"
    assert not widget._login_button.isVisibleTo(widget)


def test_needs_login_hides_switch_profile_button_when_disabled(qtbot, mock_status):
    """With profile switching disabled, the switch profile button stays hidden even
    when logged out, but the Log in button remains the obvious next step."""
    mock_status.auth_status = api.AwsAuthenticationStatus.NEEDS_LOGIN

    with (
        patch(f"{MODULE}.DeadlineAuthenticationStatus.getInstance", return_value=mock_status),
        patch(f"{MODULE}.config_file.get_setting", return_value="my-profile"),
    ):
        widget = DeadlineAuthenticationStatusWidget(show_profile_switch=False)
        qtbot.addWidget(widget)

    assert not widget._switch_profile_button.isVisibleTo(widget)
    assert widget._login_button.isVisibleTo(widget)


def test_console_profile_needs_login_shows_login_button(qtbot, mock_status):
    """
    An expired AWS Console sign-in profile is recoverable by logging in, so it must
    land in NEEDS_LOGIN and offer the Log in button -- not the dead-end
    CONFIGURATION_ERROR state it fell into when console profiles read as
    HOST_PROVIDED.
    """
    mock_status.creds_source = api.AwsCredentialsSource.AWS_CONSOLE_LOGIN
    mock_status.auth_status = api.AwsAuthenticationStatus.NEEDS_LOGIN

    widget = _make_widget(qtbot, mock_status, profile_name="console-us-west-2")

    assert widget._get_current_auth_state_key() == AuthenticationState.NEEDS_LOGIN
    assert widget._login_button.isVisibleTo(widget)
    assert not widget._more_info_button.isVisibleTo(widget)


def test_console_profile_offers_logout(qtbot, mock_status):
    """
    Logging out a console profile deletes its cached token in-process, which always
    works, so offer it.
    """
    mock_status.creds_source = api.AwsCredentialsSource.AWS_CONSOLE_LOGIN
    mock_status.auth_status = api.AwsAuthenticationStatus.AUTHENTICATED

    widget = _make_widget(qtbot, mock_status, profile_name="console-us-west-2")

    assert widget._should_show_logout()
    assert widget._logout_menu_action.isVisible()


def test_host_provided_profile_does_not_offer_logout(qtbot, mock_status):
    """Profiles with no login flow have nothing to log out of."""
    mock_status.creds_source = api.AwsCredentialsSource.HOST_PROVIDED
    mock_status.auth_status = api.AwsAuthenticationStatus.AUTHENTICATED

    widget = _make_widget(qtbot, mock_status, profile_name="plain-profile")

    assert not widget._should_show_logout()
    assert not widget._logout_menu_action.isVisible()
