# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides a widget to place in AWS Deadline Cloud submitter dialogs, that shows
the current status of AWS Deadline Cloud authentication.
The current status is handled by DeadlineAuthenticationStatus.
"""

import enum

from dataclasses import dataclass
from logging import getLogger
from typing import Callable, Union, Dict

from qtpy.QtCore import Signal
from qtpy.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QHBoxLayout,
    QLabel,
    QWidget,
    QApplication,
    QStyle,
    QFrame,
    QMenu,
    QPushButton,
    QMessageBox,
)

from ... import api
from ..deadline_authentication_status import DeadlineAuthenticationStatus
from ...config import config_file

logger = getLogger(__name__)


class RightAlignedQMenu(QMenu):
    """
    A modified QMenu that positions itself to the right of its parent widget
    instead of the default left positioning.
    """

    def showEvent(self, event):
        """
        Override showEvent to position the menu to the right of the parent button.
        """
        if self.parent():
            parent_top_right = self.parent().mapToGlobal(self.parent().rect().topRight())
            self.move(parent_top_right.x(), parent_top_right.y())

        super().showEvent(event)
        self.adjustSize()  # The menu was displaying incorrectly on Mac until a resize event


@dataclass
class AuthenticationStateConfig:
    """
    Configuration dataclass that encapsulates all the visual and behavioral
    settings needed to display the authentication widget in different states,
    including which icon to show, what text to display, and which buttons should
    be visible.

    Attributes:
        icon (QStyle.StandardPixmap): The standard Qt icon to display for this state.
        text (Union[str, Callable[[], str]]): The text to display, either as a string
            or a callable that returns a string for dynamic text.
        switch_profile_button_visible (bool): Whether the switch profile button should
            be visible. If the button isn't visible that switch profile menu item will be.
            Defaults to False.
        login_visible (bool): Whether the login button should be visible. Defaults to False.
        logout_visible (Union[bool, Callable[[], bool]]): Whether the logout option should
            be visible, either as a boolean or callable returning boolean. Defaults to False.
        more_info_visible (bool): Whether the more info button should be visible.
            Defaults to False.
    """

    icon: QStyle.StandardPixmap
    text: Union[str, Callable[[], str]]
    switch_profile_button_visible: bool = False
    login_visible: bool = False
    logout_visible: Union[bool, Callable[[], bool]] = False
    more_info_visible: bool = False


class AuthenticationState(enum.Enum):
    """
    Enum defining all the possible states that the authentication status widget can display,
    each corresponding to a different combination of authentication status and
    API availability.

    Values:
        REFRESHING: Authentication status is being loaded or refreshed.
        AUTHENTICATED_READY: User is authenticated and has API access to list-farms
        AUTHENTICATED_NO_API: User is authenticated but lacks API access permissions to list-farms
        NEEDS_LOGIN: User needs to log in to authenticate with a DCM profile
        CONFIGURATION_ERROR: There is a configuration issue with the AWS profile.
        UNEXPECTED_ERROR: An unknown or unexpected error occurred during authentication.
    """

    REFRESHING = enum.auto()
    AUTHENTICATED_READY = enum.auto()
    AUTHENTICATED_NO_API = enum.auto()
    NEEDS_LOGIN = enum.auto()
    CONFIGURATION_ERROR = enum.auto()
    UNEXPECTED_ERROR = enum.auto()


class DeadlineAuthenticationStatusWidget(QWidget):
    """
    A Qt widget that displays the current AWS Deadline Cloud authentication status.

    This widget provides a visual indicator of the authentication status, showing an appropriate
    icon, profile name, and relevant action buttons based on the current authentication status.

    Signals:
        switch_profile_clicked: Emitted when the user clicks to switch AWS profiles.
        login_clicked: Emitted when the user clicks the login button.
        logout_clicked: Emitted when the user clicks the logout option.
    """

    switch_profile_clicked = Signal()
    login_clicked = Signal()
    logout_clicked = Signal()

    def __init__(self, parent=None, show_profile_switch=True) -> None:
        """
        Initialize the authentication status widget.

        Args:
            parent: The parent Qt widget. Defaults to None.
            show_profile_switch (bool): Whether to show the switch profile control.
                Defaults to True.
        """
        super().__init__(parent=parent)

        self._build_ui(show_profile_switch)

        # Connect to authentication status
        self._status = DeadlineAuthenticationStatus.getInstance()
        self._status.creds_source_changed.connect(self._update_ui)
        self._status.auth_status_changed.connect(self._update_ui)
        self._status.api_availability_changed.connect(self._update_ui)

        # Initial update
        self._update_ui()

    def _build_ui(self, show_profile_switch: bool = True) -> None:
        """
        Build the user interface components for the authentication status widget.

        Creates a styled frame containing a status icon, profile button with dropdown menu,
        and various action buttons (switch profile, login, more info) that are shown/hidden
        based on the current authentication state.

        Args:
            show_profile_switch (bool): Whether to enable profile switching functionality.
                Defaults to True.
        """
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)

        frame = QFrame(self)
        frame.setStyleSheet("QFrame {background-color: palette(base); border-radius: 4px;}")
        frame.setFrameShape(QFrame.StyledPanel)
        main_layout.addWidget(frame)

        frame_layout = QHBoxLayout(frame)
        frame_layout.setContentsMargins(5, 5, 5, 5)

        self._status_icon = QLabel()
        frame_layout.addWidget(self._status_icon)

        self._profile_button = QPushButton()
        self._profile_button.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                border: none;
                padding-right: 5px;
                text-align: left;
            }
        """)

        self._auth_menu = RightAlignedQMenu(self._profile_button)

        self._show_profile_switch = show_profile_switch
        self._switch_profile_menu_action = self._auth_menu.addAction(
            "Switch profile", self.switch_profile_clicked.emit
        )
        self._logout_menu_action = self._auth_menu.addAction("Log out", self.logout_clicked.emit)

        frame_layout.addWidget(self._profile_button)

        frame_layout.addStretch()

        self._switch_profile_button = QPushButton("Switch profile")
        self._switch_profile_button.clicked.connect(self.switch_profile_clicked.emit)
        frame_layout.addWidget(self._switch_profile_button)

        self._login_button = QPushButton("Log in")
        self._login_button.clicked.connect(self.login_clicked.emit)
        frame_layout.addWidget(self._login_button)

        self._more_info_button = QPushButton("More info")
        self._more_info_button.clicked.connect(self._show_more_info)
        frame_layout.addWidget(self._more_info_button)

    def _get_profile_name(self) -> str:
        """
        Get the current AWS profile name from the configuration.

        Returns:
            str: The AWS profile name configured for Deadline Cloud authentication.
        """
        return config_file.get_setting("defaults.aws_profile_name", self._status.config)

    def _should_show_logout(self) -> bool:
        """
        Determine whether the logout option should be visible based on the credential source.

        The logout option is only shown when using Deadline Cloud Monitor credentials.

        Returns:
            bool: True if logout should be visible, False otherwise.
        """
        return self._status.creds_source == api.AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN

    def _get_auth_state_configs(self) -> Dict[AuthenticationState, AuthenticationStateConfig]:
        """
        Get the configuration mapping for each authentication state.

        Defines the UI appearance and behavior for each possible authentication state,
        including which icon to display, what text to show, and which buttons should
        be visible.

        Returns:
            Dict[AuthenticationState, AuthenticationStateConfig]: A dictionary mapping
                each authentication state to its corresponding UI configuration.
        """
        return {
            AuthenticationState.REFRESHING: AuthenticationStateConfig(
                icon=QStyle.StandardPixmap.SP_BrowserReload,
                text=self._get_profile_name,
            ),
            AuthenticationState.AUTHENTICATED_READY: AuthenticationStateConfig(
                icon=QStyle.StandardPixmap.SP_DialogApplyButton,
                text=self._get_profile_name,
                logout_visible=self._should_show_logout,
            ),
            AuthenticationState.AUTHENTICATED_NO_API: AuthenticationStateConfig(
                icon=QStyle.StandardPixmap.SP_MessageBoxWarning,
                text=lambda: f"{self._get_profile_name()} doesn't have access permissions to submit a job.",
                logout_visible=self._should_show_logout,
                more_info_visible=True,
            ),
            AuthenticationState.NEEDS_LOGIN: AuthenticationStateConfig(
                icon=QStyle.StandardPixmap.SP_MessageBoxWarning,
                text=f"{self._get_profile_name()}  -  You are logged out.",
                switch_profile_button_visible=True,
                login_visible=True,
            ),
            AuthenticationState.CONFIGURATION_ERROR: AuthenticationStateConfig(
                icon=QStyle.StandardPixmap.SP_MessageBoxWarning,
                text=lambda: f"A configuration error was received while accessing credentials for the profile '{self._get_profile_name()}'.",
                more_info_visible=True,
            ),
            AuthenticationState.UNEXPECTED_ERROR: AuthenticationStateConfig(
                icon=QStyle.StandardPixmap.SP_MessageBoxWarning,
                text="There was an error with authentication",
                more_info_visible=True,
            ),
        }

    def _get_current_auth_state_key(self) -> AuthenticationState:
        """
        Evaluates the credentials source, authentication status, and API availability
        to determine which authentication state the widget should display.

        Returns:
            AuthenticationState: The current authentication state enum value
        """
        is_refreshing = (
            self._status.creds_source is None
            or self._status.auth_status is None
            or self._status.api_availability is None
        )

        if is_refreshing:
            return AuthenticationState.REFRESHING
        elif (
            self._status.auth_status == api.AwsAuthenticationStatus.AUTHENTICATED
            and self._status.api_availability
        ):
            return AuthenticationState.AUTHENTICATED_READY
        elif (
            self._status.auth_status == api.AwsAuthenticationStatus.AUTHENTICATED
            and not self._status.api_availability
        ):
            return AuthenticationState.AUTHENTICATED_NO_API
        elif self._status.auth_status == api.AwsAuthenticationStatus.NEEDS_LOGIN:
            return AuthenticationState.NEEDS_LOGIN
        elif self._status.auth_status == api.AwsAuthenticationStatus.CONFIGURATION_ERROR:
            return AuthenticationState.CONFIGURATION_ERROR
        else:
            return AuthenticationState.UNEXPECTED_ERROR

    def _show_more_info(self) -> None:
        """
        Show a QMessageBox with detailed information about the current authentication issue
        and hopefully steps the user can take to resolve it.
        """

        current_state = self._get_current_auth_state_key()

        # Determine the current authentication state and provide appropriate help
        if current_state == AuthenticationState.AUTHENTICATED_NO_API:
            # Authenticated but no API availability - permissions issue or possibly configuration issue
            title = "Unable to Call AWS Deadline Cloud API"
            message = (
                f"You are authenticated with the profile '{self._get_profile_name()}', "
                "but this profile is unable to call AWS Deadline Cloud ListFarms and unable to submit jobs to AWS Deadline Cloud.\n\n"
                "To resolve this issue:\n\n"
                "• Check that there aren't any environment variables pointing to the wrong AWS region (e.g., AWS_DEFAULT_REGION)\n"
                "• If you are not using a Deadline Cloud Monitor profile, check that the profile has permissions for these AWS Deadline Cloud APIs needed for submitting:\n"
                "  • deadline:AssumeQueueRoleForUser\n"
                "  • deadline:CreateJob\n"
                "  • deadline:GetJob\n"
                "  • deadline:GetQueue\n"
                "  • deadline:GetQueueEnvironment\n"
                "  • deadline:GetStorageProfileForQueue\n"
                "  • deadline:GetStorageProfile\n"
                "  • deadline:ListFarms\n"
                "  • deadline:ListQueues\n"
                "  • deadline:ListQueueEnvironments\n"
                "  • deadline:ListStorageProfilesForQueue"
            )
        elif self._status.auth_status == api.AwsAuthenticationStatus.CONFIGURATION_ERROR:
            title = "Issue With Profile Configuration"
            message = (
                f"There is a configuration issue with the profile '{self._get_profile_name()}'.\n\n"
                "To resolve this issue:\n"
                "• Verify your AWS config and credentials files are correct\n"
                "  • By default these files can be found in ~/.aws on Linux/MacOS or %USERPROFILE%/.aws on Windows\n"
                "• Verify that the correct AWS region is set\n"
                "  • Check that no environment variables like AWS_DEFAULT_REGION are set to an incorrect region\n"
                "• If you are not using a Deadline Cloud Monitor profile:\n"
                "  • Verify that any credential process being used is able to retrieve the credentials or that they aren't expired\n"
                "    • You can run the following command to check: aws sts get-caller-identity --profile <PROFILE_NAME>"
            )
        else:
            title = "Unknown Issue With Configured Profile"
            message = (
                f"There was an unknown issue when trying to authenticate with the profile '{self._get_profile_name()}'.\n\n"
                "Check any available console logs for errors to try and diagnose the problem.\n"
                "Logs are commonly found:\n"
                "  • In the terminal that the dialog or software the submitter is running in was launched from"
                "  • In the built-in console within the software that the submitter is running in"
            )

        # Show the message box
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.setIcon(QMessageBox.Information)
        msg_box.setStandardButtons(QMessageBox.Ok)
        msg_box.exec_()

    def _update_ui(self) -> None:
        """
        Update the UI based on the current authentication state.

        This method is called whenever the authentication status changes.
        It determines the current state and applies the appropriate UI configuration.
        """
        state_key = self._get_current_auth_state_key()
        state_configs = self._get_auth_state_configs()
        config = state_configs[state_key]

        self._apply_ui_state(config)

    def _apply_ui_state(self, config: AuthenticationStateConfig) -> None:
        """
        Apply the UI configuration for a specific authentication state.

        Updates the widget's appearance and behavior based on the provided configuration,
        including setting the status icon, profile button text, and controlling the
        visibility of various action buttons and menu items.

        Args:
            config (AuthenticationStateConfig): The configuration object containing
                UI settings for the current authentication state.
        """
        # Set icon
        self._status_icon.setPixmap(QApplication.style().standardIcon(config.icon).pixmap(16, 16))

        # Set text
        text = config.text() if callable(config.text) else config.text
        self._profile_button.setText(text)
        self._profile_button.setToolTip(text)

        # Set visibility states
        # Only one of the switch profile button or menu action are shown at a time
        self._switch_profile_button.setVisible(
            config.switch_profile_button_visible and self._show_profile_switch
        )
        self._switch_profile_menu_action.setVisible(
            not config.switch_profile_button_visible and self._show_profile_switch
        )

        self._login_button.setVisible(config.login_visible)
        logout_visible = (
            config.logout_visible() if callable(config.logout_visible) else config.logout_visible
        )
        self._logout_menu_action.setVisible(logout_visible)
        self._more_info_button.setVisible(config.more_info_visible)

        # Hide the dropdown menu if there are no visible actions for the current state
        if any(action.isVisible() for action in self._auth_menu.actions()):
            self._profile_button.setMenu(self._auth_menu)
        else:
            self._profile_button.setMenu(None)
