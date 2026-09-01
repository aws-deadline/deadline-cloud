# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the path picker widgets' home-directory collapsing."""

import ntpath
import os
import posixpath

import pytest

# importorskip, not a try/except: it binds the module when Qt is available and skips the
# file when it is not, where the except branch would leave the name unbound and every case
# would fail with NameError instead.
_path_widgets = pytest.importorskip("deadline.client.ui.widgets.path_widgets")
_collapse_user_dir = _path_widgets._collapse_user_dir
DirectoryPickerWidget = _path_widgets.DirectoryPickerWidget
InputFilePickerWidget = _path_widgets.InputFilePickerWidget


def _home_module(module, home):
    """``module`` with ``expanduser`` pinned to ``home``, so the cases do not depend on
    the home directory of whoever runs them."""

    class _PinnedHome:
        @staticmethod
        def expanduser(path):
            return home if path == "~" else path

        def __getattr__(self, name):
            return getattr(module, name)

    return _PinnedHome()


class TestCollapseUserDir:
    """
    The collapsed text is written straight into settings by the config dialog
    (job_history_dir, job_bundle_default_directory), so a path that is not really inside
    the home directory must come back untouched rather than rewritten.
    """

    @pytest.mark.parametrize(
        "path, expected",
        [
            (r"C:\Users\bob\projects\scene.ma", r"~\projects\scene.ma"),
            (r"C:\Users\bob\scene.ma", r"~\scene.ma"),
            (r"C:\Users\bob", "~"),
            # Case-insensitive, like the filesystem.
            (r"c:\users\BOB\projects\scene.ma", r"~\projects\scene.ma"),
            # A sibling that merely shares a string prefix is not inside the home
            # directory. Slicing by the home directory's length rewrote the first of
            # these to '~\r\projects\scene.ma' and the second to '\projects\scene.ma',
            # because join() drops the '~' when what follows is rooted.
            (r"C:\Users\bobby\projects\scene.ma", r"C:\Users\bobby\projects\scene.ma"),
            (r"C:\Users\bob2\projects\scene.ma", r"C:\Users\bob2\projects\scene.ma"),
            # Elsewhere entirely, including another path space.
            (r"D:\projects\scene.ma", r"D:\projects\scene.ma"),
            (r"\\host\share\scene.ma", r"\\host\share\scene.ma"),
            (r"C:\Users", r"C:\Users"),
        ],
    )
    def test_windows(self, path, expected):
        assert (
            _collapse_user_dir(path, path_module=_home_module(ntpath, r"C:\Users\bob")) == expected
        )

    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/home/bob/projects/scene.ma", "~/projects/scene.ma"),
            ("/home/bob", "~"),
            ("/home/bobby/projects/scene.ma", "/home/bobby/projects/scene.ma"),
            ("/home/bob2/projects/scene.ma", "/home/bob2/projects/scene.ma"),
            ("/mnt/share/scene.ma", "/mnt/share/scene.ma"),
            ("/home", "/home"),
            # POSIX paths are case-sensitive, so a case variant is a different directory.
            ("/home/BOB/projects/scene.ma", "/home/BOB/projects/scene.ma"),
        ],
    )
    def test_posix(self, path, expected):
        assert (
            _collapse_user_dir(path, path_module=_home_module(posixpath, "/home/bob")) == expected
        )

    def test_round_trips_through_expanduser(self):
        """The collapsed text is expanded again when the widget is read back, so the two
        have to agree -- that is what makes a wrong collapse persist as a wrong path."""
        module = _home_module(posixpath, "/home/bob")
        for path in ("/home/bob/projects/scene.ma", "/home/bobby/projects/scene.ma"):
            collapsed = _collapse_user_dir(path, path_module=module)
            assert posixpath.expanduser(collapsed.replace("~", "/home/bob", 1)) == path


class TestWidgetsCollapseOnSetText:
    """
    The helper being right is not enough: the widgets have to call it. Both of these pass
    with the calls deleted if only the helper is tested, and the collapsed text is what the
    config dialog persists.
    """

    def test_directory_picker_collapses_a_path_in_the_home_directory(self, qtbot):
        home = os.path.expanduser("~")
        widget = DirectoryPickerWidget(
            initial_directory="", directory_label="Test Dir", collapse_user_dir=True
        )
        qtbot.addWidget(widget)

        widget.setText(os.path.join(home, "projects"))
        assert widget.text() == os.path.join("~", "projects")

        # A sibling that merely shares the prefix must survive untouched.
        widget.setText(os.path.join(home + "2", "projects"))
        assert widget.text() == os.path.join(home + "2", "projects")

    def test_file_picker_collapses_a_path_in_the_home_directory(self, qtbot):
        home = os.path.expanduser("~")
        widget = InputFilePickerWidget(
            initial_filename="",
            file_label="Test File",
            filter="All Files (*)",
            selected_filter="All Files (*)",
            collapse_user_dir=True,
        )
        qtbot.addWidget(widget)

        widget.setText(os.path.join(home, "scene.ma"))
        assert widget.text() == os.path.join("~", "scene.ma")

        widget.setText(os.path.join(home + "2", "scene.ma"))
        assert widget.text() == os.path.join(home + "2", "scene.ma")

    def test_collapsing_is_opt_in(self, qtbot):
        """The default leaves the path alone, which is what the pickers that store an
        absolute path depend on."""
        home = os.path.expanduser("~")
        widget = DirectoryPickerWidget(initial_directory="", directory_label="Test Dir")
        qtbot.addWidget(widget)

        widget.setText(os.path.join(home, "projects"))
        assert widget.text() == os.path.join(home, "projects")
