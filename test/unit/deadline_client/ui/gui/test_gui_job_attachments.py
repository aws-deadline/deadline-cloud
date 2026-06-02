# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI tests for JobAttachmentsWidget using pytest-qt."""

from deadline.client.ui.widgets.job_attachments_tab import JobAttachmentsWidget
from deadline.client.job_bundle.submission import AssetReferences


class TestJobAttachmentsWidgetCreation:
    def test_creates_with_empty_asset_references(self, qtbot):
        """Widget creates successfully with empty AssetReferences."""
        widget = JobAttachmentsWidget(
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        assert widget.input_files.count() == 0
        assert widget.input_directories.count() == 0
        assert widget.output_directories.count() == 0

    def test_creates_with_populated_auto_detected_attachments(self, qtbot):
        """Widget shows auto-detected attachments in the lists."""
        auto = AssetReferences(
            input_filenames={"/tmp/scene.ma", "/tmp/texture.png"},
            input_directories={"/tmp/assets"},
            output_directories={"/tmp/output"},
        )
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        assert widget.input_files.count() == 2
        assert widget.input_directories.count() == 1
        assert widget.output_directories.count() == 1

        input_file_texts = {
            widget.input_files.item(i).text() for i in range(widget.input_files.count())
        }
        assert input_file_texts == {"/tmp/scene.ma", "/tmp/texture.png"}

    def test_auto_detected_items_are_italic(self, qtbot):
        """Auto-detected items should be rendered in italic font."""
        auto = AssetReferences(input_filenames={"/tmp/auto_file.txt"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        item = widget.input_files.item(0)
        assert item.font().italic()

    def test_user_added_items_are_not_italic(self, qtbot):
        """User-added items should not be rendered in italic font."""
        added = AssetReferences(input_filenames={"/tmp/user_file.txt"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=AssetReferences(),
            attachments=added,
        )
        qtbot.addWidget(widget)

        item = widget.input_files.item(0)
        assert not item.font().italic()


class TestGetAssetReferences:
    def test_returns_union_of_auto_detected_and_user(self, qtbot):
        """get_asset_references() returns the union of both attachment sets."""
        auto = AssetReferences(
            input_filenames={"/tmp/auto.ma"},
            input_directories={"/tmp/auto_dir"},
            output_directories={"/tmp/auto_out"},
        )
        added = AssetReferences(
            input_filenames={"/tmp/user.ma"},
            input_directories={"/tmp/user_dir"},
            output_directories={"/tmp/user_out"},
        )
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        result = widget.get_asset_references()

        assert result.input_filenames == {"/tmp/auto.ma", "/tmp/user.ma"}
        assert result.input_directories == {"/tmp/auto_dir", "/tmp/user_dir"}
        assert result.output_directories == {"/tmp/auto_out", "/tmp/user_out"}

    def test_returns_correct_union_when_auto_detected_hidden(self, qtbot):
        """get_asset_references() returns full union even when auto-detected items are hidden."""
        auto = AssetReferences(
            input_filenames={"/tmp/auto.ma"},
            input_directories={"/tmp/auto_dir"},
        )
        added = AssetReferences(
            input_filenames={"/tmp/user.ma"},
        )
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        # Hide auto-detected items in all sections
        widget.input_files_controls.show_auto_detected.setChecked(False)
        widget.input_directories_controls.show_auto_detected.setChecked(False)
        widget.output_directories_controls.show_auto_detected.setChecked(False)

        result = widget.get_asset_references()

        # Union should still include everything regardless of visibility
        assert result.input_filenames == {"/tmp/auto.ma", "/tmp/user.ma"}
        assert result.input_directories == {"/tmp/auto_dir"}


class TestRequirePathsExist:
    def test_defaults_to_unchecked(self, qtbot):
        """get_require_paths_exist() defaults to False (unchecked)."""
        widget = JobAttachmentsWidget(
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        assert widget.get_require_paths_exist() is False

    def test_toggling_checkbox_changes_return_value(self, qtbot):
        """Checking the require_paths_exist checkbox makes get_require_paths_exist() return True."""
        widget = JobAttachmentsWidget(
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        widget.general_settings.require_paths_exist.setChecked(True)
        assert widget.get_require_paths_exist() is True

        widget.general_settings.require_paths_exist.setChecked(False)
        assert widget.get_require_paths_exist() is False


class TestRefreshUI:
    def test_refresh_with_new_auto_detected_updates_lists(self, qtbot):
        """refresh_ui with new auto-detected attachments updates the displayed lists."""
        widget = JobAttachmentsWidget(
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        assert widget.input_files.count() == 0

        new_auto = AssetReferences(
            input_filenames={"/tmp/new_auto.ma", "/tmp/new_auto2.ma"},
        )
        widget.refresh_ui(auto_detected_attachments=new_auto, attachments=None)

        assert widget.input_files.count() == 2

    def test_refresh_preserves_user_attachments_when_only_auto_changes(self, qtbot):
        """refresh_ui with only new auto-detected preserves existing user attachments."""
        added = AssetReferences(input_filenames={"/tmp/user.ma"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=AssetReferences(),
            attachments=added,
        )
        qtbot.addWidget(widget)

        assert widget.input_files.count() == 1

        new_auto = AssetReferences(input_filenames={"/tmp/auto.ma"})
        widget.refresh_ui(auto_detected_attachments=new_auto, attachments=None)

        # Should now show both auto-detected and user items
        assert widget.input_files.count() == 2
        texts = {widget.input_files.item(i).text() for i in range(widget.input_files.count())}
        assert texts == {"/tmp/auto.ma", "/tmp/user.ma"}


class TestShowAutoDetectedToggle:
    def test_unchecking_hides_auto_detected_items(self, qtbot):
        """Unchecking 'Show auto-detected' hides auto-detected items from list."""
        auto = AssetReferences(input_filenames={"/tmp/auto.ma"})
        added = AssetReferences(input_filenames={"/tmp/user.ma"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        # Both items should be visible initially
        assert widget.input_files.count() == 2

        # Uncheck to hide auto-detected
        widget.input_files_controls.show_auto_detected.setChecked(False)

        # Only user item should remain
        assert widget.input_files.count() == 1
        assert widget.input_files.item(0).text() == "/tmp/user.ma"

    def test_rechecking_shows_auto_detected_items_again(self, qtbot):
        """Re-checking 'Show auto-detected' brings auto-detected items back."""
        auto = AssetReferences(input_filenames={"/tmp/auto.ma"})
        added = AssetReferences(input_filenames={"/tmp/user.ma"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        widget.input_files_controls.show_auto_detected.setChecked(False)
        assert widget.input_files.count() == 1

        widget.input_files_controls.show_auto_detected.setChecked(True)
        assert widget.input_files.count() == 2

    def test_toggle_affects_only_its_own_section(self, qtbot):
        """Toggling show_auto_detected for input files does not affect input directories."""
        auto = AssetReferences(
            input_filenames={"/tmp/auto_file.ma"},
            input_directories={"/tmp/auto_dir"},
        )
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        assert widget.input_files.count() == 1
        assert widget.input_directories.count() == 1

        # Hide auto-detected for input files only
        widget.input_files_controls.show_auto_detected.setChecked(False)

        assert widget.input_files.count() == 0
        # Input directories should be unaffected
        assert widget.input_directories.count() == 1


class TestStatusMessages:
    def test_status_shows_correct_counts(self, qtbot):
        """Status message shows correct auto/added/selected counts."""
        auto = AssetReferences(input_filenames={"/tmp/auto1.ma", "/tmp/auto2.ma"})
        added = AssetReferences(input_filenames={"/tmp/user1.ma"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        status = widget.input_files_controls.status_message.text()
        assert "2 auto" in status
        assert "1 added" in status
        assert "0 selected" in status

    def test_status_updates_after_refresh(self, qtbot):
        """Status message updates after refresh_ui is called."""
        widget = JobAttachmentsWidget(
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        status = widget.input_files_controls.status_message.text()
        assert "0 auto" in status
        assert "0 added" in status

        new_auto = AssetReferences(input_filenames={"/tmp/a.ma", "/tmp/b.ma", "/tmp/c.ma"})
        widget.refresh_ui(auto_detected_attachments=new_auto, attachments=None)

        status = widget.input_files_controls.status_message.text()
        assert "3 auto" in status
        assert "0 added" in status

    def test_status_reflects_zero_counts_for_empty_sections(self, qtbot):
        """Sections with no attachments show zero counts."""
        auto = AssetReferences(input_filenames={"/tmp/file.ma"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=AssetReferences(),
        )
        qtbot.addWidget(widget)

        dirs_status = widget.input_directories_controls.status_message.text()
        assert "0 auto" in dirs_status
        assert "0 added" in dirs_status

        out_status = widget.output_directories_controls.status_message.text()
        assert "0 auto" in out_status
        assert "0 added" in out_status


class TestDeduplication:
    def test_duplicate_in_both_sets_shown_once(self, qtbot):
        """If an attachment is in both auto-detected and user sets, it appears only once."""
        shared_file = "/tmp/shared.ma"
        auto = AssetReferences(input_filenames={shared_file})
        added = AssetReferences(input_filenames={shared_file})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        # _populate_attachment_lists removes duplicates from the added set
        assert widget.input_files.count() == 1
        assert widget.input_files.item(0).text() == shared_file

    def test_dedup_removes_from_added_set(self, qtbot):
        """Deduplication removes the item from the user-added set, keeping auto-detected."""
        shared_file = "/tmp/shared.ma"
        auto = AssetReferences(input_filenames={shared_file})
        added = AssetReferences(input_filenames={shared_file, "/tmp/unique_user.ma"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        # The shared file should have been removed from the added set
        assert shared_file not in widget.attachments.input_filenames
        assert "/tmp/unique_user.ma" in widget.attachments.input_filenames

    def test_dedup_across_all_sections(self, qtbot):
        """Deduplication works for input directories and output directories too."""
        shared_dir = "/tmp/shared_dir"
        shared_out = "/tmp/shared_out"
        auto = AssetReferences(
            input_directories={shared_dir},
            output_directories={shared_out},
        )
        added = AssetReferences(
            input_directories={shared_dir},
            output_directories={shared_out},
        )
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        assert widget.input_directories.count() == 1
        assert widget.output_directories.count() == 1

    def test_dedup_union_still_correct(self, qtbot):
        """get_asset_references() is correct after deduplication (no double counting)."""
        shared_file = "/tmp/shared.ma"
        auto = AssetReferences(input_filenames={shared_file, "/tmp/auto_only.ma"})
        added = AssetReferences(input_filenames={shared_file, "/tmp/user_only.ma"})
        widget = JobAttachmentsWidget(
            auto_detected_attachments=auto,
            attachments=added,
        )
        qtbot.addWidget(widget)

        result = widget.get_asset_references()
        assert result.input_filenames == {shared_file, "/tmp/auto_only.ma", "/tmp/user_only.ma"}
