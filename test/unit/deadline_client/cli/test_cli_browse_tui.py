# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for _browse_tui utility functions and classes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("rich", reason="TUI tests require the 'rich' package (deadline[tui])")

from deadline.client.cli._groups._browse_tui import (
    NodeType,
    TreeNode,
    build_file_tree,
    get_all_files_under,
    get_node_icon,
    is_image,
    load_input_manifests,
    load_output_manifests,
    render_breadcrumb,
    render_file_list,
    render_header,
)


class TestIsImage:
    def test_png(self):
        assert is_image("photo.png") is True

    def test_jpg(self):
        assert is_image("photo.JPG") is True

    def test_txt(self):
        assert is_image("readme.txt") is False

    def test_no_extension(self):
        assert is_image("file") is False


class TestGetAllFilesUnder:
    def test_single_file(self):
        f = TreeNode("a.txt", NodeType.FILE)
        assert get_all_files_under(f) == [f]

    def test_folder_with_files(self):
        folder = TreeNode("dir", NodeType.FOLDER)
        f1 = TreeNode("a.txt", NodeType.FILE, parent=folder)
        f2 = TreeNode("b.txt", NodeType.FILE, parent=folder)
        folder.children = [f1, f2]
        assert get_all_files_under(folder) == [f1, f2]

    def test_nested(self):
        root = TreeNode("root", NodeType.ROOT)
        sub = TreeNode("sub", NodeType.FOLDER, parent=root)
        f = TreeNode("a.txt", NodeType.FILE, parent=sub)
        sub.children = [f]
        root.children = [sub]
        assert get_all_files_under(root) == [f]

    def test_empty_folder(self):
        folder = TreeNode("dir", NodeType.FOLDER)
        assert get_all_files_under(folder) == []


class TestBuildFileTree:
    def test_single_file(self):
        manifest = MagicMock()
        mp = MagicMock()
        mp.path = "file.txt"
        mp.size = 100
        mp.hash = "abc123"
        manifest.paths = [mp]
        tree = build_file_tree(manifest, "/root")
        assert tree.name == "/root"
        assert tree.node_type == NodeType.MANIFEST_ROOT
        assert len(tree.children) == 1
        assert tree.children[0].name == "file.txt"
        assert tree.children[0].node_type == NodeType.FILE
        assert tree.children[0].size == 100

    def test_nested_path(self):
        manifest = MagicMock()
        mp = MagicMock()
        mp.path = "dir/sub/file.txt"
        mp.size = 50
        mp.hash = "def456"
        manifest.paths = [mp]
        tree = build_file_tree(manifest, "/root")
        assert tree.children[0].name == "dir"
        assert tree.children[0].node_type == NodeType.FOLDER
        assert tree.children[0].children[0].name == "sub"
        assert tree.children[0].children[0].children[0].name == "file.txt"

    def test_shared_prefix(self):
        manifest = MagicMock()
        mp1 = MagicMock()
        mp1.path = "dir/a.txt"
        mp1.size = 10
        mp1.hash = "h1"
        mp2 = MagicMock()
        mp2.path = "dir/b.txt"
        mp2.size = 20
        mp2.hash = "h2"
        manifest.paths = [mp1, mp2]
        tree = build_file_tree(manifest, "/root")
        assert len(tree.children) == 1  # single "dir" folder
        assert len(tree.children[0].children) == 2  # two files

    def test_empty_manifest(self):
        manifest = MagicMock()
        manifest.paths = []
        tree = build_file_tree(manifest, "/root")
        assert tree.children == []


class TestGetNodeIcon:
    def test_image_file(self):
        node = TreeNode("photo.png", NodeType.FILE)
        assert "🖼️" in get_node_icon(node)

    def test_text_file(self):
        node = TreeNode("readme.txt", NodeType.FILE)
        assert get_node_icon(node) == "📝"

    def test_log_file(self):
        node = TreeNode("output.log", NodeType.FILE)
        assert get_node_icon(node) == "📝"

    def test_md_file(self):
        node = TreeNode("README.md", NodeType.FILE)
        assert get_node_icon(node) == "📝"

    def test_generic_file(self):
        node = TreeNode("data.bin", NodeType.FILE)
        assert get_node_icon(node) == "📄"

    def test_folder(self):
        node = TreeNode("dir", NodeType.FOLDER)
        assert get_node_icon(node) == "📁"

    def test_manifest_root(self):
        node = TreeNode("root", NodeType.MANIFEST_ROOT)
        assert get_node_icon(node) == "💾"

    def test_category(self):
        node = TreeNode("input", NodeType.CATEGORY)
        assert get_node_icon(node) == "📦"

    def test_root(self):
        node = TreeNode("job", NodeType.ROOT)
        assert get_node_icon(node) == "📂"


class TestRenderFunctions:
    @patch("deadline.client.cli._groups._browse_tui.console")
    def test_render_header(self, mock_console):
        render_header("Test Title", "subtitle")
        mock_console.print.assert_called_once()

    @patch("deadline.client.cli._groups._browse_tui.console")
    def test_render_breadcrumb(self, mock_console):
        root = TreeNode("root", NodeType.ROOT)
        child = TreeNode("child", NodeType.FOLDER, parent=root)
        render_breadcrumb(child)
        mock_console.print.assert_called_once()
        call_str = str(mock_console.print.call_args)
        assert "root" in call_str
        assert "child" in call_str

    @patch("deadline.client.cli._groups._browse_tui.console")
    def test_render_file_list_empty(self, mock_console):
        render_file_list([], 0)
        mock_console.print.assert_called_once()

    @patch("deadline.client.cli._groups._browse_tui.console")
    def test_render_file_list_with_items(self, mock_console):
        f = TreeNode("a.txt", NodeType.FILE, size=100)
        folder = TreeNode("dir", NodeType.FOLDER)
        render_file_list([f, folder], 0)
        mock_console.print.assert_called_once()


class TestLoadInputManifests:
    @patch("deadline.job_attachments.download.get_manifest_from_s3")
    def test_loads_manifests(self, mock_get_manifest):
        mock_manifest = MagicMock()
        mp = MagicMock()
        mp.path = "file.txt"
        mp.size = 100
        mp.hash = "abc"
        mock_manifest.paths = [mp]
        mock_get_manifest.return_value = mock_manifest

        job = {
            "attachments": {
                "manifests": [{"inputManifestPath": "manifest.json", "rootPath": "/data"}]
            }
        }
        trees = load_input_manifests(job, "prefix", "bucket", MagicMock())
        assert len(trees) == 1
        assert trees[0].name == "/data"

    @patch("deadline.job_attachments.download.get_manifest_from_s3")
    def test_skips_empty_input_path(self, mock_get_manifest):
        job = {"attachments": {"manifests": [{"inputManifestPath": "", "rootPath": "/data"}]}}
        trees = load_input_manifests(job, "prefix", "bucket", MagicMock())
        assert len(trees) == 0
        mock_get_manifest.assert_not_called()

    def test_no_attachments(self):
        job: dict = {}
        trees = load_input_manifests(job, "prefix", "bucket", MagicMock())
        assert len(trees) == 0

    @patch("deadline.job_attachments.download.get_manifest_from_s3")
    def test_skips_none_manifest(self, mock_get_manifest):
        mock_get_manifest.return_value = None
        job = {
            "attachments": {
                "manifests": [{"inputManifestPath": "manifest.json", "rootPath": "/data"}]
            }
        }
        trees = load_input_manifests(job, "prefix", "bucket", MagicMock())
        assert len(trees) == 0


class TestLoadOutputManifests:
    @patch("deadline.job_attachments.download.merge_asset_manifests")
    @patch("deadline.job_attachments.download.get_output_manifests_by_asset_root")
    def test_loads_output_manifests(self, mock_get_output, mock_merge):
        mock_manifest = MagicMock()
        mp = MagicMock()
        mp.path = "output.exr"
        mp.size = 5000
        mp.hash = "xyz"
        mock_manifest.paths = [mp]
        mock_get_output.return_value = {"/output": [mock_manifest]}
        mock_merge.return_value = mock_manifest

        trees = load_output_manifests(MagicMock(), "farm-1", "queue-1", "job-1", MagicMock())
        assert len(trees) == 1
        assert trees[0].name == "/output"

    @patch("deadline.job_attachments.download.merge_asset_manifests")
    @patch("deadline.job_attachments.download.get_output_manifests_by_asset_root")
    def test_skips_none_merged(self, mock_get_output, mock_merge):
        mock_get_output.return_value = {"/output": [MagicMock()]}
        mock_merge.return_value = None
        trees = load_output_manifests(MagicMock(), "farm-1", "queue-1", "job-1", MagicMock())
        assert len(trees) == 0


class TestDownloadSingleFile:
    @patch("deadline.client.cli._groups._browse_tui.os")
    def test_download(self, mock_os):
        from deadline.client.cli._groups._browse_tui import download_single_file

        mock_os.path.join.return_value = "/tmp/file.txt"
        session = MagicMock()
        s3_settings = MagicMock()
        s3_settings.rootPrefix = "prefix"
        s3_settings.s3BucketName = "bucket"
        node = TreeNode("file.txt", NodeType.FILE, hash="abc123")
        result = download_single_file(session, s3_settings, node, "/tmp")
        session.client.assert_called_once_with("s3")
        assert result == "/tmp/file.txt"


class TestShowFileInfo:
    @patch("deadline.client.cli._groups._browse_tui.click")
    @patch("deadline.client.cli._groups._browse_tui.console")
    def test_show_file_info(self, mock_console, mock_click):
        from deadline.client.cli._groups._browse_tui import show_file_info

        node = TreeNode("file.txt", NodeType.FILE, path="dir/file.txt", size=1024, hash="abc")
        show_file_info(node)
        assert mock_console.clear.called
        assert mock_click.getchar.called


class TestShowManifestList:
    @patch("deadline.client.cli._groups._browse_tui.click")
    @patch("deadline.client.cli._groups._browse_tui.console")
    def test_show_manifest_list(self, mock_console, mock_click):
        from deadline.client.cli._groups._browse_tui import show_manifest_list

        root = TreeNode("Job", NodeType.ROOT)
        cat = TreeNode("input", NodeType.CATEGORY, parent=root)
        manifest = TreeNode("/data", NodeType.MANIFEST_ROOT, parent=cat)
        f = TreeNode("file.txt", NodeType.FILE, parent=manifest)
        manifest.children = [f]
        cat.children = [manifest]
        root.children = [cat]
        show_manifest_list(root)
        assert mock_console.clear.called


class TestPreviewFileContent:
    @patch("deadline.client.cli._groups._browse_tui.click")
    @patch("deadline.client.cli._groups._browse_tui.console")
    @patch("deadline.client.cli._groups._browse_tui.download_single_file")
    def test_preview_text_file(self, mock_download, mock_console, mock_click, tmp_path):
        from deadline.client.cli._groups._browse_tui import preview_file_content

        # Create a temp text file
        text_file = tmp_path / "test.txt"
        text_file.write_text("hello world")
        mock_download.return_value = str(text_file)

        node = TreeNode("test.txt", NodeType.FILE, size=11, hash="abc")
        preview_file_content(MagicMock(), MagicMock(), node)
        assert mock_console.clear.called

    @patch("deadline.client.cli._groups._browse_tui.click")
    @patch("deadline.client.cli._groups._browse_tui.console")
    @patch("deadline.client.cli._groups._browse_tui.download_single_file")
    def test_preview_binary_file(self, mock_download, mock_console, mock_click, tmp_path):
        from deadline.client.cli._groups._browse_tui import preview_file_content

        bin_file = tmp_path / "test.bin"
        bin_file.write_bytes(b"\x00\x01\x02\x03" * 10)
        mock_download.return_value = str(bin_file)

        node = TreeNode("test.bin", NodeType.FILE, size=40, hash="abc")
        preview_file_content(MagicMock(), MagicMock(), node)
        assert mock_console.clear.called

    @patch("deadline.client.cli._groups._browse_tui.click")
    @patch("deadline.client.cli._groups._browse_tui.console")
    @patch("deadline.client.cli._groups._browse_tui.download_single_file")
    def test_preview_python_file(self, mock_download, mock_console, mock_click, tmp_path):
        from deadline.client.cli._groups._browse_tui import preview_file_content

        py_file = tmp_path / "test.py"
        py_file.write_text("print('hello')")
        mock_download.return_value = str(py_file)

        node = TreeNode("test.py", NodeType.FILE, size=14, hash="abc")
        preview_file_content(MagicMock(), MagicMock(), node)
        assert mock_console.clear.called

    @patch("deadline.client.cli._groups._browse_tui.click")
    @patch("deadline.client.cli._groups._browse_tui.console")
    @patch("deadline.client.cli._groups._browse_tui.download_single_file")
    def test_preview_image_file(self, mock_download, mock_console, mock_click, tmp_path):
        from deadline.client.cli._groups._browse_tui import preview_file_content

        img_file = tmp_path / "test.png"
        img_file.write_bytes(b"\x89PNG")
        mock_download.return_value = str(img_file)

        node = TreeNode("test.png", NodeType.FILE, size=4, hash="abc")
        preview_file_content(MagicMock(), MagicMock(), node)
        assert mock_console.clear.called


class TestOpenImageViewer:
    @patch("deadline.client.cli._groups._browse_tui.subprocess")
    @patch(
        "deadline.client.cli._groups._browse_tui.download_single_file", return_value="/tmp/img.png"
    )
    def test_open_image_darwin(self, mock_download, mock_subprocess):
        from deadline.client.cli._groups._browse_tui import open_image_viewer

        with patch("deadline.client.cli._groups._browse_tui.sys") as mock_sys:
            mock_sys.platform = "darwin"
            open_image_viewer(MagicMock(), MagicMock(), TreeNode("img.png", NodeType.FILE))
            mock_subprocess.run.assert_called_once_with(["open", "/tmp/img.png"])

    @patch("deadline.client.cli._groups._browse_tui.subprocess")
    @patch(
        "deadline.client.cli._groups._browse_tui.download_single_file", return_value="/tmp/img.png"
    )
    def test_open_image_linux(self, mock_download, mock_subprocess):
        from deadline.client.cli._groups._browse_tui import open_image_viewer

        with patch("deadline.client.cli._groups._browse_tui.sys") as mock_sys:
            mock_sys.platform = "linux"
            open_image_viewer(MagicMock(), MagicMock(), TreeNode("img.png", NodeType.FILE))
            mock_subprocess.run.assert_called_once_with(["xdg-open", "/tmp/img.png"])


class TestDownloadFolder:
    @patch("deadline.client.cli._groups._browse_tui.console")
    def test_download_folder(self, mock_console):
        from deadline.client.cli._groups._browse_tui import download_folder

        session = MagicMock()
        s3_settings = MagicMock()
        s3_settings.rootPrefix = "prefix"
        s3_settings.s3BucketName = "bucket"

        folder = TreeNode("dir", NodeType.FOLDER)
        f1 = TreeNode("a.txt", NodeType.FILE, path="dir/a.txt", hash="h1", parent=folder)
        folder.children = [f1]

        count = download_folder(session, s3_settings, folder, "/tmp/dest")
        assert count == 1
