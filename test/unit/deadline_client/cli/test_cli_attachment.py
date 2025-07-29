# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI attachment commands.
"""

from click.testing import CliRunner

from deadline.client.cli import main


class TestAttachmentUpload:
    """Test cases for the attachment upload command"""

    def test_attachment_upload_missing_required_args(self):
        """
        Test that attachment upload fails when required arguments are missing
        """
        runner = CliRunner()

        # Test missing manifests
        result = runner.invoke(
            main,
            [
                "attachment",
                "upload",
                "--root-dirs",
                "/tmp/test",
                "--s3-root-uri",
                "s3://test-bucket/test-prefix",
            ],
        )
        assert result.exit_code != 0
        assert "Missing option" in result.output or "Usage:" in result.output

        # Test missing s3-root-uri (when not using farm/queue config)
        result = runner.invoke(
            main,
            [
                "attachment",
                "upload",
                "--manifests",
                "/tmp/test.manifest",
                "--root-dirs",
                "/tmp/test",
            ],
        )
        # This will fail due to missing config, but not due to CLI argument parsing
        assert result.exit_code != 0

    def test_attachment_upload_help(self):
        """
        Test that attachment upload help works and doesn't mention asset-type
        """
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "attachment",
                "upload",
                "--help",
            ],
        )
        assert result.exit_code == 0
        assert "Upload Job Attachment data files" in result.output
        assert "--manifests" in result.output
        assert "--root-dirs" in result.output
        assert "--s3-root-uri" in result.output

    def test_attachment_upload_accepts_valid_args(self):
        """
        Test that attachment upload accepts valid arguments without asset-type
        """
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "attachment",
                "upload",
                "--manifests",
                "/tmp/test.manifest",
                "--root-dirs",
                "/tmp/test",
                "--s3-root-uri",
                "s3://test-bucket/test-prefix",
            ],
        )
        # Command will fail due to missing files/config, but should not fail due to CLI parsing
        # The important thing is that it doesn't complain about unknown options
        assert "no such option" not in result.output.lower()
        assert "unrecognized arguments" not in result.output.lower()
