# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI attachment commands.
"""

from click.testing import CliRunner

from deadline.client.cli import main


class TestAttachmentUpload:
    """Test cases for the attachment upload command"""

    def test_asset_type_invalid_choices(self):
        """
        Test that invalid choices for --asset-type are rejected
        """
        runner = CliRunner()

        # Test invalid choices
        invalid_choices = ["all", ""]
        for choice in invalid_choices:
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
                    "--asset-type",
                    choice,
                ],
            )
            # Should fail with invalid value error
            assert result.exit_code != 0, f"Invalid choice '{choice}' was accepted"
            assert "Invalid value" in result.output or "Usage:" in result.output, (
                f"Invalid choice '{choice}' did not show proper error"
            )

    def test_asset_type_case_insensitive(self):
        """
        Test that asset-type choices are case insensitive
        """
        runner = CliRunner()

        # Test case variations
        case_variations = ["INPUT", "Output", "OTHER", "iNpUt", "oUtPuT", "oThEr"]
        for choice in case_variations:
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
                    "--asset-type",
                    choice,
                ],
                catch_exceptions=False,
            )
            # The command will fail due to missing files/config, but it should not fail due to invalid asset-type
            assert "Invalid value" not in result.output, f"Case variation '{choice}' was rejected"
