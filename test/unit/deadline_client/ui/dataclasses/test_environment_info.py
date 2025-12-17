# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from deadline.client.ui.dataclasses._environment_info import _EnvironmentInfo


@pytest.mark.parametrize(
    "version_string,expected",
    [
        pytest.param(
            "0.53.3.post24+g9eb659973.d20251211",
            "December 11, 2025",
            id="full_version_with_date",
        ),
        pytest.param(
            "1.0.0.d20240115",
            "January 15, 2024",
            id="version_with_date_no_post",
        ),
        pytest.param(
            "0.53.3",
            "Unknown",
            id="release_version_no_date",
        ),
        pytest.param(
            "invalid",
            "Unknown",
            id="invalid_version_string",
        ),
        pytest.param(
            "",
            "Unknown",
            id="empty_string",
        ),
    ],
)
def test_extract_release_date(version_string, expected):
    """Test that release dates are correctly extracted from version strings."""
    result = _EnvironmentInfo._extract_release_date(version_string)
    assert result == expected
