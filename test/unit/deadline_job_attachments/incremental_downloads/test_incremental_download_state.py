# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import json
import pytest
import tempfile
import shutil
from unittest.mock import MagicMock

from deadline.job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
)
from freezegun import freeze_time
from datetime import datetime


class TestIncrementalDownloadState:
    @pytest.fixture
    def mock_logger(self):
        """
        Fixture to create a mock logger object.
        """
        logger = MagicMock()
        logger.echo = MagicMock()
        return logger

    @pytest.fixture
    def temp_dir(self):
        """
        Fixture to provide a temporary directory that is cleaned up after tests.
        """
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        # Clean up after tests
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def test_paths(self, temp_dir):
        """
        Fixture to provide test file paths using a real temporary directory.
        """
        return {
            "location": temp_dir,
            "progress_file": os.path.join(temp_dir, "download_progress.json"),
        }

    @pytest.fixture
    def sample_state_data(self):
        """
        Fixture to provide sample state data.
        """
        return {
            "lastLookbackTime": "2023-01-01T00:00:00Z",
            "jobs": [
                {
                    "jobId": "job-123",
                    "sessions": [
                        {
                            "sessionId": "session-123",
                            "sessionLifecycleStatus": "RUNNING",
                            "lastDownloadedSessActionId": 5,
                        }
                    ],
                }
            ],
        }

    @pytest.fixture
    def state_file(self, test_paths, sample_state_data):
        """
        Fixture to create a real state file with sample data.
        """
        with open(test_paths["progress_file"], "w") as f:
            json.dump(sample_state_data, f, indent=2)
        yield test_paths["progress_file"]
        # Cleanup is handled by the temp_dir fixture

    @pytest.fixture
    def mock_state(self):
        """
        Fixture to create a sample IncrementalDownloadState.
        """
        model = IncrementalDownloadState()
        model.last_lookback_time = datetime.fromisoformat("2023-01-01T00:00:00")
        model.jobs = [
            {
                "jobId": "job-123",
                "sessions": [
                    {
                        "sessionId": "session-123",
                        "sessionLifecycleStatus": "RUNNING",
                        "lastDownloadedSessActionId": 5,
                    }
                ],
            }
        ]
        return model

    @freeze_time("2025-05-26 12:00:00+00:00")
    def test_bootstrap_fresh_state(self, mock_logger):
        """
        Test bootstrap_fresh_state with lookback minutes.
        """
        # Setup
        bootstrap_lookback_in_minutes = 60

        # Execute
        result = IncrementalDownloadState.from_bootstrap(
            bootstrap_lookback_in_minutes,
            mock_logger.echo,
        )

        # Assert
        assert result.last_lookback_time.isoformat() == "2025-05-26T11:00:00+00:00"
        assert result.jobs == []

    @freeze_time("2025-05-26 12:00:00+00:00")
    def test_bootstrap_fresh_state_no_lookback(self, mock_logger):
        """
        Test bootstrap_fresh_state without lookback minutes.
        """
        # Execute
        result = IncrementalDownloadState.from_bootstrap(
            None,
            mock_logger.echo,
        )

        # Assert
        assert result.last_lookback_time.isoformat() == "2025-05-26T12:00:00+00:00"
        assert result.jobs == []

    def test_load_progress_from_state_file(self, mock_logger, state_file, sample_state_data):
        """
        Test load_progress_from_state_file successfully loads the state file.
        Uses a real file instead of mocking.
        """
        # Execute
        result = IncrementalDownloadState.from_file(
            state_file,
            mock_logger.echo,
        )

        # Assert
        assert result.last_lookback_time == sample_state_data["lastLookbackTime"]
        assert result.jobs == sample_state_data["jobs"]

    def test_load_progress_from_state_file_exception(self, mock_logger, test_paths):
        """
        Test load_progress_from_state_file when the file doesn't exist.
        """
        # Use a non-existent file path
        non_existent_file = os.path.join(test_paths["location"], "non_existent.json")

        # Execute and Assert
        with pytest.raises(Exception):
            IncrementalDownloadState.from_file(
                non_existent_file,
                mock_logger.echo,
            )

    def test_save_progress_to_state_file(self, mock_logger, test_paths, mock_state):
        """
        Test save_progress_to_state_file successfully saves the state file.
        Uses real file operations instead of mocking.
        """
        # Execute
        mock_state.save_file(
            test_paths["progress_file"],
            mock_logger.echo,
        )

        # Assert
        assert os.path.exists(test_paths["progress_file"])

        # Verify file contents
        with open(test_paths["progress_file"], "r") as f:
            saved_data = json.load(f)

        assert saved_data["lastLookbackTime"] == mock_state.last_lookback_time.isoformat()
        assert saved_data["jobs"] == mock_state.jobs

    def test_incremental_download_state_init(self):
        """
        Test IncrementalDownloadState initialization.
        """
        # Test with default values
        state = IncrementalDownloadState()
        assert state.last_lookback_time is None
        assert state.jobs == []

        # Test with provided values
        last_lookback_time = datetime.fromisoformat("2023-01-01T00:00:00")
        jobs = [{"jobId": "job-123", "sessions": []}]
        state = IncrementalDownloadState(last_lookback_time=last_lookback_time, jobs=jobs)
        assert state.last_lookback_time == last_lookback_time
        assert state.jobs == jobs

    def test_incremental_download_state_from_dict(self):
        """
        Test IncrementalDownloadState.from_dict method.
        """
        # Test with empty dict
        state = IncrementalDownloadState.from_dict({})
        assert state.last_lookback_time is None
        assert state.jobs == []

        # Test with None
        state = IncrementalDownloadState.from_dict(None)
        assert state.last_lookback_time is None
        assert state.jobs == []

        # Test with valid data
        data = {
            "lastLookbackTime": "2023-01-01T00:00:00Z",
            "jobs": [
                {
                    "jobId": "job-123",
                    "sessions": [
                        {
                            "sessionId": "session-123",
                            "sessionLifecycleStatus": "RUNNING",
                            "lastDownloadedSessActionId": 5,
                        }
                    ],
                }
            ],
        }
        state = IncrementalDownloadState.from_dict(data)
        assert state.last_lookback_time == "2023-01-01T00:00:00Z"
        assert len(state.jobs) == 1
        assert state.jobs[0]["jobId"] == "job-123"

    def test_incremental_download_state_to_dict(self):
        """
        Test IncrementalDownloadState.to_dict method.
        """
        # Create a state
        last_lookback_time = datetime.fromisoformat("2023-01-01T00:00:00")
        jobs = [
            {
                "jobId": "job-123",
                "sessions": [
                    {
                        "sessionId": "session-123",
                        "sessionLifecycleStatus": "RUNNING",
                        "lastDownloadedSessActionId": 5,
                    }
                ],
            }
        ]
        state = IncrementalDownloadState(last_lookback_time=last_lookback_time, jobs=jobs)

        # Convert to dict
        result = state.to_dict()

        # Assert
        assert result == {"lastLookbackTime": last_lookback_time.isoformat(), "jobs": jobs}
