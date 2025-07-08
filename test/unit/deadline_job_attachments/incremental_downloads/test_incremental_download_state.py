# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import json
import pytest
import tempfile
from unittest.mock import MagicMock

from deadline.job_attachments._incremental_downloads.incremental_download_state import (
    IncrementalDownloadJob,
    IncrementalDownloadState,
    EVENTUAL_CONSISTENCY_MAX_SECONDS,
)
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
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def test_paths(self, temp_dir):
        """
        Fixture to provide test file paths using a real temporary directory.
        """
        return {
            "location": temp_dir,
            "progress_file": os.path.join(temp_dir, "download_checkpoint.json"),
        }

    @pytest.fixture
    def sample_state_data(self):
        """
        Fixture to provide sample state data.
        """
        return {
            "downloadsStartedTimestamp": "2023-01-01T00:00:00+00:00",
            "downloadsCompletedTimestamp": "2023-01-02T00:00:00+00:00",
            "eventualConsistencyMaxSeconds": EVENTUAL_CONSISTENCY_MAX_SECONDS,
            "jobs": [{"jobId": "job-123", "name": "Job 1"}, {"jobId": "job-124", "name": "Job 2"}],
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
    def mock_download_state(self):
        """
        Fixture to create a sample IncrementalDownloadState. This state matches the sample_state_data fixture.
        """
        return IncrementalDownloadState(
            downloads_started_timestamp=datetime.fromisoformat("2023-01-01T00:00:00+00:00"),
            downloads_completed_timestamp=datetime.fromisoformat("2023-01-02T00:00:00+00:00"),
            jobs=[
                IncrementalDownloadJob({"jobId": "job-123", "name": "Job 1"}, None, {}),
                IncrementalDownloadJob(
                    {"jobId": "job-124", "name": "Job 2"},
                    datetime.fromisoformat("2023-01-02T00:00:00+00:00"),
                    {},
                ),
            ],
        )

    def test_incremental_download_state_init(self):
        """
        Test IncrementalDownloadState initialization.
        """
        bootstrap_time = datetime.fromisoformat("2023-01-01T00:00:00")
        completed_time = datetime.fromisoformat("2023-01-02T00:00:00")

        # Test with minimal bootstrapped construction
        state = IncrementalDownloadState(bootstrap_time)
        assert state.downloads_started_timestamp == bootstrap_time
        assert state.downloads_completed_timestamp == bootstrap_time
        assert state.eventual_consistency_max_seconds == 120
        assert state.jobs == []

        # Test with provided values
        jobs = [IncrementalDownloadJob({"jobId": "job-123"}, None, {})]
        state = IncrementalDownloadState(
            downloads_started_timestamp=bootstrap_time,
            downloads_completed_timestamp=completed_time,
            jobs=jobs,
        )
        assert state.downloads_started_timestamp == bootstrap_time
        assert state.downloads_completed_timestamp == completed_time
        assert state.jobs == jobs

    def test_incremental_download_state_dict_roundtrip(self, mock_download_state):
        """
        Test IncrementalDownloadState.from_dict and to_dict methods, by roundtripping.
        """

        dict_state = mock_download_state.to_dict()

        assert dict_state == IncrementalDownloadState.from_dict(dict_state).to_dict()

    def test_incremental_download_state_file_roundtrip(
        self, temp_dir, mock_download_state: IncrementalDownloadState
    ):
        """
        Test IncrementalDownloadState.from_file and save_file methods, by roundtripping.
        """

        dict_state = mock_download_state.to_dict()

        file_path = os.path.join(temp_dir, "checkpoint.json")
        mock_download_state.save_file(file_path)
        roundtrip_state = IncrementalDownloadState.from_file(file_path)

        assert roundtrip_state.to_dict() == dict_state
