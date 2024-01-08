# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.


from deadline.job_attachments.progress_tracker import (
    SummaryStatistics,
    DownloadSummaryStatistics,
    ProgressTracker,
    ProgressStatus,
)
import pytest
import concurrent


# += operator doesn't seem to be non-threadsafe in python 3.10 or later, but can be an issue in earlier versions.
class TestProgressTracker:
    """
    Tests for ProgressTracker class
    """

    def test_increment_race_condition(self):
        progress_tracker = ProgressTracker(ProgressStatus.NONE, 0, 0)

        N = 10**5
        K = 10

        def increment():
            for _ in range(N):
                progress_tracker.increase_processed(1, 0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for _ in range(K):
                executor.submit(increment)

        assert progress_tracker.processed_files == N * K


class TestSummaryStatistics:
    """
    Tests for SummaryStatistics class
    """

    def test_aggregate_with_no_stats(self):
        summary1 = SummaryStatistics()
        summary2 = SummaryStatistics()

        expected_aggregated_stats = SummaryStatistics()

        aggregated = summary1.aggregate(summary2)
        assert aggregated == expected_aggregated_stats

    def test_aggregate(self):
        summary1 = SummaryStatistics(
            total_time=10.0,
            total_files=10,
            total_bytes=1000,
            processed_files=7,
            processed_bytes=700,
            skipped_files=3,
            skipped_bytes=300,
            transfer_rate=70.0,
        )
        summary2 = SummaryStatistics(
            total_time=10.0,
            total_files=10,
            total_bytes=1000,
            processed_files=8,
            processed_bytes=800,
            skipped_files=2,
            skipped_bytes=200,
            transfer_rate=80.0,
        )

        expected_aggregated_stats = SummaryStatistics(
            total_time=20.0,
            total_files=20,
            total_bytes=2000,
            processed_files=15,
            processed_bytes=1500,
            skipped_files=5,
            skipped_bytes=500,
            transfer_rate=75.0,
        )

        aggregated = summary1.aggregate(summary2)
        assert aggregated == expected_aggregated_stats

    def test_aggregate_summary_stats_and_download_summary_stats(self):
        summary1 = SummaryStatistics(
            total_time=10.0,
            total_files=10,
            total_bytes=1000,
            processed_files=7,
            processed_bytes=700,
            skipped_files=3,
            skipped_bytes=300,
            transfer_rate=70.0,
        )
        summary2 = DownloadSummaryStatistics(
            total_time=10.0,
            total_files=10,
            total_bytes=1000,
            processed_files=8,
            processed_bytes=800,
            skipped_files=2,
            skipped_bytes=200,
            transfer_rate=80.0,
            file_counts_by_root_directory={
                "/home/username/outputs1": 1,
                "/home/username/outputs2": 2,
                "/home/username/outputs3": 5,
            },
        )

        expected_aggregated_stats = SummaryStatistics(
            total_time=20.0,
            total_files=20,
            total_bytes=2000,
            processed_files=15,
            processed_bytes=1500,
            skipped_files=5,
            skipped_bytes=500,
            transfer_rate=75.0,
        )

        aggregated = summary1.aggregate(summary2)
        assert aggregated == expected_aggregated_stats


class TestDownloadSummaryStatistics:
    """
    Tests for DownloadSummaryStatistics class
    """

    def test_aggregate_with_no_stats(self):
        summary1 = DownloadSummaryStatistics()
        summary2 = DownloadSummaryStatistics()

        expected_aggregated_stats = DownloadSummaryStatistics()

        aggregated = summary1.aggregate(summary2)
        assert aggregated == expected_aggregated_stats

    def test_aggregate(self):
        summary1 = DownloadSummaryStatistics(
            total_time=10.0,
            total_files=10,
            total_bytes=1000,
            processed_files=7,
            processed_bytes=700,
            skipped_files=3,
            skipped_bytes=300,
            transfer_rate=70.0,
            file_counts_by_root_directory={
                "/home/username/outputs1": 1,
                "/home/username/outputs2": 2,
                "/home/username/outputs3": 4,
            },
        )
        summary2 = DownloadSummaryStatistics(
            total_time=10.0,
            total_files=10,
            total_bytes=1000,
            processed_files=8,
            processed_bytes=800,
            skipped_files=2,
            skipped_bytes=200,
            transfer_rate=80.0,
            file_counts_by_root_directory={
                "/home/username/outputs3": 5,
                "/home/username/outputs4": 3,
            },
        )

        expected_aggregated_stats = DownloadSummaryStatistics(
            total_time=20.0,
            total_files=20,
            total_bytes=2000,
            processed_files=15,
            processed_bytes=1500,
            skipped_files=5,
            skipped_bytes=500,
            transfer_rate=75.0,
            file_counts_by_root_directory={
                "/home/username/outputs1": 1,
                "/home/username/outputs2": 2,
                "/home/username/outputs3": 9,
                "/home/username/outputs4": 3,
            },
        )

        aggregated = summary1.aggregate(summary2)
        assert aggregated == expected_aggregated_stats

    def test_aggregate_with_summary_stats(self):
        """
        Tests if it raises exception when DownloadSummaryStatistics calls aggreate function
        with SummaryStatistics object.
        """
        summary1 = DownloadSummaryStatistics()
        summary2 = SummaryStatistics()

        with pytest.raises(TypeError):
            summary1.aggregate(summary2)
