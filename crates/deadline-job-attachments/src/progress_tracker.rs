use std::fmt;
use std::sync::Mutex;
use std::time::Instant;

/// Convert a byte count to a human-readable string (e.g., "1.5 GB").
///
/// Uses SI prefixes (1 KB = 1000 bytes).
pub fn human_readable_file_size(size_in_bytes: u64) -> String {
    let postfixes = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut converted: f64 = size_in_bytes as f64;
    let mut rounded: f64;

    for postfix in &postfixes {
        rounded = (converted * 100.0).round() / 100.0;
        if rounded < 1000.0 {
            if *postfix == "B" {
                return format!("{} {postfix}", rounded as u64);
            } else {
                let s = format!("{rounded:.2}");
                let s = s.trim_end_matches('0').trim_end_matches('.');
                return format!("{s} {postfix}");
            }
        }
        converted /= 1000.0;
    }

    let rounded = (converted * 100.0).round() / 100.0;
    let s = format!("{rounded:.2}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    format!("{s} {}", postfixes.last().unwrap())
}

// --- ProgressStatus ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressStatus {
    None,
    PreparingInProgress,
    UploadInProgress,
    DownloadInProgress,
    SnapshotInProgress,
}

impl ProgressStatus {
    pub fn title(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::PreparingInProgress => "PREPARING_IN_PROGRESS",
            Self::UploadInProgress => "UPLOAD_IN_PROGRESS",
            Self::DownloadInProgress => "DOWNLOAD_IN_PROGRESS",
            Self::SnapshotInProgress => "SNAPSHOT_IN_PROGRESS",
        }
    }

    pub fn verb_in_message(&self) -> &'static str {
        match self {
            Self::None => "",
            Self::PreparingInProgress => "Processed",
            Self::UploadInProgress => "Uploaded",
            Self::DownloadInProgress => "Downloaded",
            Self::SnapshotInProgress => "Snapshotted",
        }
    }
}

// --- ProgressReportMetadata ---

#[derive(Debug)]
pub struct ProgressReportMetadata {
    pub status: ProgressStatus,
    pub progress: f64,
    pub transfer_rate: f64,
    pub progress_message: String,
    pub processed_files: u64,
}

// --- SummaryStatistics ---

#[derive(Debug, Clone, serde::Serialize)]
pub struct SummaryStatistics {
    pub total_time: f64,
    pub total_files: u64,
    pub total_bytes: u64,
    pub processed_files: u64,
    pub processed_bytes: u64,
    pub skipped_files: u64,
    pub skipped_bytes: u64,
    pub transfer_rate: f64,
}

impl Default for SummaryStatistics {
    fn default() -> Self {
        Self {
            total_time: 0.0,
            total_files: 0,
            total_bytes: 0,
            processed_files: 0,
            processed_bytes: 0,
            skipped_files: 0,
            skipped_bytes: 0,
            transfer_rate: 0.0,
        }
    }
}

impl SummaryStatistics {
    pub fn aggregate(&mut self, other: &SummaryStatistics) {
        self.total_time += other.total_time;
        self.total_files += other.total_files;
        self.total_bytes += other.total_bytes;
        self.processed_files += other.processed_files;
        self.processed_bytes += other.processed_bytes;
        self.skipped_files += other.skipped_files;
        self.skipped_bytes += other.skipped_bytes;
        self.transfer_rate = if self.total_time > 0.0 {
            self.processed_bytes as f64 / self.total_time
        } else {
            0.0
        };
    }
}

impl fmt::Display for SummaryStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let file_word = if self.processed_files == 1 {
            "file"
        } else {
            "files"
        };
        write!(
            f,
            "Processed {} {} totaling {}.\n\
             Skipped re-processing {} files totaling {}.\n\
             Total processing time of {} seconds at {}/s.\n",
            self.processed_files,
            file_word,
            human_readable_file_size(self.processed_bytes),
            self.skipped_files,
            human_readable_file_size(self.skipped_bytes),
            round_to_5(self.total_time),
            human_readable_file_size(self.transfer_rate as u64),
        )
    }
}

fn round_to_5(v: f64) -> f64 {
    (v * 100000.0).round() / 100000.0
}

// --- DownloadSummaryStatistics ---

#[derive(Debug, Clone)]
pub struct DownloadSummaryStatistics {
    pub stats: SummaryStatistics,
    pub file_counts_by_root_directory: std::collections::BTreeMap<String, usize>,
    pub downloaded_files: Vec<String>,
}

impl DownloadSummaryStatistics {
    pub fn aggregate(&mut self, other: &DownloadSummaryStatistics) {
        self.stats.aggregate(&other.stats);
        for (k, v) in &other.file_counts_by_root_directory {
            *self.file_counts_by_root_directory.entry(k.clone()).or_insert(0) += v;
        }
    }
}

// --- ProgressTracker ---

const CALLBACK_INTERVAL_SECS: f64 = 1.0;
const MAX_FILES_IN_CHUNK: u64 = 50;

struct TrackerInner {
    continue_reporting: bool,
    processed_files: u64,
    processed_bytes: u64,
    skipped_files: u64,
    skipped_bytes: u64,
    completed_files_in_chunk: u64,
    last_report_time: Option<Instant>,
    last_report_processed_bytes: u64,
}

pub struct ProgressTracker {
    status: ProgressStatus,
    total_files: u64,
    total_bytes: u64,
    reporting_files_per_chunk: u64,
    callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    inner: Mutex<TrackerInner>,
    total_time: Mutex<f64>,
}

impl ProgressTracker {
    pub fn new(
        status: ProgressStatus,
        total_files: u64,
        total_bytes: u64,
        callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    ) -> Self {
        let reporting_files_per_chunk = if total_files >= MAX_FILES_IN_CHUNK {
            MAX_FILES_IN_CHUNK
        } else {
            1
        };
        Self {
            status,
            total_files,
            total_bytes,
            reporting_files_per_chunk,
            callback,
            inner: Mutex::new(TrackerInner {
                continue_reporting: true,
                processed_files: 0,
                processed_bytes: 0,
                skipped_files: 0,
                skipped_bytes: 0,
                completed_files_in_chunk: 0,
                last_report_time: None,
                last_report_processed_bytes: 0,
            }),
            total_time: Mutex::new(0.0),
        }
    }

    pub fn set_total_time(&self, t: f64) {
        *self.total_time.lock().unwrap() = t;
    }

    pub fn continue_reporting(&self) -> bool {
        self.inner.lock().unwrap().continue_reporting
    }

    pub fn increase_processed(&self, num_files: u64, file_bytes: u64) {
        let mut inner = self.inner.lock().unwrap();
        Self::init_timestamp(&mut inner);
        inner.processed_files += num_files;
        inner.processed_bytes += file_bytes;
        inner.completed_files_in_chunk += num_files;
    }

    pub fn increase_skipped(&self, num_files: u64, file_bytes: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.skipped_files += num_files;
        inner.skipped_bytes += file_bytes;
        inner.completed_files_in_chunk += num_files;
    }

    pub fn report_progress(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        self.report_progress_inner(&mut inner)
    }

    pub fn track_progress(&self, bytes_amount: u64, current_file_done: bool) -> bool {
        let mut inner = self.inner.lock().unwrap();
        Self::init_timestamp(&mut inner);
        inner.processed_bytes += bytes_amount;
        if current_file_done {
            inner.processed_files += 1;
            inner.completed_files_in_chunk += 1;
        }
        self.report_progress_inner(&mut inner)
    }

    /// Returns the current count of processed files.
    pub fn processed_files(&self) -> u64 {
        self.inner.lock().unwrap().processed_files
    }

    pub fn get_summary_statistics(&self) -> SummaryStatistics {
        let inner = self.inner.lock().unwrap();
        let total_time = *self.total_time.lock().unwrap();
        let transfer_rate = if total_time > 0.0 {
            inner.processed_bytes as f64 / total_time
        } else {
            0.0
        };
        SummaryStatistics {
            total_time,
            total_files: self.total_files,
            total_bytes: self.total_bytes,
            processed_files: inner.processed_files,
            processed_bytes: inner.processed_bytes,
            skipped_files: inner.skipped_files,
            skipped_bytes: inner.skipped_bytes,
            transfer_rate,
        }
    }

    fn init_timestamp(inner: &mut TrackerInner) {
        if inner.last_report_time.is_none() {
            inner.last_report_time = Some(Instant::now());
        }
    }

    fn report_progress_inner(&self, inner: &mut TrackerInner) -> bool {
        if !inner.continue_reporting {
            return false;
        }

        let now = Instant::now();
        let elapsed = inner
            .last_report_time
            .map_or(0.0, |t| now.duration_since(t).as_secs_f64());

        let all_done =
            inner.processed_files + inner.skipped_files == self.total_files;
        let time_trigger = inner.last_report_time.is_none()
            || elapsed >= CALLBACK_INTERVAL_SECS;
        let chunk_trigger =
            inner.completed_files_in_chunk >= self.reporting_files_per_chunk;

        if time_trigger || chunk_trigger || all_done {
            let metadata = self.build_metadata(inner, elapsed);
            let should_continue = match &self.callback {
                Some(cb) => cb(metadata),
                None => true,
            };
            inner.continue_reporting = should_continue;
            inner.last_report_processed_bytes = inner.processed_bytes;
            inner.last_report_time = Some(now);
            inner.completed_files_in_chunk = 0;
        }

        inner.continue_reporting
    }

    fn build_metadata(&self, inner: &TrackerInner, elapsed: f64) -> ProgressReportMetadata {
        let completed_bytes = inner.processed_bytes + inner.skipped_bytes;
        let progress = if self.total_bytes > 0 {
            ((completed_bytes as f64 / self.total_bytes as f64) * 1000.0).round() / 10.0
        } else {
            0.0
        };
        let transfer_rate = if elapsed > 0.0 {
            (inner.processed_bytes - inner.last_report_processed_bytes) as f64 / elapsed
        } else {
            0.0
        };
        let rate_label = if self.status == ProgressStatus::PreparingInProgress {
            "Hashing speed"
        } else {
            "Transfer rate"
        };
        let file_word = if self.total_files == 1 {
            "file"
        } else {
            "files"
        };
        let progress_message = format!(
            "{} {} / {} of {} {} ({}: {}/s)",
            self.status.verb_in_message(),
            human_readable_file_size(completed_bytes),
            human_readable_file_size(self.total_bytes),
            self.total_files,
            file_word,
            rate_label,
            human_readable_file_size(transfer_rate as u64),
        );
        ProgressReportMetadata {
            status: self.status,
            progress,
            transfer_rate,
            progress_message,
            processed_files: inner.processed_files,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}};
    use std::thread;
    use std::time::Duration;

    // === Create with callback ===

    #[test]
    fn progress_tracker_new_with_callback_stores_it() {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called;
        let callback = move |_: ProgressReportMetadata| -> bool {
            called_clone.store(true, Ordering::SeqCst);
            true
        };
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            10,
            1000,
            Some(Box::new(callback)),
        );
        assert!(tracker.continue_reporting());
    }

    // === Create without callback ===

    #[test]
    fn progress_tracker_new_without_callback_defaults_to_noop() {
        let tracker = ProgressTracker::new(
            ProgressStatus::UploadInProgress,
            5,
            500,
            None,
        );
        // Should not panic, report_progress returns true (no cancellation)
        assert!(tracker.report_progress());
    }

    // === increase_processed increments counters ===

    #[test]
    fn increase_processed_increments_files_and_bytes() {
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            10,
            10000,
            None,
        );
        tracker.increase_processed(1, 1000);
        let stats = tracker.get_summary_statistics();
        assert_eq!(stats.processed_files, 1);
        assert_eq!(stats.processed_bytes, 1000);
    }

    // === increase_skipped increments counters ===

    #[test]
    fn increase_skipped_increments_files_and_bytes() {
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            10,
            10000,
            None,
        );
        tracker.increase_skipped(1, 500);
        let stats = tracker.get_summary_statistics();
        assert_eq!(stats.skipped_files, 1);
        assert_eq!(stats.skipped_bytes, 500);
    }

    // === report_progress fires callback when time elapsed ===

    #[test]
    fn report_progress_fires_callback_after_time_interval() {
        let call_count = Arc::new(AtomicU32::new(0));
        let count_clone = call_count.clone();
        let callback = move |_: ProgressReportMetadata| -> bool {
            count_clone.fetch_add(1, Ordering::SeqCst);
            true
        };
        let tracker = ProgressTracker::new(
            ProgressStatus::UploadInProgress,
            100,
            10000,
            Some(Box::new(callback)),
        );
        // First call initializes timestamps; sleep past the 1s interval
        tracker.increase_processed(1, 100);
        thread::sleep(Duration::from_millis(1100));
        tracker.increase_processed(1, 100);
        tracker.report_progress();
        assert!(call_count.load(Ordering::SeqCst) >= 1);
    }

    // === report_progress fires callback when chunk complete ===

    #[test]
    fn report_progress_fires_callback_on_chunk_complete() {
        let call_count = Arc::new(AtomicU32::new(0));
        let count_clone = call_count.clone();
        let callback = move |_: ProgressReportMetadata| -> bool {
            count_clone.fetch_add(1, Ordering::SeqCst);
            true
        };
        // total_files >= 50, so reporting_files_per_chunk = 50
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            100,
            10000,
            Some(Box::new(callback)),
        );
        // Process 50 files to complete a chunk
        for _ in 0..50 {
            tracker.increase_processed(1, 100);
        }
        tracker.report_progress();
        assert!(call_count.load(Ordering::SeqCst) >= 1);
    }

    // === report_progress fires callback at 100% ===

    #[test]
    fn report_progress_fires_callback_at_100_percent() {
        let call_count = Arc::new(AtomicU32::new(0));
        let count_clone = call_count.clone();
        let callback = move |_: ProgressReportMetadata| -> bool {
            count_clone.fetch_add(1, Ordering::SeqCst);
            true
        };
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            2,
            200,
            Some(Box::new(callback)),
        );
        tracker.increase_processed(2, 200);
        tracker.report_progress();
        assert!(call_count.load(Ordering::SeqCst) >= 1);
    }

    // === callback returns false sets continue_reporting to false ===

    #[test]
    fn report_progress_callback_returns_false_cancels() {
        let callback = |_: ProgressReportMetadata| -> bool { false };
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            2,
            200,
            Some(Box::new(callback)),
        );
        // Process all files to trigger 100% report
        tracker.increase_processed(2, 200);
        let result = tracker.report_progress();
        assert!(!result);
        assert!(!tracker.continue_reporting());
    }

    // === continue_reporting already false returns false without callback ===

    #[test]
    fn report_progress_already_cancelled_returns_false_without_callback() {
        let call_count = Arc::new(AtomicU32::new(0));
        let count_clone = call_count.clone();
        let callback = move |_: ProgressReportMetadata| -> bool {
            count_clone.fetch_add(1, Ordering::SeqCst);
            false
        };
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            2,
            200,
            Some(Box::new(callback)),
        );
        // First: trigger cancellation
        tracker.increase_processed(2, 200);
        tracker.report_progress();
        let count_after_cancel = call_count.load(Ordering::SeqCst);

        // Second: should return false without invoking callback again
        let result = tracker.report_progress();
        assert!(!result);
        assert_eq!(call_count.load(Ordering::SeqCst), count_after_cancel);
    }

    // === get_summary_statistics returns correct values ===

    #[test]
    fn get_summary_statistics_returns_correct_totals() {
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            10,
            5000,
            None,
        );
        tracker.increase_processed(3, 2000);
        tracker.increase_skipped(2, 1000);
        tracker.set_total_time(2.0);

        let stats = tracker.get_summary_statistics();
        assert_eq!(stats.total_files, 10);
        assert_eq!(stats.total_bytes, 5000);
        assert_eq!(stats.processed_files, 3);
        assert_eq!(stats.processed_bytes, 2000);
        assert_eq!(stats.skipped_files, 2);
        assert_eq!(stats.skipped_bytes, 1000);
        assert!((stats.transfer_rate - 1000.0).abs() < f64::EPSILON);
    }

    // === total_time is 0 means transfer_rate is 0 ===

    #[test]
    fn get_summary_statistics_zero_time_zero_rate() {
        let tracker = ProgressTracker::new(
            ProgressStatus::PreparingInProgress,
            10,
            5000,
            None,
        );
        tracker.increase_processed(3, 2000);
        // total_time defaults to 0.0
        let stats = tracker.get_summary_statistics();
        assert!((stats.transfer_rate - 0.0).abs() < f64::EPSILON);
    }

    // === SummaryStatistics aggregate sums fields ===

    #[test]
    fn summary_statistics_aggregate_sums_all_fields() {
        let mut s1 = SummaryStatistics {
            total_time: 1.0,
            total_files: 5,
            total_bytes: 1000,
            processed_files: 3,
            processed_bytes: 600,
            skipped_files: 2,
            skipped_bytes: 400,
            transfer_rate: 600.0,
        };
        let s2 = SummaryStatistics {
            total_time: 2.0,
            total_files: 10,
            total_bytes: 2000,
            processed_files: 7,
            processed_bytes: 1400,
            skipped_files: 3,
            skipped_bytes: 600,
            transfer_rate: 700.0,
        };
        s1.aggregate(&s2);
        assert_eq!(s1.total_time, 3.0);
        assert_eq!(s1.total_files, 15);
        assert_eq!(s1.total_bytes, 3000);
        assert_eq!(s1.processed_files, 10);
        assert_eq!(s1.processed_bytes, 2000);
        assert_eq!(s1.skipped_files, 5);
        assert_eq!(s1.skipped_bytes, 1000);
        // transfer_rate recalculated: 2000 / 3.0
        assert!((s1.transfer_rate - 2000.0 / 3.0).abs() < 0.01);
    }

    // === SummaryStatistics Display ===

    #[test]
    fn summary_statistics_display_multiple_files() {
        let stats = SummaryStatistics {
            total_time: 1.23456,
            total_files: 10,
            total_bytes: 5000,
            processed_files: 7,
            processed_bytes: 3500,
            skipped_files: 3,
            skipped_bytes: 1500,
            transfer_rate: 2845.52,
        };
        let output = stats.to_string();
        assert!(output.contains("Processed 7 files totaling"));
        assert!(output.contains("Skipped re-processing 3 files totaling"));
        assert!(output.contains("Total processing time of 1.23456 seconds"));
    }

    // === SummaryStatistics Display singular file ===

    #[test]
    fn summary_statistics_display_singular_file() {
        let stats = SummaryStatistics {
            total_time: 0.5,
            total_files: 1,
            total_bytes: 100,
            processed_files: 1,
            processed_bytes: 100,
            skipped_files: 0,
            skipped_bytes: 0,
            transfer_rate: 200.0,
        };
        let output = stats.to_string();
        assert!(output.contains("Processed 1 file totaling"), "expected singular 'file', got: {output}");
        assert!(!output.contains("1 files"), "should not have plural 'files' for count 1");
    }

    // === track_progress with file_done=true increments both ===

    #[test]
    fn track_progress_file_done_increments_files_and_bytes() {
        let tracker = ProgressTracker::new(
            ProgressStatus::UploadInProgress,
            10,
            10000,
            None,
        );
        tracker.track_progress(500, true);
        let stats = tracker.get_summary_statistics();
        assert_eq!(stats.processed_bytes, 500);
        assert_eq!(stats.processed_files, 1);
    }

    // === track_progress with file_done=false only increments bytes ===

    #[test]
    fn track_progress_not_file_done_only_increments_bytes() {
        let tracker = ProgressTracker::new(
            ProgressStatus::UploadInProgress,
            10,
            10000,
            None,
        );
        tracker.track_progress(500, false);
        let stats = tracker.get_summary_statistics();
        assert_eq!(stats.processed_bytes, 500);
        assert_eq!(stats.processed_files, 0);
    }
}
