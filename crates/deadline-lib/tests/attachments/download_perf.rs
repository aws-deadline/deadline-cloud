//! Level 1 performance test for download filter regex caching (Batch C).
//!
//! Verifies that filter_manifests with many files completes within a
//! reasonable time bound. The uncached implementation (compiling a regex
//! per filter per file) takes ~380ms for 10K files × 5 filters. After
//! caching, it drops to <1ms. The threshold is generous (50ms) to avoid
//! flakiness while still catching the O(N×M) regex compilation cost.

use std::collections::HashMap;
use std::time::Instant;

use deadline_lib::attachments::download::{filter_manifests, matches_any_filter};
use openjd_snapshots::{FileEntry, HashAlgorithm, Snapshot, WHOLE_FILE_CHUNK_SIZE};

fn make_large_manifest(file_count: usize) -> Snapshot {
    let files: Vec<FileEntry> = (0..file_count)
        .map(|i| {
            let path = format!("shots/shot_{:04}/renders/frame_{:04}.exr", i / 100, i % 100);
            let mut e = FileEntry::file(&path, 1024, 1_700_000_000_000_000);
            e.hash = Some(format!("{i:032x}"));
            e
        })
        .collect();
    let total_size = files.len() as u64 * 1024;
    let mut snap = Snapshot::new(HashAlgorithm::Xxh128, WHOLE_FILE_CHUNK_SIZE);
    snap.files = files;
    snap.total_size = total_size;
    snap
}

/// Regression test: filter_manifests with 5000 files × 5 filters must
/// complete in under 50ms. Before caching, this takes ~190ms due to
/// regex compilation per call. After caching, it takes <1ms.
#[test]
fn filter_manifests_5000_files_completes_within_budget() {
    let manifest = make_large_manifest(5_000);
    let filters: Vec<String> = vec![
        "*.exr".into(),
        "*/renders/*".into(),
        "*.png".into(),
        "output_*".into(),
        "*.mov".into(),
    ];

    let mut manifests_by_root: HashMap<String, Vec<Snapshot>> = HashMap::new();
    manifests_by_root.insert("/project".into(), vec![manifest]);

    let start = Instant::now();
    let result = filter_manifests(&manifests_by_root, &filters);
    let elapsed = start.elapsed();

    // All .exr files should match
    assert!(!result.is_empty(), "expected matches for *.exr filter");

    // Performance assertion: must complete within 50ms
    // Uncached: ~190ms (5000 files × 5 filters = 25000 regex compilations)
    // Cached: <1ms (5 regex compilations total)
    assert!(
        elapsed.as_millis() < 50,
        "filter_manifests took {elapsed:?} for 5000 files × 5 filters — \
         regex compilation is likely not cached"
    );
}

/// Smaller correctness + perf test: matches_any_filter called repeatedly
/// with the same filters should not degrade linearly with call count.
/// Note: matches_any_filter is a convenience function that compiles patterns
/// per call. The hot path (filter_manifests) uses FilterSet internally.
/// This test verifies the per-call overhead is acceptable for small batches.
#[test]
fn matches_any_filter_1000_calls_same_filters_within_budget() {
    let filters: Vec<String> = vec!["*.exr".into(), "*/renders/*".into(), "*.png".into()];
    let paths: Vec<String> = (0..1_000)
        .map(|i| format!("/project/shots/shot_{i:03}/renders/frame_{i:03}.exr"))
        .collect();

    let start = Instant::now();
    let mut match_count = 0u32;
    for path in &paths {
        if matches_any_filter(path, &filters) {
            match_count += 1;
        }
    }
    let elapsed = start.elapsed();

    assert_eq!(match_count, 1_000, "all .exr paths should match");
    // 1000 calls × 3 filters = 3000 regex compilations in debug mode.
    // Budget is generous (5s) since this tests correctness not perf.
    // The real perf test is filter_manifests_5000_files above.
    assert!(
        elapsed.as_secs() < 5,
        "matches_any_filter took {elapsed:?} for 1000 calls — unexpectedly slow"
    );
}
