//! Level 1 tests for download include-path filtering.
//!
//! Only tests that can't be reliably asserted through CLI output:
//! - normalize_filters: preprocessing edge cases (each would need complex S3 setup at L2)
//! - matches_any_filter: ? wildcard, [seq] class, empty filters, Windows drive letter

use deadline_job_attachments::download::{matches_any_filter, normalize_filters};

// =====================================================================
// normalize_filters — combinatorial preprocessing edge cases
// =====================================================================

#[test]
fn normalize_filters_backslash_to_forward_slash() {
    let result = normalize_filters(&["renders\\frame_001.exr".into()]);
    assert_eq!(result, vec!["renders/frame_001.exr"]);
}

#[test]
fn normalize_filters_strips_leading_dot_slash() {
    let result = normalize_filters(&["./renders/frame.exr".into()]);
    assert_eq!(result, vec!["renders/frame.exr"]);
}

#[test]
fn normalize_filters_collapses_double_slashes() {
    let result = normalize_filters(&["renders//deep//file.exr".into()]);
    assert_eq!(result, vec!["renders/deep/file.exr"]);
}

#[test]
fn normalize_filters_drops_empty_after_normalization() {
    let result = normalize_filters(&["./".into(), "".into(), "valid.exr".into()]);
    assert_eq!(result, vec!["valid.exr"]);
}

#[test]
fn normalize_filters_combined() {
    let result = normalize_filters(&[".\\renders//frame.exr".into()]);
    assert_eq!(result, vec!["renders/frame.exr"]);
}

#[test]
fn normalize_filters_preserves_absolute_paths() {
    let result = normalize_filters(&["/mnt/project/renders/*.exr".into()]);
    assert_eq!(result, vec!["/mnt/project/renders/*.exr"]);
}

// =====================================================================
// matches_any_filter — edge cases not exercised by L2 CLI tests
// =====================================================================

#[test]
fn matches_any_filter_question_mark_wildcard() {
    assert!(matches_any_filter(
        "/mnt/project/renders/frame_001.exr",
        &["frame_00?.exr".into()]
    ));
}

#[test]
fn matches_any_filter_bracket_character_class() {
    assert!(matches_any_filter(
        "/mnt/project/renders/frame_001.exr",
        &["frame_00[123].exr".into()]
    ));
}

#[test]
fn matches_any_filter_bracket_negation() {
    // [!3] matches any char NOT 3
    assert!(matches_any_filter(
        "/mnt/project/renders/frame_001.exr",
        &["frame_00[!3].exr".into()]
    ));
    assert!(!matches_any_filter(
        "/mnt/project/renders/frame_003.exr",
        &["frame_00[!3].exr".into()]
    ));
}

#[test]
fn matches_any_filter_empty_filters_no_match() {
    assert!(!matches_any_filter("/mnt/project/file.exr", &[]));
}

#[test]
fn matches_any_filter_windows_drive_letter_is_absolute() {
    assert!(matches_any_filter(
        "C:/Users/artist/renders/frame.exr",
        &["C:/Users/artist/renders/*.exr".into()]
    ));
}
